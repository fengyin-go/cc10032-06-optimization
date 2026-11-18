"""补数任务领域服务：管理实时指标的离线补数任务。

数据流角色: watermark_track / late_handler → side_output（迟到旁路版本）
            → backfill_job（按目标指标和位点范围消费）

序号6修复后的去重流程（读取迟到旁路数据 → 合并补数范围 → 清理补数任务清单）:
  1. 所有补数入口（人工登记、暂停后重试、迟到自动触发）统一走 submit_backfill，
     在 store.backfill_lock 串行化，并发修复只应用一次并保持幂等；
  2. 同一目标指标的重叠范围先扣除已完成区间，再合并为一个可执行作业的唯一活动范围；
  3. 合并后的作业只引用一个当前迟到旁路数据版本，补数任务清单保留全部来源版本；
  4. 自动补数与人工补数相遇时支持“人工吸收自动”或“父子作业”两种顺序，
     对外结果始终收敛到一个活动范围；
  5. 暂停/恢复在事务内进行，进度只增不减，已完成区间不会重新计入待处理；
  6. 事务中断整体回滚到修复前版本，修复动作同步写入修复台账。
"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService
from app.services.side_output import side_output_service
from app.store import merge_ranges, store, subtract_ranges


def _parse_int(value: Any) -> int | None:
    try:
        return int(str(value).strip())
    except (ValueError, TypeError, AttributeError):
        return None


def _stringify_ranges(ranges: list[tuple[int, int]] | list[list[int]]) -> str:
    return ",".join(f"{lo}-{hi}" for lo, hi in ranges)


class BackfillJobService(BaseModuleService):
    MODULE = "backfill_job"
    ENTITY = "补数作业"
    LIST_FIELDS = ["作业名称", "目标指标", "补数范围", "并行度", "起始位点", "截止位点", "进度比例"]
    REQUIRED_FIELDS = ["作业名称", "目标指标", "补数范围"]
    # 序号6：新增“已暂停”中间态，暂停/恢复不再把作业打回待执行
    STATUS_ORDER = ["待执行", "补数中", "已暂停", "已完成", "部分失败"]
    ACTION_RULES = {"启动补数": "待执行", "暂停补数": "补数中"}
    NEGATIVE_ACTIONS = []

    ACTIVE_STATUSES = ("待执行", "补数中", "已暂停")

    # ------------------------------------------------------------------
    # 基础工具
    # ------------------------------------------------------------------

    def _all_jobs(self) -> list[dict[str, Any]]:
        return store.rows(self.MODULE)

    def _valid_jobs(self) -> list[dict[str, Any]]:
        """位点可解析的有效作业；历史残留脏数据不参与去重计算。"""
        return [j for j in self._all_jobs() if not self._is_residual(j)]

    def _is_residual(self, job: dict[str, Any]) -> bool:
        if job.get("_residual"):
            return True
        if isinstance(job.get("_active_ranges"), list):
            return False
        return _parse_int(job.get("起始位点")) is None or _parse_int(job.get("截止位点")) is None

    def _active_ranges(self, job: dict[str, Any]) -> list[tuple[int, int]]:
        return merge_ranges([tuple(r) for r in job.get("_active_ranges", [])])

    def _completed_ranges(self, job: dict[str, Any]) -> list[tuple[int, int]]:
        return merge_ranges([tuple(r) for r in job.get("_completed_ranges", [])])

    def _external_active_jobs(self, metric: str) -> list[dict[str, Any]]:
        """对外的活动作业：父子模式下子作业不计入独立活动范围。"""
        return [
            j for j in self._valid_jobs()
            if j.get("目标指标") == metric
            and j.get("status") in self.ACTIVE_STATUSES
            and not j.get("_parent_job")
        ]

    def _completed_union(self, metric: str) -> list[tuple[int, int]]:
        """同一目标指标全部作业的共同完成回执区间。"""
        covered: list[tuple[int, int]] = []
        for j in self._valid_jobs():
            if j.get("目标指标") == metric:
                covered.extend(self._completed_ranges(j))
        return merge_ranges(covered)

    def _recompute_display(self, job: dict[str, Any]) -> None:
        """根据活动/完成区间重算清单展示字段与进度比例。"""
        active = self._active_ranges(job)
        completed = self._completed_ranges(job)
        overall = merge_ranges(active + completed)
        job["_active_ranges"] = [list(r) for r in active]
        job["_completed_ranges"] = [list(r) for r in completed]
        job["补数范围"] = _stringify_ranges(active) or _stringify_ranges(completed)
        if overall:
            job["起始位点"] = str(overall[0][0])
            job["截止位点"] = str(overall[-1][1])
        total = sum(hi - lo + 1 for lo, hi in overall)
        done = sum(hi - lo + 1 for lo, hi in completed)
        pct = int(done * 100 / total) if total else 0
        if total and done >= total:
            pct = 100
        # 补数中作业的进度比例由 track_progress 按次推进，区间重算不能把它覆盖回低值
        if job.get("status") == "补数中":
            displayed = _parse_int(str(job.get("进度比例", "0%")).replace("%", "")) or 0
            pct = max(pct, displayed)
        # 暂停后恢复不允许完成比例倒退
        floor = job.get("_paused_progress_pct")
        if floor is not None and active:
            pct = max(pct, floor)
        job["进度比例"] = f"{pct}%"

    def _bind_side_output(self, job: dict[str, Any], metric: str) -> None:
        """把作业绑定到目标指标当前唯一的迟到旁路数据版本。"""
        ref = side_output_service.current_version_ref(metric)
        previous = job.get("_side_output_ref") or {}
        job["_side_output_ref"] = ref
        sources = job.setdefault("_source_versions", [])
        if ref["version"] is not None and ref["version"] not in [
            s.get("version") for s in sources
        ]:
            sources.append(ref)
        if previous.get("version") and previous.get("version") != ref.get("version"):
            if previous not in sources:
                sources.append(previous)
            job.setdefault("_merge_logs", []).append({
                "at": time.time(),
                "event": "side_output_rebind",
                "from_version": previous.get("version"),
                "to_version": ref.get("version"),
            })

    def _new_job(
        self,
        metric: str,
        ranges: list[tuple[int, int]],
        *,
        trigger_source: str,
        request_key: str | None,
        job_name: str | None,
        parent_job: int | None = None,
    ) -> dict[str, Any]:
        rows = self._all_jobs()
        job_id = max((int(r.get("id", 0)) for r in rows), default=0) + 1
        job = {
            "id": job_id,
            "作业名称": job_name or f"{trigger_source}_backfill_{metric}_{job_id}",
            "目标指标": metric,
            "补数范围": _stringify_ranges(ranges),
            "并行度": str(min(8, max(1, sum(hi - lo + 1 for lo, hi in ranges) // 100 + 1))),
            "起始位点": str(ranges[0][0]) if ranges else "",
            "截止位点": str(ranges[-1][1]) if ranges else "",
            "进度比例": "0%",
            "status": "待执行",
            "pending": True,
            "abnormal": False,
            "_active_ranges": [list(r) for r in ranges],
            "_completed_ranges": [],
            "_trigger_source": trigger_source,
            "_request_keys": [request_key] if request_key else [],
            "_merge_logs": [],
            "_source_versions": [],
            "_parent_job": parent_job,
            "_child_jobs": [],
            "_retry_token": None,
            "_paused_progress_pct": None,
        }
        self._bind_side_output(job, metric)
        rows.append(job)
        return job

    def _ledger(self, action: str, detail: dict[str, Any]) -> None:
        store.append_repair_ledger({
            "module": self.MODULE,
            "related_modules": ["late_handler", "side_output"],
            "repair_seq": "序号6",
            "action": action,
            "detail": detail,
        })

    # ------------------------------------------------------------------
    # 统一补数入口（人工 / 自动 / 重试共用）
    # ------------------------------------------------------------------

    def submit_backfill(
        self,
        target_metric: str,
        start_offset: Any,
        end_offset: Any,
        *,
        trigger_source: str = "manual",
        request_key: str | None = None,
        job_name: str | None = None,
        mode: str = "absorb",
        abort: bool = False,
    ) -> dict[str, Any]:
        """序号6统一补数入口：合并重叠范围、扣减已完成区间、保持幂等。

        mode:
          - absorb：保留主作业（人工优先）并吸收新范围，重复作业并入主作业；
          - parent_child：主作业作为父作业，新触发生成子作业，对外仍收敛为
            父作业的一个活动范围。
        """
        lo, hi = _parse_int(start_offset), _parse_int(end_offset)
        if lo is None or hi is None or lo > hi:
            store.register_failure(
                self.MODULE, "submit_backfill",
                f"非法补数范围 {start_offset}-{end_offset}",
                strategy="escalate",
                context={"target_metric": target_metric, "trigger_source": trigger_source},
            )
            return {
                "ok": False,
                "error": f"非法补数范围：{start_offset}-{end_offset}，起始位点须为不大于截止位点的整数",
                "target_metric": target_metric,
            }

        with store.backfill_lock:
            with store.transaction("backfill_submit"):
                # 1. 幂等：同一请求键只生效一次（暂停、重试、自动触发并发时）
                if request_key:
                    for job in self._valid_jobs():
                        if (
                            job.get("目标指标") == target_metric
                            and request_key in job.get("_request_keys", [])
                        ):
                            # 即使重复请求不新建作业，也同步到最新旁路版本，
                            # 保证合并后的作业始终引用一个当前版本。
                            self._bind_side_output(job, target_metric)
                            self._recompute_display(job)
                            return {
                                "ok": True,
                                "idempotent": True,
                                "reason": "相同补数请求已处理，直接返回既有作业",
                                "job_id": job["id"],
                                "job_name": job["作业名称"],
                                "target_metric": target_metric,
                                "active_ranges": self._active_ranges(job),
                                "side_output_ref": job.get("_side_output_ref"),
                            }

                # 2. 已完成区间不能被重新计入待处理
                completed = self._completed_union(target_metric)
                remaining = subtract_ranges([(lo, hi)], completed)
                if not remaining:
                    cover = next(
                        (j for j in self._valid_jobs()
                         if j.get("目标指标") == target_metric
                         and self._completed_ranges(j)),
                        None,
                    )
                    return {
                        "ok": True,
                        "idempotent": True,
                        "reason": "请求区间已全部完成，不再生成待处理作业",
                        "job_id": cover["id"] if cover else None,
                        "target_metric": target_metric,
                        "active_ranges": [],
                        "covered_ranges": completed,
                    }

                # 3. 找同一目标指标上范围重叠的对外活动作业
                candidates = [
                    j for j in self._external_active_jobs(target_metric)
                    if any(
                        not (hi_ < lo_ or lo > hi_)
                        for lo_, hi_ in self._active_ranges(j)
                    )
                ]

                if not candidates:
                    job = self._new_job(
                        target_metric, remaining,
                        trigger_source=trigger_source, request_key=request_key,
                        job_name=job_name,
                    )
                    self._recompute_display(job)
                    self._ledger("create_backfill", {
                        "job_id": job["id"], "target_metric": target_metric,
                        "ranges": [list(r) for r in remaining],
                        "trigger_source": trigger_source,
                        "side_output_version": job.get("_side_output_ref", {}).get("version"),
                    })
                    if abort:
                        store.abort_transaction("backfill_submit", "模拟事务中断")
                    return {
                        "ok": True, "idempotent": False, "mode": "create",
                        "job_id": job["id"], "job_name": job["作业名称"],
                        "target_metric": target_metric,
                        "active_ranges": self._active_ranges(job),
                        "side_output_ref": job.get("_side_output_ref"),
                    }

                # 4. 主作业选择：人工补数优先于自动补数，其次取最早创建的
                manual = [j for j in candidates if j.get("_trigger_source") != "auto"]
                primary = sorted(manual or candidates, key=lambda j: int(j["id"]))[0]
                others = [j for j in candidates if j["id"] != primary["id"]]

                if mode == "parent_child":
                    child = self._new_job(
                        target_metric, remaining,
                        trigger_source=trigger_source, request_key=request_key,
                        job_name=job_name, parent_job=primary["id"],
                    )
                    primary.setdefault("_child_jobs", []).append(child["id"])
                    before = self._active_ranges(primary)
                    primary["_active_ranges"] = [
                        list(r) for r in subtract_ranges(
                            merge_ranges(before + self._active_ranges(child)), completed,
                        )
                    ]
                    if request_key:
                        primary.setdefault("_request_keys", []).append(request_key)
                    primary.setdefault("_merge_logs", []).append({
                        "at": time.time(), "event": "parent_child",
                        "child_job_id": child["id"],
                        "before_ranges": [list(r) for r in before],
                        "after_ranges": [list(r) for r in self._active_ranges(primary)],
                        "trigger_source": trigger_source,
                    })
                    self._bind_side_output(primary, target_metric)
                    self._recompute_display(primary)
                    self._recompute_display(child)
                    self._ledger("parent_child_backfill", {
                        "parent_job_id": primary["id"], "child_job_id": child["id"],
                        "target_metric": target_metric,
                        "active_scope": [list(r) for r in self._active_ranges(primary)],
                    })
                    result_mode = "parent_child"
                    target_job = primary
                else:
                    before = self._active_ranges(primary)
                    absorbed: list[dict[str, Any]] = []
                    union_input = list(before) + list(remaining)
                    for other in others:
                        union_input.extend(self._active_ranges(other))
                        # 共同完成回执归集到主作业
                        primary["_completed_ranges"] = [
                            list(r) for r in merge_ranges(
                                self._completed_ranges(primary)
                                + self._completed_ranges(other),
                            )
                        ]
                        for key in other.get("_request_keys", []):
                            primary.setdefault("_request_keys", []).append(key)
                        for src in other.get("_source_versions", []):
                            if src not in primary.setdefault("_source_versions", []):
                                primary["_source_versions"].append(src)
                        other["_active_ranges"] = []
                        other["_merged_into"] = primary["id"]
                        other["_deduplicated"] = True
                        other["status"] = "已完成"
                        other["pending"] = False
                        self._recompute_display(other)
                        absorbed.append({"job_id": other["id"], "job_name": other["作业名称"]})
                    if request_key:
                        primary.setdefault("_request_keys", []).append(request_key)
                    primary["_trigger_source"] = (
                        "manual" if primary.get("_trigger_source") == "manual"
                        or trigger_source == "manual" else "auto"
                    )
                    primary["_active_ranges"] = [
                        list(r)
                        for r in subtract_ranges(merge_ranges(union_input), completed)
                    ]
                    primary.setdefault("_merge_logs", []).append({
                        "at": time.time(), "event": "absorb",
                        "before_ranges": [list(r) for r in before],
                        "after_ranges": [list(r) for r in self._active_ranges(primary)],
                        "absorbed_jobs": absorbed,
                        "incoming_ranges": [list(r) for r in remaining],
                        "trigger_source": trigger_source,
                    })
                    self._bind_side_output(primary, target_metric)
                    self._recompute_display(primary)
                    self._ledger("absorb_backfill", {
                        "primary_job_id": primary["id"],
                        "absorbed_jobs": absorbed,
                        "target_metric": target_metric,
                        "before_ranges": [list(r) for r in before],
                        "after_ranges": [list(r) for r in self._active_ranges(primary)],
                        "side_output_version": primary.get("_side_output_ref", {}).get("version"),
                    })
                    result_mode = "absorb"
                    target_job = primary

                if abort:
                    store.abort_transaction("backfill_submit", "模拟事务中断")
                return {
                    "ok": True, "idempotent": False, "mode": result_mode,
                    "job_id": target_job["id"], "job_name": target_job["作业名称"],
                    "target_metric": target_metric,
                    "active_ranges": self._active_ranges(target_job),
                    "side_output_ref": target_job.get("_side_output_ref"),
                    "merge_logs": target_job.get("_merge_logs", []),
                }

    # ------------------------------------------------------------------
    # 清理补数任务清单：合并同指标重叠作业
    # ------------------------------------------------------------------

    def merge_overlapping_jobs(
        self, target_metric: str | None = None, *, abort: bool = False,
    ) -> dict[str, Any]:
        """扫描补数任务清单，把同目标指标的重叠活动作业合并为一个。

        幂等：第二次执行时已无重叠对外活动作业，返回零变更。
        """
        with store.backfill_lock:
            with store.transaction("backfill_merge"):
                metrics = sorted({
                    j.get("目标指标", "")
                    for j in self._valid_jobs()
                    if target_metric is None or j.get("目标指标") == target_metric
                })
                affected: list[dict[str, Any]] = []
                merged_count = 0
                for metric in metrics:
                    active = self._external_active_jobs(metric)
                    if len(active) <= 1:
                        continue
                    ranges_set = [
                        (j["id"], self._active_ranges(j)) for j in active
                    ]
                    overlaps = False
                    for i in range(len(ranges_set)):
                        for k in range(i + 1, len(ranges_set)):
                            a, b = ranges_set[i][1], ranges_set[k][1]
                            if any(not (hi < lo2 or lo > hi2)
                                   for lo, hi in a for lo2, hi2 in b):
                                overlaps = True
                    if not overlaps:
                        continue
                    primary = sorted(
                        [j for j in active if j.get("_trigger_source") != "auto"] or active,
                        key=lambda j: int(j["id"]),
                    )[0]
                    before = self._active_ranges(primary)
                    completed = self._completed_union(metric)
                    union_input = list(before)
                    absorbed = []
                    for other in active:
                        if other["id"] == primary["id"]:
                            continue
                        union_input.extend(self._active_ranges(other))
                        primary["_completed_ranges"] = [
                            list(r) for r in merge_ranges(
                                self._completed_ranges(primary)
                                + self._completed_ranges(other),
                            )
                        ]
                        for key in other.get("_request_keys", []):
                            primary.setdefault("_request_keys", []).append(key)
                        for src in other.get("_source_versions", []):
                            if src not in primary.setdefault("_source_versions", []):
                                primary["_source_versions"].append(src)
                        other["_active_ranges"] = []
                        other["_merged_into"] = primary["id"]
                        other["_deduplicated"] = True
                        other["status"] = "已完成"
                        other["pending"] = False
                        self._recompute_display(other)
                        absorbed.append(other["id"])
                        merged_count += 1
                    primary["_active_ranges"] = [
                        list(r)
                        for r in subtract_ranges(merge_ranges(union_input), completed)
                    ]
                    primary.setdefault("_merge_logs", []).append({
                        "at": time.time(), "event": "cleanup_merge",
                        "before_ranges": [list(r) for r in before],
                        "after_ranges": [list(r) for r in self._active_ranges(primary)],
                        "absorbed_jobs": absorbed,
                    })
                    self._bind_side_output(primary, metric)
                    self._recompute_display(primary)
                    affected.append({
                        "target_metric": metric,
                        "primary_job_id": primary["id"],
                        "absorbed_job_ids": absorbed,
                        "before_ranges": [list(r) for r in before],
                        "after_ranges": [list(r) for r in self._active_ranges(primary)],
                    })
                    self._ledger("cleanup_merge", affected[-1])
                if abort:
                    store.abort_transaction("backfill_merge", "模拟事务中断")
                return {
                    "ok": True,
                    "metrics_with_duplicates": len(affected),
                    "merged_jobs": merged_count,
                    "details": affected,
                }

    def deduplicate_backfill_jobs(self) -> dict[str, Any]:
        """兼容旧接口名：内部改为走序号6的范围合并清理流程。"""
        result = self.merge_overlapping_jobs()
        return {
            "ok": True,
            "metrics_with_duplicates": result["metrics_with_duplicates"],
            "deduplicated_jobs": result["merged_jobs"],
            "details": result["details"],
        }

    # ------------------------------------------------------------------
    # 暂停 / 恢复 / 重试
    # ------------------------------------------------------------------

    def pause_job(self, job_id: int) -> dict[str, Any]:
        """暂停补数：快照进度并保留唯一旁路引用，活动范围不变。"""
        with store.backfill_lock:
            with store.transaction("backfill_pause"):
                job = store.find(self.MODULE, job_id)
                if job is None or self._is_residual(job):
                    return {"ok": False, "error": f"补数任务 {job_id} 不存在或为残留数据"}
                if job.get("status") != "补数中":
                    return {"ok": False, "error": f"任务状态为「{job.get('status')}」，不可暂停"}
                pct = int(str(job.get("进度比例", "0%")).replace("%", ""))
                job["_paused_progress_pct"] = pct
                job["_paused_at"] = time.time()
                job["status"] = "已暂停"
                job["pending"] = True
                self._ledger("pause_backfill", {
                    "job_id": job_id, "progress_pct": pct,
                    "active_ranges": [list(r) for r in self._active_ranges(job)],
                    "side_output_version": job.get("_side_output_ref", {}).get("version"),
                })
                return {
                    "ok": True, "job_id": job_id, "status": job["status"],
                    "paused_progress_pct": pct,
                    "active_ranges": self._active_ranges(job),
                    "side_output_ref": job.get("_side_output_ref"),
                }

    def resume_job(self, job_id: int) -> dict[str, Any]:
        """恢复补数：进度不低于暂停快照，已完成区间不重新计入。"""
        with store.backfill_lock:
            with store.transaction("backfill_resume"):
                job = store.find(self.MODULE, job_id)
                if job is None or self._is_residual(job):
                    return {"ok": False, "error": f"补数任务 {job_id} 不存在或为残留数据"}
                if job.get("status") != "已暂停":
                    return {"ok": False, "error": f"任务状态为「{job.get('status')}」，不可恢复"}
                if not self._active_ranges(job):
                    return {"ok": False, "error": "活动范围为空（区间均已完成），无需恢复"}
                job["status"] = "补数中"
                job["_resumed_at"] = time.time()
                self._recompute_display(job)
                self._ledger("resume_backfill", {
                    "job_id": job_id,
                    "progress_pct": int(str(job["进度比例"]).replace("%", "")),
                    "active_ranges": [list(r) for r in self._active_ranges(job)],
                })
                return {
                    "ok": True, "job_id": job_id, "status": job["status"],
                    "progress_pct": int(str(job["进度比例"]).replace("%", "")),
                    "active_ranges": self._active_ranges(job),
                }

    def retry_job(self, job_id: int, retry_token: str | None = None) -> dict[str, Any]:
        """失败/暂停后重试：并发携带相同令牌只生效一次，且不重复已完成区间。"""
        with store.backfill_lock:
            job = store.find(self.MODULE, job_id)
            if job is None or self._is_residual(job):
                return {"ok": False, "error": f"补数任务 {job_id} 不存在或为残留数据"}
            token = retry_token or f"retry::{job_id}::{int(time.time())}"
            if job.get("_retry_token") == token:
                return {
                    "ok": True, "idempotent": True,
                    "reason": "相同重试令牌已生效，并发重试只应用一次",
                    "job_id": job_id, "status": job.get("status"),
                }
            with store.transaction("backfill_retry"):
                job["_retry_token"] = token
                if not self._active_ranges(job):
                    return {
                        "ok": True, "requeued": False,
                        "reason": "已完成区间不能被重新计入待处理，无剩余活动范围",
                        "job_id": job_id,
                    }
                job["status"] = "补数中"
                job["pending"] = True
                job["abnormal"] = False
                self._recompute_display(job)
                self._ledger("retry_backfill", {
                    "job_id": job_id, "retry_token": token,
                    "active_ranges": [list(r) for r in self._active_ranges(job)],
                })
                return {
                    "ok": True, "requeued": True, "idempotent": False,
                    "job_id": job_id, "status": job["status"],
                    "active_ranges": self._active_ranges(job),
                    "progress_pct": int(str(job["进度比例"]).replace("%", "")),
                }

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        """动作路由：启动补数同时承担“恢复”语义，暂停补数进入已暂停态。"""
        job = store.find(self.MODULE, entry_id)
        if job is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"
        if action == "暂停补数":
            result = self.pause_job(entry_id)
        elif action == "启动补数":
            if job.get("status") == "已暂停":
                result = self.resume_job(entry_id)
            elif job.get("status") == "待执行":
                with store.backfill_lock:
                    with store.transaction("backfill_start"):
                        job["status"] = "补数中"
                        self._recompute_display(job)
                result = {"ok": True, "job_id": entry_id, "status": "补数中"}
            elif job.get("status") in ("部分失败",):
                result = self.retry_job(entry_id)
            else:
                return None, f"任务状态为「{job.get('status')}」，不可执行启动补数"
        else:
            return None, f"动作「{action}」不属于{self.MODULE}可执行范围"
        if not result.get("ok"):
            return None, result.get("error", "动作执行失败")
        return job, f"{self.ENTITY}已{action}"

    # ------------------------------------------------------------------
    # 进度跟踪
    # ------------------------------------------------------------------

    def track_progress(self, job_name: str) -> dict[str, Any]:
        """跟踪补数进度；推进只增不减，完成时活动区间转入共同完成回执。"""
        job = next((r for r in self._all_jobs() if r.get("作业名称") == job_name), None)
        if job is None:
            return {"ok": False, "error": f"补数任务「{job_name}」不存在"}
        if self._is_residual(job):
            return {"ok": False, "error": f"补数任务「{job_name}」为历史残留数据，位点不可解析"}

        current = _parse_int(str(job.get("进度比例", "0%")).replace("%", "")) or 0
        if job.get("status") == "补数中":
            with store.backfill_lock:
                with store.transaction("backfill_progress"):
                    progress_pct = min(100, current + 15)
                    floor = job.get("_paused_progress_pct")
                    if floor is not None:
                        progress_pct = max(progress_pct, floor)
                    job["进度比例"] = f"{progress_pct}%"
                    if progress_pct >= 100:
                        job["_completed_ranges"] = [
                            list(r) for r in merge_ranges(
                                self._completed_ranges(job) + self._active_ranges(job),
                            )
                        ]
                        job["_active_ranges"] = []
                        job["status"] = "已完成"
                        job["pending"] = False
                        job["_paused_progress_pct"] = None
                        self._verify_with_aggregate_sink(job)
                        self._ledger("complete_backfill", {
                            "job_id": job["id"],
                            "completed_ranges": job["_completed_ranges"],
                        })
                    self._recompute_display(job)
        else:
            progress_pct = current

        active = self._active_ranges(job)
        completed = self._completed_ranges(job)
        total_range = sum(hi - lo + 1 for lo, hi in merge_ranges(active + completed))
        return {
            "ok": True,
            "job": job_name,
            "status": job.get("status", ""),
            "progress_pct": progress_pct,
            "total_range": total_range,
            "completed_range": sum(hi - lo + 1 for lo, hi in completed),
            "active_ranges": active,
            "completed_ranges": completed,
        }

    def _verify_with_aggregate_sink(self, job: dict[str, Any]) -> None:
        """补数完成后与聚合落库进行交叉验证。"""
        target = job.get("目标指标", "")
        matched = sum(
            1 for r in store.rows("aggregate_sink")
            if target in str(r.get("源指标集", ""))
        )
        if matched > 0:
            job["_verified_with_aggregate"] = matched
            job["_verified_at"] = time.time()

    # ------------------------------------------------------------------
    # 范围校验 / 清单 / 详情 / 复核
    # ------------------------------------------------------------------

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        """人工登记补数：缺失字段走通用校验，合法登记统一走范围合并入口。"""
        missing = [
            field for field in self.REQUIRED_FIELDS
            if not str(values.get(field) or "").strip()
        ]
        if missing:
            return None, missing
        lo = _parse_int(values.get("起始位点"))
        hi = _parse_int(values.get("截止位点"))
        if lo is None or hi is None:
            # 兼容旧的“补数范围”单字段写法，如 100-200
            rng = str(values.get("补数范围", "")).split("-")
            if len(rng) == 2:
                lo, hi = _parse_int(rng[0]), _parse_int(rng[1])
        result = self.submit_backfill(
            str(values.get("目标指标")), lo if lo is not None else "",
            hi if hi is not None else "",
            trigger_source="manual",
            request_key=f"manual::{values.get('作业名称')}::{lo}-{hi}",
            job_name=str(values.get("作业名称")),
        )
        if not result.get("ok"):
            return None, [result.get("error", "补数范围不合法")]
        return store.find(self.MODULE, result["job_id"]), []

    def validate_backfill_range(
        self, target_metric: str, start_offset: str, end_offset: str,
    ) -> dict[str, Any]:
        """校验补数范围：起止位点合法、不与活动范围冲突、不重复已完成区间。"""
        issues = []
        start, end = _parse_int(start_offset), _parse_int(end_offset)
        if start is None:
            issues.append("起始位点必须为整数")
            start = 0
        if end is None:
            issues.append("截止位点必须为整数")
            end = 0
        if start > end:
            issues.append("起始位点不能大于截止位点")
        if start == end:
            issues.append("补数范围不能为空（起始位点等于截止位点）")

        if start is not None and end is not None and start <= end:
            for j in self._external_active_jobs(target_metric):
                if any(not (end < lo or start > hi) for lo, hi in self._active_ranges(j)):
                    issues.append(
                        f"指标「{target_metric}」活动作业「{j.get('作业名称')}」"
                        f"范围 {_stringify_ranges(self._active_ranges(j))} 与请求范围重叠，将走合并流程"
                    )
            covered = subtract_ranges([(start, end)], self._completed_union(target_metric))
            if not covered:
                issues.append("请求区间已全部完成，已完成区间不能被重新计入待处理")

        return {
            "ok": len(issues) == 0,
            "target": target_metric,
            "range": f"{start_offset}-{end_offset}",
            "range_size": (end - start) if isinstance(start, int) and isinstance(end, int) else 0,
            "issues": issues,
        }

    def estimate_parallelism(self, range_size: int, data_rate: float = 1000.0) -> dict[str, Any]:
        """根据数据范围自动推算最优并行度。"""
        optimal = max(1, min(32, int((range_size * data_rate) // 10000)))
        return {
            "range_size": range_size,
            "estimated_events": int(range_size * data_rate),
            "recommended_parallelism": optimal,
            "estimated_duration_sec": (range_size * data_rate) / (optimal * 100),
        }

    def list_pending_backfills(self) -> list[dict[str, Any]]:
        """列出待执行/补数中/已暂停任务，携带唯一活动范围与旁路版本引用。"""
        pending = [
            r for r in self._all_jobs()
            if r.get("status") in ("待执行", "补数中", "已暂停")
        ]
        result = []
        for j in pending:
            residual = self._is_residual(j)
            entry = {
                "job_id": j.get("id"),
                "job_name": j.get("作业名称", ""),
                "target": j.get("目标指标", ""),
                "range": j.get("补数范围", ""),
                "parallelism": j.get("并行度", ""),
                "progress": j.get("进度比例", "0%"),
                "status": j.get("status", ""),
                "residual": residual,
                "trigger_source": j.get("_trigger_source", "legacy"),
                "active_ranges": [] if residual else [list(r) for r in self._active_ranges(j)],
                "completed_ranges": [] if residual else [list(r) for r in self._completed_ranges(j)],
                "side_output_version": (j.get("_side_output_ref") or {}).get("version"),
                "parent_job": j.get("_parent_job"),
            }
            for wt in store.rows("watermark_track"):
                if wt.get("数据源") in str(j.get("目标指标", "")):
                    entry["watermark_status"] = wt.get("status", "")
                    break
            result.append(entry)
        return result

    def job_detail(self, job_id: int) -> dict[str, Any]:
        """6号详情面板：返回完整结果，含合并前后范围、来源版本、暂停恢复快照。"""
        job = store.find(self.MODULE, job_id)
        if job is None:
            return {"ok": False, "error": f"补数任务 {job_id} 不存在"}
        residual = self._is_residual(job)
        children = [
            store.find(self.MODULE, cid) for cid in job.get("_child_jobs", [])
        ]
        return {
            "ok": True,
            "job": {
                "id": job["id"],
                "作业名称": job.get("作业名称"),
                "目标指标": job.get("目标指标"),
                "status": job.get("status"),
                "进度比例": job.get("进度比例"),
                "补数范围": job.get("补数范围"),
                "起始位点": job.get("起始位点"),
                "截止位点": job.get("截止位点"),
                "residual": residual,
                "ineffective": residual,
                "trigger_source": job.get("_trigger_source", "legacy"),
                "active_ranges": [] if residual else [list(r) for r in self._active_ranges(job)],
                "completed_ranges": [] if residual else [list(r) for r in self._completed_ranges(job)],
                "side_output_ref": job.get("_side_output_ref"),
                "source_versions": job.get("_source_versions", []),
                "request_keys": job.get("_request_keys", []),
                "merge_logs": job.get("_merge_logs", []),
                "parent_job": job.get("_parent_job"),
                "child_jobs": [
                    {
                        "job_id": c["id"], "job_name": c.get("作业名称"),
                        "status": c.get("status"),
                        "active_ranges": [list(r) for r in self._active_ranges(c)],
                    }
                    for c in children if c
                ],
                "paused_progress_pct": job.get("_paused_progress_pct"),
                "merged_into": job.get("_merged_into"),
                "retry_token": job.get("_retry_token"),
                "verified_with_aggregate": job.get("_verified_with_aggregate"),
            },
        }

    def review_checklist(self, target_metric: str | None = None) -> dict[str, Any]:
        """补数任务清单复核：唯一活动范围、单一旁路引用、完成区间不重入。"""
        self.cleanup_residual(write_ledger=False)
        metrics = sorted({
            j.get("目标指标", "") for j in self._valid_jobs()
            if target_metric is None or j.get("目标指标") == target_metric
        })
        items = []
        for metric in metrics:
            external = self._external_active_jobs(metric)
            scopes = [self._active_ranges(j) for j in external]
            unified = merge_ranges([r for scope in scopes for r in scope])
            ref_versions = {
                (j.get("_side_output_ref") or {}).get("version")
                for j in external
            }
            ref_versions.discard(None)
            current = side_output_service.current_version_ref(metric)
            completed = self._completed_union(metric)
            reentry = any(
                any(not (hi < lo2 or lo > hi2) for lo, hi in completed for lo2, hi2 in scope)
                for scope in scopes
            )
            paused = [
                {
                    "job_id": j["id"],
                    "paused_progress_pct": j.get("_paused_progress_pct"),
                    "current_progress_pct": int(str(j.get("进度比例", "0%")).replace("%", "")),
                }
                for j in external
                if j.get("status") == "已暂停"
            ]
            items.append({
                "target_metric": metric,
                "active_job_count": len(external),
                "unique_active_scope": len(external) <= 1,
                "active_ranges": [list(r) for r in unified],
                "completed_ranges": [list(r) for r in completed],
                "side_output_current_version": current.get("version"),
                "referenced_versions": sorted(v for v in ref_versions if v is not None),
                "single_side_output_ref": len(ref_versions) <= 1,
                "completed_reentry": reentry,
                "paused_jobs": paused,
                "progress_non_regressive": all(
                    p["current_progress_pct"] >= (p["paused_progress_pct"] or 0)
                    for p in paused
                ),
                "child_jobs": [
                    j["id"] for j in self._valid_jobs()
                    if j.get("目标指标") == metric and j.get("_parent_job")
                ],
            })
        return {
            "ok": True,
            "items": items,
            "residual_jobs": [
                {"job_id": j["id"], "job_name": j.get("作业名称")}
                for j in self._all_jobs() if j.get("_residual")
            ],
            "ledger_entries": len(store.repair_ledger(self.MODULE)),
            "all_converged": all(i["unique_active_scope"] for i in items),
        }

    def cleanup_residual(self, write_ledger: bool = True) -> dict[str, Any]:
        """标记6号残留数据：位点不可解析的历史脏行不参与补数计算与展示复核。"""
        residual = []
        for j in self._all_jobs():
            if self._is_residual(j):
                if not j.get("_residual"):
                    j["_residual"] = True
                    j["_ineffective"] = True
                    residual.append({"job_id": j["id"], "job_name": j.get("作业名称")})
        if residual and write_ledger:
            self._ledger("mark_residual", {"residual_jobs": residual})
            store.register_failure(
                self.MODULE, "cleanup_residual",
                "补数任务清单存在位点不可解析的残留数据",
                strategy="skip", context={"residual_jobs": residual},
            )
        return {"ok": True, "residual_jobs": residual}

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        rows = self._all_jobs()
        base["pending_jobs"] = sum(1 for r in rows if r.get("status") == "待执行")
        base["running_jobs"] = sum(1 for r in rows if r.get("status") == "补数中")
        base["paused_jobs"] = sum(1 for r in rows if r.get("status") == "已暂停")
        base["completed_jobs"] = sum(1 for r in rows if r.get("status") == "已完成")
        base["partial_failures"] = sum(1 for r in rows if r.get("status") == "部分失败")
        base["deduplicated"] = sum(1 for r in rows if r.get("_deduplicated"))
        base["residual_jobs"] = sum(1 for r in rows if r.get("_residual"))
        base["active_metrics"] = len({
            j.get("目标指标") for j in self._valid_jobs()
            if j.get("status") in self.ACTIVE_STATUSES and not j.get("_parent_job")
        })
        base["side_output_refs"] = len({
            (j.get("_side_output_ref") or {}).get("side_output_key")
            for j in self._valid_jobs()
            if j.get("_side_output_ref")
        })
        return base
