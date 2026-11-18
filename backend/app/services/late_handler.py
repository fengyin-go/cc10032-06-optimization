"""迟到处理领域服务：定义流式计算中迟到数据的处理策略。

增强能力: 迟到计数统计、阈值报警、旁路输出调度、迟到数据标记、与补数联动触发
"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService
from app.services.side_output import side_output_service
from app.store import store


class LateHandlerService(BaseModuleService):
    MODULE = "late_handler"
    ENTITY = "处理策略"
    LIST_FIELDS = ["策略名称", "允许延迟", "迟到动作", "旁路输出", "计数方式", "阈值百分比", "标记字段"]
    REQUIRED_FIELDS = ["策略名称", "允许延迟", "迟到动作"]
    STATUS_ORDER = ["草稿", "已启用", "已停用"]
    ACTION_RULES = {"启用策略": "草稿", "停用策略": "已启用"}
    NEGATIVE_ACTIONS = ["停用策略"]

    def count_late_events(
        self, policy_name: str, events: list[dict[str, Any]], watermark: float,
    ) -> dict[str, Any]:
        """统计迟到事件数量并计算迟到率。"""
        policy = None
        for r in store.rows(self.MODULE):
            if r.get("策略名称") == policy_name:
                policy = r
                break
        try:
            allowed_sec = float((policy or {}).get("允许延迟", "10"))
        except (ValueError, TypeError):
            allowed_sec = 10.0

        cutoff = watermark - allowed_sec
        late = [e for e in events if e.get("event_time", 0) < cutoff]
        on_time = len(events) - len(late)
        late_ratio = len(late) / max(len(events), 1)

        # 计算迟到分布
        if late:
            lats = [e["event_time"] for e in late if "event_time" in e]
            late_dist = {
                "min_ts": min(lats) if lats else 0,
                "max_ts": max(lats) if lats else 0,
                "count": len(late),
            }
        else:
            late_dist = None

        return {
            "ok": True,
            "policy": policy_name,
            "total_events": len(events),
            "on_time": on_time,
            "late_count": len(late),
            "late_ratio": round(late_ratio, 4),
            "allowed_lateness": allowed_sec,
            "watermark": watermark,
            "late_distribution": late_dist,
        }

    def check_threshold_trigger(
        self, policy_name: str, late_ratio: float,
    ) -> dict[str, Any]:
        """检查迟到率是否超过阈值，触发旁路输出。"""
        policy = None
        for r in store.rows(self.MODULE):
            if r.get("策略名称") == policy_name:
                policy = r
                break
        try:
            threshold = float((policy or {}).get("阈值百分比", "5")) / 100
        except (ValueError, TypeError):
            threshold = 0.05

        triggered = late_ratio > threshold
        action_taken = "none"
        if triggered:
            bypass_action = (policy or {}).get("迟到动作", "bypass")
            if bypass_action == "bypass":
                action_taken = "bypass_to_side_output"
            elif bypass_action == "drop":
                action_taken = "drop_late"
            elif bypass_action == "retry":
                action_taken = "retry_with_adjusted_watermark"

        return {
            "ok": True,
            "policy": policy_name,
            "late_ratio": late_ratio,
            "threshold": threshold,
            "triggered": triggered,
            "action": (policy or {}).get("迟到动作", "bypass"),
            "action_taken": action_taken,
        }

    def trigger_backfill_for_late(
        self, policy_name: str, late_count: int, source_name: str,
        *,
        target_metric: str | None = None,
        request_key: str | None = None,
        mode: str = "absorb",
        start_offset: int = 0,
        end_offset: int | None = None,
        entries: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """当迟到数据超过阈值时自动触发补数任务。

        序号6修复：先发布版本化迟到旁路数据，再通过补数服务的统一入口提交，
        与人工补数在同一区间相遇时按 mode 收敛为一个活动范围；
        request_key 保证同一迟到批次重复触发时幂等，不再生成多条作业。
        """
        if late_count == 0:
            return {"ok": True, "triggered": False, "reason": "无迟到数据"}

        # 写入旁路输出标记
        mark_field = "迟到了"
        for r in store.rows(self.MODULE):
            if r.get("策略名称") == policy_name:
                mark_field = r.get("标记字段", "_late_mark")
                break

        metric = target_metric or f"late_data_{source_name}"
        hi = late_count if end_offset is None else end_offset
        # 1. 发布迟到旁路数据新版本（补数作业唯一引用的当前版本）
        side_version = side_output_service.publish(
            metric,
            [(start_offset, hi)],
            source=f"{policy_name}:{source_name}",
            entries=entries,
        )

        # 2. 通过统一入口提交自动补数（重叠范围合并，不再直接 append 作业）
        from app.services.backfill_job import BackfillJobService
        backfill_service = BackfillJobService()
        result = backfill_service.submit_backfill(
            metric, start_offset, hi,
            trigger_source="auto",
            request_key=request_key or f"auto::{policy_name}::{source_name}::{start_offset}-{hi}",
            job_name=f"late_backfill_{source_name}_{int(time.time())}",
            mode=mode,
        )

        store.append_repair_ledger({
            "module": "late_handler",
            "related_modules": ["backfill_job", "side_output"],
            "repair_seq": "序号6",
            "action": "auto_trigger_backfill",
            "detail": {
                "policy": policy_name, "source": source_name,
                "late_count": late_count,
                "side_output_version": side_version["version"],
                "submit_result": {
                    k: v for k, v in result.items() if k != "merge_logs"
                },
            },
        })

        if not result.get("ok"):
            return {
                "ok": False, "triggered": False,
                "reason": result.get("error", "自动补数提交失败"),
                "side_output_version": side_version["version"],
            }
        return {
            "ok": True,
            "triggered": True,
            "idempotent": bool(result.get("idempotent")),
            "policy": policy_name,
            "late_count": late_count,
            "target_metric": metric,
            "mark_field": mark_field,
            "side_output_version": side_version["version"],
            "side_output_key": side_version["side_output_key"],
            "backfill_job_id": result.get("job_id"),
            "backfill_job_name": result.get("job_name"),
            "active_ranges": result.get("active_ranges"),
            "mode": result.get("mode", "idempotent" if result.get("idempotent") else "create"),
        }

    def list_late_statistics(self) -> list[dict[str, Any]]:
        """列出所有迟到处理策略的统计信息（含旁路版本与联动补数）。"""
        result = []
        for r in store.rows(self.MODULE):
            if r.get("id", 0) < 100:
                name = r.get("策略名称", "")
                linked_jobs = sum(
                    1 for b in store.rows("backfill_job")
                    if name in str(b.get("_triggered_by", ""))
                    or any(
                        name in str(sv.get("side_output_key", ""))
                        for sv in b.get("_source_versions", [])
                    )
                )
                # 汇总该策略相关的当前旁路版本
                side_versions = [
                    {
                        "target_metric": ver["target_metric"],
                        "version": ver["version"],
                        "ranges": ver["ranges"],
                    }
                    for ver in store.list_side_outputs()
                    if name in str(ver.get("source", ""))
                ]
                result.append({
                    "policy": name,
                    "action": r.get("迟到动作", ""),
                    "threshold": r.get("阈值百分比", ""),
                    "status": r.get("status", ""),
                    "linked_backfills": linked_jobs,
                    "side_output_versions": side_versions,
                })
        return result

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        rows = store.rows(self.MODULE)
        base["active"] = sum(1 for r in rows if r.get("status") == "已启用")
        return base