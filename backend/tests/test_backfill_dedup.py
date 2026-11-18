"""序号6：迟到补数去重流程修复测试。

覆盖需求:
  - 暂停、重试、自动触发重复补报时同目标只保留一个活动作业
  - 重叠范围合并为一个可执行作业，已完成区间不重新计入待处理
  - 合并后作业只引用一个当前迟到旁路数据版本，清单保留来源版本
  - 自动/人工补数相遇：absorb 与 parent_child 两种修复顺序
  - 并发修复只应用一次且幂等；事务中断回滚到修复前版本
  - 暂停/恢复完成比例不倒退；并发重试只生效一次
  - 6号残留数据隔离、详情面板完整结果、修复台账同步
"""
from __future__ import annotations

import threading

import pytest

from app.pipeline import check_watermark_delay, create_backfill_from_late, LateDataEntry
from app.services.backfill_job import BackfillJobService
from app.services.late_handler import LateHandlerService
from app.services.side_output import side_output_service
from app.store import merge_ranges, store, subtract_ranges


@pytest.fixture(autouse=True)
def clean_backfill_tables():
    """每个用例使用干净的补数/旁路/台账状态，避免种子残留干扰断言。"""
    saved_backfill = [dict(r) for r in store.rows("backfill_job")]
    saved_side = store._side_outputs
    saved_ledger = store._repair_ledger
    store.rows("backfill_job").clear()
    store._side_outputs = {}
    store._repair_ledger = []
    yield
    store._tables["backfill_job"] = saved_backfill
    store._side_outputs = saved_side
    store._repair_ledger = saved_ledger


@pytest.fixture()
def service() -> BackfillJobService:
    return BackfillJobService()


@pytest.fixture()
def late() -> LateHandlerService:
    return LateHandlerService()


# ---------------------------------------------------------------------------
# 区间合并工具
# ---------------------------------------------------------------------------

class TestRangeHelpers:
    def test_merge_overlapping_and_adjacent(self):
        assert merge_ranges([(1, 5), (3, 8), (20, 25)]) == [(1, 8), (20, 25)]
        # 相邻区间也合并
        assert merge_ranges([(1, 5), (6, 10)]) == [(1, 10)]
        # 非法区间丢弃
        assert merge_ranges([(9, 3), (1, 2)]) == [(1, 2)]

    def test_subtract_completed_ranges(self):
        # 已完成区间不能被重新计入待处理
        assert subtract_ranges([(1, 100)], [(30, 60)]) == [(1, 29), (61, 100)]
        assert subtract_ranges([(1, 100)], [(1, 100)]) == []
        assert subtract_ranges([(1, 10), (20, 30)], [(5, 22)]) == [(1, 4), (23, 30)]


# ---------------------------------------------------------------------------
# 1. 重叠范围合并与唯一活动作业
# ---------------------------------------------------------------------------

class TestOverlapMerge:
    def test_three_triggers_merge_into_one_executable_job(self, service):
        """暂停、重试、自动触发分别提交，重叠范围必须合并为一个可执行作业。"""
        r1 = service.submit_backfill("metric_a", 0, 100, trigger_source="manual",
                                     request_key="k1", job_name="人工补数")
        # 模拟“暂停后重试”入口再次补报重叠区间
        r2 = service.submit_backfill("metric_a", 50, 150, trigger_source="manual",
                                     request_key="k2")
        # 自动触发补报另一重叠区间
        r3 = service.submit_backfill("metric_a", 120, 200, trigger_source="auto",
                                     request_key="k3")
        assert r1["ok"] and r2["ok"] and r3["ok"]
        active = service._external_active_jobs("metric_a")
        assert len(active) == 1, "同一目标指标对外只能有一个活动作业"
        job = active[0]
        assert service._active_ranges(job) == [(0, 200)]
        assert job["status"] in ("待执行", "补数中")

    def test_merged_job_is_executable_with_single_scope(self, service):
        service.submit_backfill("m", 10, 40, request_key="a")
        service.submit_backfill("m", 30, 80, request_key="b")
        job = service._external_active_jobs("m")[0]
        detail = service.job_detail(job["id"])["job"]
        assert detail["active_ranges"] == [[10, 80]]
        assert detail["status"] in ("待执行", "补数中")

    def test_completed_range_not_re_counted(self, service):
        """已完成区间不能被重新计入待处理。"""
        r = service.submit_backfill("m", 0, 100, request_key="c1")
        job = store.find("backfill_job", r["job_id"])
        # 直接构造完成回执
        job["_completed_ranges"] = [[0, 60]]
        job["_active_ranges"] = [[61, 100]]
        service._recompute_display(job)
        # 重复补报已完成的 [0,60]
        again = service.submit_backfill("m", 0, 60, request_key="c2")
        assert again["idempotent"] is True
        assert again["active_ranges"] == []
        # 部分重叠：只保留未完成部分
        partial = service.submit_backfill("m", 50, 90, request_key="c3")
        surviving = service._external_active_jobs("m")[0]
        assert service._active_ranges(surviving) == [(61, 100)]

    def test_cleanup_merge_is_idempotent(self, service):
        # 直接构造同指标重叠作业，绕过提交入口来单独验证清理接口
        rows = store.rows("backfill_job")
        base = max((int(r.get("id", 0)) for r in rows), default=0)
        for idx, (lo, hi) in enumerate([(0, 10), (5, 20), (15, 30)]):
            rows.append({
                "id": base + 1 + idx,
                "作业名称": f"重叠作业{idx}", "目标指标": "m",
                "补数范围": f"{lo}-{hi}", "起始位点": str(lo), "截止位点": str(hi),
                "进度比例": "0%", "status": "待执行", "pending": True,
                "abnormal": False,
                "_active_ranges": [[lo, hi]], "_completed_ranges": [],
                "_trigger_source": "manual", "_request_keys": [f"raw{idx}"],
                "_merge_logs": [], "_source_versions": [],
                "_child_jobs": [],
            })
        assert len(service._external_active_jobs("m")) == 3
        second = service.merge_overlapping_jobs()
        assert second["merged_jobs"] == 2
        active = service._external_active_jobs("m")
        assert len(active) == 1
        assert service._active_ranges(active[0]) == [(0, 30)]
        # 再跑一次：零变更（幂等）
        third = service.merge_overlapping_jobs()
        assert third["merged_jobs"] == 0
        assert len(service._external_active_jobs("m")) == 1

    def test_same_request_key_applies_once(self, service):
        r1 = service.submit_backfill("m", 0, 10, request_key="dup-key")
        r2 = service.submit_backfill("m", 0, 10, request_key="dup-key")
        assert r2["idempotent"] is True
        assert r1["job_id"] == r2["job_id"]
        assert len(service._external_active_jobs("m")) == 1


# ---------------------------------------------------------------------------
# 2. 迟到旁路数据版本与单一引用
# ---------------------------------------------------------------------------

class TestSideOutputVersioning:
    def test_publish_versions_and_current_ref(self):
        v1 = side_output_service.publish("m", [(0, 10)], source="p1")
        v2 = side_output_service.publish("m", [(0, 20)], source="p1")
        assert v1["version"] == 1 and v2["version"] == 2
        assert v2["status"] == "current"
        # 重新读取历史版本，旧版本应已被取代
        versions = side_output_service.versions("m")
        assert versions[0]["status"] == "superseded"
        ref = side_output_service.current_version_ref("m")
        assert ref["version"] == 2
        assert ref["ranges"] == [[0, 20]]

    def test_merged_job_references_single_current_version(self, service):
        side_output_service.publish("m", [(0, 50)], source="auto")
        service.submit_backfill("m", 0, 50, trigger_source="auto", request_key="a1")
        side_output_service.publish("m", [(0, 90)], source="auto")
        service.submit_backfill("m", 40, 90, trigger_source="auto", request_key="a2")
        job = service._external_active_jobs("m")[0]
        ref = job["_side_output_ref"]
        assert ref["version"] == 2, "合并后只能引用一个当前旁路版本"
        # 清单保留来源版本历史
        versions = {s["version"] for s in job["_source_versions"]}
        assert versions == {1, 2}

    def test_pause_keeps_unique_reference(self, service):
        side_output_service.publish("m", [(0, 30)])
        r = service.submit_backfill("m", 0, 30, request_key="p")
        job = store.find("backfill_job", r["job_id"])
        job["status"] = "补数中"
        before = job["_side_output_ref"]["version"]
        service.pause_job(r["job_id"])
        service.resume_job(r["job_id"])
        assert job["_side_output_ref"]["version"] == before
        assert len({j["_side_output_ref"]["version"]
                    for j in service._external_active_jobs("m")}) == 1


# ---------------------------------------------------------------------------
# 3. 自动与人工补数相遇：两种修复顺序
# ---------------------------------------------------------------------------

class TestAutoManualConvergence:
    def test_absorb_keeps_manual_job(self, service):
        """人工补数与自动补数同时到达：保留人工作业并吸收自动范围。"""
        manual = service.submit_backfill("m", 0, 100, trigger_source="manual",
                                         request_key="man", job_name="人工补数")
        auto = service.submit_backfill("m", 80, 200, trigger_source="auto",
                                       request_key="auto1", mode="absorb")
        active = service._external_active_jobs("m")
        assert len(active) == 1
        assert active[0]["id"] == manual["job_id"], "必须保留人工作业"
        assert service._active_ranges(active[0]) == [(0, 200)]
        assert auto["mode"] == "absorb"

    def test_auto_first_manual_still_wins(self, service):
        """自动先到、人工后到：人工仍成为主作业。"""
        service.submit_backfill("m", 0, 50, trigger_source="auto", request_key="a")
        manual = service.submit_backfill("m", 30, 90, trigger_source="manual",
                                         request_key="b", job_name="人工补数")
        active = service._external_active_jobs("m")
        assert len(active) == 1
        assert active[0]["id"] == manual["job_id"]
        assert active[0]["_trigger_source"] == "manual"

    def test_parent_child_mode_converges_to_one_scope(self, service):
        """父子作业表达两次触发，对外结果收敛到一个活动范围。"""
        parent = service.submit_backfill("m", 0, 100, trigger_source="manual",
                                         request_key="man", mode="parent_child")
        child = service.submit_backfill("m", 90, 160, trigger_source="auto",
                                        request_key="auto", mode="parent_child")
        # 对外活动作业只有父作业
        external = service._external_active_jobs("m")
        assert len(external) == 1
        assert external[0]["id"] == parent["job_id"]
        assert child["mode"] == "parent_child"
        detail = service.job_detail(parent["job_id"])["job"]
        assert len(detail["child_jobs"]) == 1
        # 父作业活动范围吸收子作业区间
        assert service._active_ranges(external[0]) == [(0, 160)]
        # 复核清单认为活动范围唯一
        review = service.review_checklist("m")
        assert review["all_converged"] is True

    def test_two_repair_orders_produce_documented_ledger_versions(self, service):
        """两种修复顺序在补数任务清单中留下不同结果版本的台账记录。"""
        service.submit_backfill("m", 0, 10, trigger_source="manual", request_key="o1")
        service.submit_backfill("m", 5, 20, trigger_source="auto",
                                request_key="o2", mode="absorb")
        absorb_actions = [e["action"] for e in store.repair_ledger("backfill_job")]
        assert "absorb_backfill" in absorb_actions

        store._repair_ledger.clear()
        service.submit_backfill("m2", 0, 10, trigger_source="manual",
                                request_key="o3", mode="parent_child")
        service.submit_backfill("m2", 5, 20, trigger_source="auto",
                                request_key="o4", mode="parent_child")
        pc_actions = [e["action"] for e in store.repair_ledger("backfill_job")]
        assert "parent_child_backfill" in pc_actions


# ---------------------------------------------------------------------------
# 4. 暂停/恢复：完成比例不倒退
# ---------------------------------------------------------------------------

class TestPauseResume:
    def _start(self, service, metric="m", lo=0, hi=100):
        r = service.submit_backfill(metric, lo, hi, request_key="k")
        # 经启动动作进入补数中，模拟真实暂停/恢复链路
        service.run_action(r["job_id"], "启动补数")
        job = store.find("backfill_job", r["job_id"])
        assert job["status"] == "补数中"
        return job

    def test_pause_resume_progress_never_regresses(self, service):
        job = self._start(service)
        # 推进到 15%
        service.track_progress(job["作业名称"])
        assert job["进度比例"] == "15%"
        paused = service.pause_job(job["id"])
        assert paused["status"] == "已暂停"
        assert paused["paused_progress_pct"] == 15
        # 暂停期间 track_progress 不推进也不倒退
        service.track_progress(job["作业名称"])
        assert job["进度比例"] == "15%"
        service.resume_job(job["id"])
        service.track_progress(job["作业名称"])
        assert int(job["进度比例"].replace("%", "")) >= 30
        review = service.review_checklist("m")
        assert review["items"][0]["progress_non_regressive"] is True

    def test_pause_preserves_active_scope(self, service):
        job = self._start(service)
        service.pause_job(job["id"])
        assert service._active_ranges(job) == [(0, 100)]
        service.resume_job(job["id"])
        assert service._active_ranges(job) == [(0, 100)]

    def test_action_routes_pause_and_resume(self, service):
        job = self._start(service)
        entry, msg = service.run_action(job["id"], "暂停补数")
        assert entry["status"] == "已暂停"
        entry, msg = service.run_action(job["id"], "启动补数")
        assert entry["status"] == "补数中"


# ---------------------------------------------------------------------------
# 5. 并发重试只生效一次
# ---------------------------------------------------------------------------

class TestConcurrentRetry:
    def test_concurrent_retry_same_token_applies_once(self, service):
        r = service.submit_backfill("m", 0, 50, request_key="r")
        job = store.find("backfill_job", r["job_id"])
        job["status"] = "部分失败"
        job["abnormal"] = True
        outcomes: list[dict] = []
        barrier = threading.Barrier(5)

        def worker():
            barrier.wait()
            outcomes.append(service.retry_job(r["job_id"], retry_token="token-1"))

        threads = [threading.Thread(target=worker) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        applied = [o for o in outcomes if not o.get("idempotent")]
        assert len(applied) == 1, "并发重试只能有一次真正生效"
        assert job["_retry_token"] == "token-1"

    def test_retry_does_not_requeue_completed_ranges(self, service):
        r = service.submit_backfill("m", 0, 20, request_key="r2")
        job = store.find("backfill_job", r["job_id"])
        job["_completed_ranges"] = [[0, 20]]
        job["_active_ranges"] = []
        result = service.retry_job(r["job_id"], retry_token="t2")
        assert result["requeued"] is False
        assert "已完成区间" in result["reason"]


# ---------------------------------------------------------------------------
# 6. 事务中断回滚
# ---------------------------------------------------------------------------

class TestTransactionRollback:
    def test_submit_abort_rolls_back_to_pre_fix_version(self, service):
        service.submit_backfill("m", 0, 10, request_key="base")
        before_ids = [j["id"] for j in store.rows("backfill_job")]
        with pytest.raises(RuntimeError, match="回滚"):
            service.submit_backfill("m", 0, 30, request_key="abort-1", abort=True)
        after_ids = [j["id"] for j in store.rows("backfill_job")]
        assert before_ids == after_ids, "事务中断后必须回到修复前版本"
        # 台账也一并回滚
        assert all(e["detail"].get("request_key") != "abort-1"
                   for e in store.repair_ledger())

    def test_merge_abort_rolls_back(self, service):
        # 直接构造重叠作业，保证合并前确实存在两个活动作业
        rows = store.rows("backfill_job")
        base = max((int(r.get("id", 0)) for r in rows), default=0)
        for idx, (lo, hi) in enumerate([(0, 10), (5, 20)]):
            rows.append({
                "id": base + 1 + idx,
                "作业名称": f"回滚作业{idx}", "目标指标": "m",
                "补数范围": f"{lo}-{hi}", "起始位点": str(lo), "截止位点": str(hi),
                "进度比例": "0%", "status": "待执行", "pending": True,
                "abnormal": False,
                "_active_ranges": [[lo, hi]], "_completed_ranges": [],
                "_trigger_source": "manual", "_request_keys": [f"rb{idx}"],
                "_merge_logs": [], "_source_versions": [], "_child_jobs": [],
            })
        before = len(service._external_active_jobs("m"))
        assert before == 2
        with pytest.raises(RuntimeError):
            service.merge_overlapping_jobs(abort=True)
        assert len(service._external_active_jobs("m")) == before


# ---------------------------------------------------------------------------
# 7. 迟到处理调用链与自动触发
# ---------------------------------------------------------------------------

class TestLateHandlerChain:
    def test_auto_trigger_is_idempotent(self, late):
        r1 = late.trigger_backfill_for_late("迟到处理样例2", 10, "src1",
                                            target_metric="metric_x")
        r2 = late.trigger_backfill_for_late("迟到处理样例2", 10, "src1",
                                            target_metric="metric_x")
        assert r1["triggered"] is True
        assert r2["idempotent"] is True
        assert r1["backfill_job_id"] == r2["backfill_job_id"]
        jobs = [j for j in store.rows("backfill_job")
                if j.get("目标指标") == "metric_x" and not j.get("_parent_job")]
        assert len(jobs) == 1
        assert r1["side_output_version"] is not None

    def test_pipeline_chain_idempotent_and_versions(self):
        entries = [
            LateDataEntry(original={"id": i}, event_time=float(100 + i),
                          expected_watermark=200.0, bypass_reason="late")
            for i in range(5)
        ]
        r1 = create_backfill_from_late(entries, "metric_p", "src")
        r2 = create_backfill_from_late(entries, "metric_p", "src")
        assert r1["created"] is True
        assert r2["idempotent"] is True
        versions = side_output_service.versions("metric_p")
        assert len(versions) == 2  # 每次发布都留版本，作业只引用当前版本
        job = store.find("backfill_job", r1["job_id"])
        assert job["_side_output_ref"]["version"] == 2

    def test_watermark_to_backfill_full_chain(self, service):
        events = [
            {"event_id": f"e{i}", "event_time": 100.0 + i,
             "payload": {"user_id": "u1", "amount": 1.0}}
            for i in range(10)
        ]
        late_entries, ratio = check_watermark_delay(
            "src", 200.0, events, allowed_lateness_sec=1.0)
        assert len(late_entries) > 0
        result = create_backfill_from_late(late_entries, "metric_chain", "src")
        assert result["created"] is True
        review = service.review_checklist("metric_chain")
        assert review["all_converged"] is True


# ---------------------------------------------------------------------------
# 8. 残留数据、详情面板、台账、复核
# ---------------------------------------------------------------------------

class TestResidualAndReview:
    def test_residual_rows_marked_ineffective(self, service):
        # 模拟6号残留：位点字段不可解析
        store.rows("backfill_job").append({
            "id": 999, "作业名称": "残留作业", "目标指标": "metric_r",
            "补数范围": "补数任务样例1", "起始位点": "补数任务样例1",
            "截止位点": "补数任务样例1", "进度比例": "补数任务样例1",
            "status": "待执行", "pending": True, "abnormal": False,
        })
        result = service.cleanup_residual()
        assert any(x["job_id"] == 999 for x in result["residual_jobs"])
        # 残留不参与去重计算，但仍在待处理清单中显式标记为不生效
        pending = service.list_pending_backfills()
        residual_entry = next(p for p in pending if p["job_id"] == 999)
        assert residual_entry["residual"] is True
        assert residual_entry["active_ranges"] == []
        # 详情面板可复核不生效项
        detail = service.job_detail(999)
        assert detail["ok"] is True
        assert detail["job"]["ineffective"] is True

    def test_detail_returns_full_result(self, service):
        side_output_service.publish("m", [(0, 10)])
        r = service.submit_backfill("m", 0, 10, request_key="d",
                                    job_name="详情作业")
        detail = service.job_detail(r["job_id"])["job"]
        for key in ("active_ranges", "completed_ranges", "side_output_ref",
                    "source_versions", "merge_logs", "request_keys",
                    "trigger_source", "status"):
            assert key in detail
        assert detail["side_output_ref"]["version"] is not None

    def test_review_checklist_flags_duplicates_and_refs(self, service):
        side_output_service.publish("m", [(0, 10)])
        service.submit_backfill("m", 0, 10, request_key="x")
        # 直接构造第二个重叠活动作业，模拟提交入口合并前的清单状态
        rows = store.rows("backfill_job")
        rows.append({
            "id": max((int(r.get("id", 0)) for r in rows), default=0) + 1,
            "作业名称": "复核重叠作业", "目标指标": "m",
            "补数范围": "8-30", "起始位点": "8", "截止位点": "30",
            "进度比例": "0%", "status": "待执行", "pending": True,
            "abnormal": False,
            "_active_ranges": [[8, 30]], "_completed_ranges": [],
            "_trigger_source": "auto", "_request_keys": ["y"],
            "_merge_logs": [], "_source_versions": [], "_child_jobs": [],
        })
        review = service.review_checklist()
        item = next(i for i in review["items"] if i["target_metric"] == "m")
        assert item["unique_active_scope"] is False
        assert item["active_job_count"] == 2
        assert item["single_side_output_ref"] is True
        # 合并后复核收敛
        service.merge_overlapping_jobs("m")
        review2 = service.review_checklist("m")
        assert review2["all_converged"] is True

    def test_repair_ledger_shared_across_modules(self, late):
        late.trigger_backfill_for_late("迟到处理样例2", 5, "src",
                                       target_metric="metric_l")
        backfill_entries = store.repair_ledger("backfill_job")
        late_entries = store.repair_ledger("late_handler")
        assert len(backfill_entries) >= 1
        assert len(late_entries) >= 1
        # 台账记录关联模块，其他模块可同步查询同一修复
        assert "side_output" in backfill_entries[0]["related_modules"]
        all_entries = store.repair_ledger()
        # 同一次修复在两个模块都留有台账，且通过 related_modules 互相可见
        assert any(e["module"] == "backfill_job" for e in all_entries)
        assert any(e["module"] == "late_handler" for e in all_entries)
        assert any("side_output" in e["related_modules"] for e in all_entries)

    def test_validate_range_reports_overlap_and_completed(self, service):
        r = service.submit_backfill("m", 0, 100, request_key="v")
        job = store.find("backfill_job", r["job_id"])
        job["_completed_ranges"] = [[0, 40]]
        job["_active_ranges"] = [[41, 100]]
        overlap = service.validate_backfill_range("m", "50", "60")
        assert overlap["ok"] is False
        assert any("重叠" in i for i in overlap["issues"])
        done = service.validate_backfill_range("m", "0", "40")
        assert any("已全部完成" in i for i in done["issues"])
        bad = service.validate_backfill_range("m", "a", "b")
        assert any("整数" in i for i in bad["issues"])
