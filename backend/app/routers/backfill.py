"""补数任务专用接口：序号6去重流程的工作台入口。

在通用资源路由之外提供：
  - 统一提交（人工/自动）、清单合并清理、暂停/恢复/重试
  - 详情面板完整结果、补数任务清单复核
  - 迟到旁路数据版本、修复台账查询
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.services.backfill_job import BackfillJobService
from app.services.late_handler import LateHandlerService
from app.services.side_output import side_output_service
from app.store import store

router = APIRouter(prefix="/api/backfill", tags=["补数任务序号6"])

service = BackfillJobService()
late_service = LateHandlerService()


class SubmitPayload(BaseModel):
    target_metric: str
    start_offset: int
    end_offset: int
    trigger_source: str = "manual"
    request_key: str | None = None
    job_name: str | None = None
    mode: str = "absorb"
    abort: bool = False


class MergePayload(BaseModel):
    target_metric: str | None = None
    abort: bool = False


class RetryPayload(BaseModel):
    retry_token: str | None = None


class AutoTriggerPayload(BaseModel):
    policy_name: str
    late_count: int
    source_name: str
    target_metric: str | None = None
    request_key: str | None = None
    mode: str = "absorb"
    start_offset: int = 0
    end_offset: int | None = None


def _require_ok(result: dict[str, Any]) -> dict[str, Any]:
    if not result.get("ok"):
        raise HTTPException(status_code=400, detail=result.get("error", "操作失败"))
    return result


@router.post("/submit")
def submit_backfill(payload: SubmitPayload) -> dict[str, Any]:
    """统一补数提交：重叠范围合并、已完成区间扣减、幂等。"""
    try:
        return service.submit_backfill(
            payload.target_metric,
            payload.start_offset,
            payload.end_offset,
            trigger_source=payload.trigger_source,
            request_key=payload.request_key,
            job_name=payload.job_name,
            mode=payload.mode,
            abort=payload.abort,
        )
    except RuntimeError as exc:
        # 事务中断已在服务层整体回滚到修复前版本，这里返回结构化结果而非 500
        raise HTTPException(status_code=409, detail={
            "ok": False,
            "rolled_back": True,
            "message": str(exc),
        })


@router.post("/merge")
def merge_jobs(payload: MergePayload) -> dict[str, Any]:
    """清理补数任务清单：同目标指标重叠作业合并为一个可执行作业。"""
    try:
        return service.merge_overlapping_jobs(payload.target_metric, abort=payload.abort)
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail={
            "ok": False,
            "rolled_back": True,
            "message": str(exc),
        })


@router.post("/{job_id}/pause")
def pause_job(job_id: int) -> dict[str, Any]:
    return _require_ok(service.pause_job(job_id))


@router.post("/{job_id}/resume")
def resume_job(job_id: int) -> dict[str, Any]:
    return _require_ok(service.resume_job(job_id))


@router.post("/{job_id}/retry")
def retry_job(job_id: int, payload: RetryPayload) -> dict[str, Any]:
    """并发重试携带相同令牌只生效一次。"""
    return service.retry_job(job_id, payload.retry_token)


@router.get("/{job_id}/detail")
def job_detail(job_id: int) -> dict[str, Any]:
    """6号详情面板：修复后接口返回完整结果。"""
    return _require_ok(service.job_detail(job_id))


@router.get("/review/checklist")
def review_checklist(target_metric: str | None = None) -> dict[str, Any]:
    """补数工作台复核：唯一活动范围、单一旁路引用、完成比例不倒退。"""
    return service.review_checklist(target_metric)


@router.get("/pending/list")
def pending_list() -> dict[str, Any]:
    return {"items": service.list_pending_backfills()}


@router.post("/residual/cleanup")
def residual_cleanup() -> dict[str, Any]:
    """标记并隔离6号残留数据，不生效项在详情面板可复核。"""
    return service.cleanup_residual()


@router.post("/auto-trigger")
def auto_trigger(payload: AutoTriggerPayload) -> dict[str, Any]:
    """迟到处理自动触发补数：发布旁路版本后走统一合并入口。"""
    return late_service.trigger_backfill_for_late(
        payload.policy_name,
        payload.late_count,
        payload.source_name,
        target_metric=payload.target_metric,
        request_key=payload.request_key,
        mode=payload.mode,
        start_offset=payload.start_offset,
        end_offset=payload.end_offset,
    )


@router.get("/side-output/versions")
def side_output_versions(target_metric: str | None = None) -> dict[str, Any]:
    """迟到旁路数据版本查询（当前版本 + 历史版本）。"""
    if target_metric is None:
        return {"items": store.list_side_outputs()}
    return {
        "target_metric": target_metric,
        "current": side_output_service.current_version(target_metric),
        "versions": side_output_service.versions(target_metric),
    }


@router.get("/repair/ledger")
def repair_ledger(module: str | None = None) -> dict[str, Any]:
    """修复台账：其他模块同步更新的修复记录。"""
    entries = store.repair_ledger(module)
    return {"total": len(entries), "items": entries}
