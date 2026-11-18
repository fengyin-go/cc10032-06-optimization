"""内存数据仓库：给每个业务模块准备一份可筛选、可流转的示例数据。

真实项目里这里会换成数据库访问层；当前实现只依赖标准库，保证克隆下来就能起。

增强: DataChannel 跨模块数据通道 + FailureRecovery 失败恢复机制
"""
from __future__ import annotations

import contextlib
import copy
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Iterator

from app.seed import SEED_ROWS


def merge_ranges(ranges: list[tuple[int, int]]) -> list[tuple[int, int]]:
    """把若干整数位点区间合并为互不重叠的最小区间集合（闭区间语义）。

    非法（起点大于终点）区间会被丢弃；相邻区间（如 [1,5] 与 [6,10]）也会
    合并为 [1,10]，保证重叠范围最终只对应一个可执行作业。
    """
    cleaned = sorted(
        (int(lo), int(hi)) for lo, hi in ranges if int(lo) <= int(hi)
    )
    merged: list[tuple[int, int]] = []
    for lo, hi in cleaned:
        if not merged or lo > merged[-1][1] + 1:
            merged.append((lo, hi))
        else:
            merged[-1] = (merged[-1][0], max(merged[-1][1], hi))
    return merged


def subtract_ranges(
    pending: list[tuple[int, int]],
    covered: list[tuple[int, int]],
) -> list[tuple[int, int]]:
    """从待处理区间集合中扣除已覆盖（已完成）区间，返回剩余待处理区间。

    序号6要求“已完成区间不能被重新计入待处理”，重复补报时先做差集再合并。
    """
    covered = merge_ranges(covered)
    result: list[tuple[int, int]] = []
    for lo, hi in merge_ranges(pending):
        segments = [(lo, hi)]
        for clo, chi in covered:
            next_segments: list[tuple[int, int]] = []
            for slo, shi in segments:
                if chi < slo or clo > shi:
                    next_segments.append((slo, shi))
                else:
                    if clo > slo:
                        next_segments.append((slo, clo - 1))
                    if chi < shi:
                        next_segments.append((chi + 1, shi))
            segments = next_segments
        result.extend(segments)
    return merge_ranges(result)


@dataclass
class DataChannelEvent:
    """跨模块数据通道事件：模块间数据传递的统一载体。"""
    channel_id: str
    source_module: str
    target_module: str
    data_type: str
    payload: dict[str, Any]
    timestamp: float
    status: str = "pending"  # pending, delivered, consumed, failed
    retry_count: int = 0
    max_retries: int = 3
    _history: list[dict[str, Any]] = field(default_factory=list)

    def mark_delivered(self) -> None:
        self.status = "delivered"
        self._history.append({"status": "delivered", "ts": time.time()})

    def mark_consumed(self, consumer_response: dict[str, Any] | None = None) -> None:
        self.status = "consumed"
        self._history.append({
            "status": "consumed",
            "ts": time.time(),
            "response": consumer_response,
        })

    def mark_failed(self, error: str) -> None:
        self.status = "failed"
        self._history.append({"status": "failed", "ts": time.time(), "error": error})

    def can_retry(self) -> bool:
        return self.retry_count < self.max_retries


@dataclass
class FailureRecovery:
    """失败恢复记录：跟踪模块间数据流中的失败并执行恢复。"""
    recovery_id: str
    failed_module: str
    failed_operation: str
    error_detail: str
    recovery_strategy: str  # retry, skip, checkpoint, escalate
    context: dict[str, Any]
    status: str = "open"  # open, recovering, recovered, escalated
    created_at: float = field(default_factory=time.time)
    resolved_at: float | None = None

    def attempt_recovery(self) -> dict[str, Any]:
        self.status = "recovering"
        if self.recovery_strategy == "retry":
            return {"action": "retry", "context": self.context, "recovery_id": self.recovery_id}
        elif self.recovery_strategy == "skip":
            self.status = "recovered"
            self.resolved_at = time.time()
            return {"action": "skip", "recovery_id": self.recovery_id}
        elif self.recovery_strategy == "checkpoint":
            return {"action": "restore_from_checkpoint", "recovery_id": self.recovery_id}
        else:  # escalate
            self.status = "escalated"
            return {"action": "escalate", "recovery_id": self.recovery_id}

    def mark_resolved(self) -> None:
        self.status = "recovered"
        self.resolved_at = time.time()


class Store:
    def __init__(self) -> None:
        self._tables: dict[str, list[dict[str, Any]]] = {
            name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()
        }
        # 跨模块数据通道：channel_id → DataChannelEvent
        self._channels: dict[str, DataChannelEvent] = {}
        # 失败恢复记录：recovery_id → FailureRecovery
        self._failures: dict[str, FailureRecovery] = {}
        # 序号6：迟到旁路数据版本（side_output_key → 版本列表，最新在末尾）
        self._side_outputs: dict[str, list[dict[str, Any]]] = {}
        # 序号6：修复台账，跨模块共享的修复/合并审计记录
        self._repair_ledger: list[dict[str, Any]] = []
        # 序号6：补数去重并发修复的串行化锁（并发重试/触发只生效一次）
        self._backfill_lock = threading.RLock()
        # 序号6：事务中断标记，事务内置位后回滚到修复前版本
        self._txn_abort: dict[str, str] = {}

    # ---- 序号6：并发控制与事务 ----

    @property
    def backfill_lock(self) -> threading.RLock:
        """补数去重流程的串行化锁，保证并发修复只应用一次且保持幂等。"""
        return self._backfill_lock

    @contextlib.contextmanager
    def transaction(self, label: str = "backfill") -> Iterator[None]:
        """轻量事务：进入时快照全部业务表，异常或主动中断时整体回滚。

        序号6要求“事务中断后回到修复前版本”，这里对表数据做深拷贝快照，
        回滚时连同迟到旁路版本与修复台账一并还原。
        """
        snapshot = {
            "tables": copy.deepcopy(self._tables),
            "side_outputs": copy.deepcopy(self._side_outputs),
            "ledger": copy.deepcopy(self._repair_ledger),
        }
        try:
            yield
            if label in self._txn_abort:
                reason = self._txn_abort.pop(label)
                self._restore_snapshot(snapshot)
                raise RuntimeError(f"事务「{label}」中断，已回滚到修复前版本：{reason}")
        except Exception:
            self._txn_abort.pop(label, None)
            self._restore_snapshot(snapshot)
            raise

    def abort_transaction(self, label: str, reason: str) -> None:
        """在事务内主动标记中断，由事务上下文统一回滚。"""
        self._txn_abort[label] = reason

    def _restore_snapshot(self, snapshot: dict[str, Any]) -> None:
        self._tables = snapshot["tables"]
        self._side_outputs = snapshot["side_outputs"]
        self._repair_ledger = snapshot["ledger"]

    # ---- 序号6：迟到旁路数据版本 ----

    def publish_side_output(
        self,
        side_output_key: str,
        target_metric: str,
        ranges: list[tuple[int, int]],
        source: str = "",
        entries: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """发布一个迟到旁路数据新版本。

        补数任务按目标指标和位点范围消费迟到旁路数据，每次发布得到单调递增
        的版本号；合并后的作业只允许引用一个当前版本。
        """
        versions = self._side_outputs.setdefault(side_output_key, [])
        version = len(versions) + 1
        merged = merge_ranges(ranges)
        record = {
            "side_output_key": side_output_key,
            "target_metric": target_metric,
            "version": version,
            "ranges": [[lo, hi] for lo, hi in merged],
            "source": source,
            "entry_count": len(entries or []),
            "entries": copy.deepcopy(entries or []),
            "created_at": time.time(),
            "status": "current",
        }
        for prev in versions:
            prev["status"] = "superseded"
        versions.append(record)
        # 返回深拷贝前重新读取，确保上一版本的 superseded 状态同步到拷贝
        return copy.deepcopy(versions[-1])

    def current_side_output(self, side_output_key: str) -> dict[str, Any] | None:
        """读取迟到旁路数据的当前版本（不存在返回 None）。"""
        versions = self._side_outputs.get(side_output_key)
        if not versions:
            return None
        return copy.deepcopy(versions[-1])

    def side_output_versions(self, side_output_key: str) -> list[dict[str, Any]]:
        """列出某一旁路键的全部版本，供补数工作台核对来源版本。"""
        return copy.deepcopy(self._side_outputs.get(side_output_key, []))

    def list_side_outputs(self) -> list[dict[str, Any]]:
        """列出全部旁路键的当前版本。"""
        return [
            copy.deepcopy(versions[-1])
            for versions in self._side_outputs.values()
            if versions
        ]

    # ---- 序号6：修复台账（其他模块同步更新）----

    def append_repair_ledger(self, entry: dict[str, Any]) -> dict[str, Any]:
        """追加修复台账记录，供各模块同步登记同一次缺陷修复。"""
        record = {
            "ledger_id": len(self._repair_ledger) + 1,
            "created_at": time.time(),
            **entry,
        }
        self._repair_ledger.append(record)
        return copy.deepcopy(record)

    def repair_ledger(self, module: str | None = None) -> list[dict[str, Any]]:
        """读取修复台账，可按模块过滤。"""
        if module is None:
            return copy.deepcopy(self._repair_ledger)
        return copy.deepcopy([
            r for r in self._repair_ledger
            if r.get("module") == module or module in r.get("related_modules", [])
        ])

    # ---- 跨模块数据通道 ----

    def send_to_channel(
        self,
        source: str,
        target: str,
        data_type: str,
        payload: dict[str, Any],
    ) -> DataChannelEvent:
        """通过数据通道将数据从一个模块发送到另一个模块。

        创建 DataChannelEvent 记录并返回，下游模块通过 consume_from_channel 消费。
        """
        import hashlib
        channel_id = hashlib.md5(
            f"{source}_{target}_{data_type}_{time.time()}".encode()
        ).hexdigest()[:16]
        event = DataChannelEvent(
            channel_id=channel_id,
            source_module=source,
            target_module=target,
            data_type=data_type,
            payload=payload,
            timestamp=time.time(),
        )
        self._channels[channel_id] = event
        # 同时写入下游模块的 store 表
        notification = {
            "id": max(
                (int(r.get("id", 0)) for r in self.rows(target)), default=0
            ) + 1,
            "channel_id": channel_id,
            "source": source,
            "data_type": data_type,
            "payload_snapshot": copy.deepcopy(payload),
            "timestamp": event.timestamp,
            "status": "pending",
            "pending": True,
            "abnormal": False,
        }
        self.rows(target).append(notification)
        return event

    def consume_from_channel(
        self, channel_id: str, consumer_module: str,
    ) -> DataChannelEvent | None:
        """消费数据通道中的事件。"""
        event = self._channels.get(channel_id)
        if event is None:
            return None
        if event.target_module != consumer_module:
            return None
        event.mark_consumed()
        # 更新下游模块表中的通知状态
        for row in self.rows(consumer_module):
            if row.get("channel_id") == channel_id:
                row["status"] = "consumed"
                row["pending"] = False
                break
        return event

    def get_pending_channels(self, target_module: str) -> list[DataChannelEvent]:
        """获取目标模块中待消费的数据通道事件。"""
        return [
            e for e in self._channels.values()
            if e.target_module == target_module and e.status == "pending"
        ]

    # ---- 失败恢复 ----

    def register_failure(
        self,
        module: str,
        operation: str,
        error: str,
        strategy: str = "retry",
        context: dict[str, Any] | None = None,
    ) -> FailureRecovery:
        """注册失败事件并创建恢复记录。"""
        import hashlib
        rid = hashlib.md5(
            f"{module}_{operation}_{time.time()}".encode()
        ).hexdigest()[:12]
        recovery = FailureRecovery(
            recovery_id=rid,
            failed_module=module,
            failed_operation=operation,
            error_detail=error,
            recovery_strategy=strategy,
            context=context or {},
        )
        self._failures[rid] = recovery
        return recovery

    def get_open_failures(self, module: str | None = None) -> list[FailureRecovery]:
        """获取未恢复的失败记录。"""
        failures = [
            f for f in self._failures.values()
            if f.status in ("open", "recovering")
        ]
        if module:
            failures = [f for f in failures if f.failed_module == module]
        return failures

    def resolve_failure(self, recovery_id: str) -> FailureRecovery | None:
        """标记失败为已恢复。"""
        recovery = self._failures.get(recovery_id)
        if recovery:
            recovery.mark_resolved()
        return recovery

    def get_failure_stats(self) -> dict[str, Any]:
        """获取失败恢复统计。"""
        all_failures = list(self._failures.values())
        open_count = sum(1 for f in all_failures if f.status in ("open", "recovering"))
        resolved_count = sum(1 for f in all_failures if f.status == "recovered")
        escalated_count = sum(1 for f in all_failures if f.status == "escalated")
        return {
            "total": len(all_failures),
            "open": open_count,
            "resolved": resolved_count,
            "escalated": escalated_count,
        }

    # ---- 基础 CRUD ----

    def module_names(self) -> list[str]:
        return sorted(self._tables)

    def rows(self, module: str) -> list[dict[str, Any]]:
        return self._tables.setdefault(module, [])

    def find(self, module: str, entry_id: int) -> dict[str, Any] | None:
        for row in self.rows(module):
            if int(row.get("id", 0)) == entry_id:
                return row
        return None

    def overview(self) -> dict[str, object]:
        modules: list[dict[str, object]] = []
        for name in self.module_names():
            rows = self.rows(name)
            modules.append({
                "name": name,
                "created": len(rows),
                "pending": sum(1 for row in rows if row.get("pending")),
                "abnormal": sum(1 for row in rows if row.get("abnormal")),
            })
        cards = [
            {"label": "业务模块", "value": len(modules)},
            {"label": "今日新增", "value": sum(int(item["created"]) for item in modules)},
            {"label": "待处理", "value": sum(int(item["pending"]) for item in modules)},
            {"label": "异常量", "value": sum(int(item["abnormal"]) for item in modules)},
        ]
        # 附加数据通道和失败恢复统计
        channel_stats = {
            "active_channels": len(self._channels),
            "pending_channels": sum(
                1 for e in self._channels.values() if e.status == "pending"
            ),
        }
        failure_stats = self.get_failure_stats()
        return {
            "cards": cards,
            "modules": modules,
            "channel_stats": channel_stats,
            "failure_stats": failure_stats,
        }


store = Store()