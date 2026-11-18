"""迟到旁路数据服务：版本化管理迟到处理链路写入的旁路数据。

数据流角色: late_handler / watermark_track（生产者）→ side_output（版本仓库）
            → backfill_job（按目标指标和位点范围消费）

序号6修复要点:
  - 每次迟到旁路发布生成单调递增版本，旧版本标记 superseded；
  - 合并后的补数作业只能引用一个当前迟到旁路数据版本；
  - 暂停、合并后作业保持唯一引用，并在补数任务清单保留来源版本。
"""
from __future__ import annotations

from typing import Any

from app.store import merge_ranges, store


class SideOutputService:
    """迟到旁路数据版本服务（无独立前端模块，作为 backfill_job 的上游仓库）。"""

    def side_output_key(self, target_metric: str) -> str:
        return f"late_side_output::{target_metric}"

    def publish(
        self,
        target_metric: str,
        ranges: list[tuple[int, int]],
        *,
        source: str = "",
        entries: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """发布迟到旁路数据新版本，返回当前版本记录。"""
        key = self.side_output_key(target_metric)
        return store.publish_side_output(
            key, target_metric, ranges, source=source, entries=entries,
        )

    def current_version(self, target_metric: str) -> dict[str, Any] | None:
        """读取目标指标当前迟到旁路数据版本。"""
        return store.current_side_output(self.side_output_key(target_metric))

    def versions(self, target_metric: str) -> list[dict[str, Any]]:
        """列出目标指标的全部旁路版本（含已被取代的历史版本）。"""
        return store.side_output_versions(self.side_output_key(target_metric))

    def current_version_ref(self, target_metric: str) -> dict[str, Any]:
        """返回作业可引用的单一版本描述；无版本时给出空引用占位。"""
        current = self.current_version(target_metric)
        if current is None:
            return {
                "side_output_key": self.side_output_key(target_metric),
                "version": None,
                "ranges": [],
                "status": "absent",
            }
        return {
            "side_output_key": current["side_output_key"],
            "version": current["version"],
            "ranges": current["ranges"],
            "status": current["status"],
        }

    @staticmethod
    def merge_consume_ranges(
        ranges: list[tuple[int, int]],
    ) -> list[tuple[int, int]]:
        """补数任务消费旁路数据前先合并请求范围，避免重复消费。"""
        return merge_ranges(ranges)


side_output_service = SideOutputService()
