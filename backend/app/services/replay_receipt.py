"""重放血缘回执：死信处理与流式血缘共同读写的跨模块产物。

设计约束（对应修复要求）：
- 一份回执只归属一个重放批次，批次内最终只形成一个可查询的有效边集；
- 回执同时保留「重放批次」与「有效血缘边」，中断后凭回执即可恢复处理范围；
- 处置状态与有效血缘边必须在同一次提交里一起落库，失败整体回滚，
  不允许出现「状态已重放、边仍异常」的错位；
- 死信处理侧保留死信来源快照，流式血缘侧保留回执来源引用，两边各自可追溯。
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from app.store import store

RECEIPT_MODULE = "replay_lineage_receipts"
RECEIPT_REF_MODULE = "replay_lineage_receipt_refs"

# 回执生命周期：进行中（中断可恢复）→ 已完成
STATUS_RUNNING = "进行中"
STATUS_COMPLETED = "已完成"


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


class ReplayReceiptService:
    """重放血缘回执的唯一写入入口，死信/血缘两个模块都通过它落库。"""

    def list_receipts(self, *, dead_letter_id: int | None = None) -> list[dict[str, Any]]:
        rows = store.rows(RECEIPT_MODULE)
        if dead_letter_id is not None:
            rows = [row for row in rows if row.get("dead_letter_id") == dead_letter_id]
        return rows

    def get_receipt(self, receipt_id: int) -> dict[str, Any] | None:
        return store.find(RECEIPT_MODULE, receipt_id)

    def get_by_batch(self, batch_no: str) -> dict[str, Any] | None:
        for row in store.rows(RECEIPT_MODULE):
            if row.get("replay_batch") == batch_no:
                return row
        return None

    def running_receipt(self, dead_letter_id: int) -> dict[str, Any] | None:
        """中断恢复入口：找到该死信尚未完成的同批次回执。"""
        for row in store.rows(RECEIPT_MODULE):
            if row.get("dead_letter_id") == dead_letter_id and row.get("receipt_status") == STATUS_RUNNING:
                return row
        return None

    def create_receipt(self, dead_letter: dict[str, Any], *, resumed: bool = False) -> dict[str, Any]:
        """为一次重放建立批次回执；中断恢复时沿用原批次号。"""
        rows = store.rows(RECEIPT_MODULE)
        receipt_id = max((int(row.get("id", 0)) for row in rows), default=0) + 1
        source = dead_letter.get("记录编号", f"DEAD-{dead_letter.get('id', 0):04d}")
        if resumed:
            previous = self.running_receipt(int(dead_letter["id"]))
            batch_no = previous["replay_batch"] if previous else f"RPLB-{receipt_id:04d}"
        else:
            batch_no = f"RPLB-{receipt_id:04d}"
        receipt = {
            "id": receipt_id,
            "receipt_no": f"RCPT-{receipt_id:04d}",
            "replay_batch": batch_no,
            "receipt_status": STATUS_RUNNING,
            "dead_letter_id": dead_letter.get("id"),
            # 来源记录：死信处理侧保留的来源快照
            "source_record": {
                "dead_letter_id": dead_letter.get("id"),
                "record_code": source,
                "source_topic": dead_letter.get("原始主题"),
                "source_partition": dead_letter.get("原始分区"),
                "failure_reason": dead_letter.get("失败原因"),
                "payload": dead_letter.get("原始消息体"),
            },
            "deactivated_edges": [],
            "valid_edges": [],
            "edge_set_version": 0,
            "created_at": _now(),
            "updated_at": _now(),
            "completed_at": None,
        }
        rows.append(receipt)
        return receipt

    def attach_edges(
        self,
        receipt: dict[str, Any],
        *,
        deactivated: list[dict[str, Any]] | None = None,
        valid_edges: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """把本批次确认过的边写进回执，并推进有效边集版本。"""
        if deactivated:
            known = {edge["edge_code"] for edge in receipt["deactivated_edges"]}
            for edge in deactivated:
                if edge["edge_code"] not in known:
                    receipt["deactivated_edges"].append(self._edge_brief(edge))
                    known.add(edge["edge_code"])
        if valid_edges:
            receipt["valid_edges"] = [self._edge_brief(edge) for edge in valid_edges]
        receipt["edge_set_version"] += 1
        receipt["updated_at"] = _now()
        return receipt

    def complete(self, receipt: dict[str, Any], valid_edges: list[dict[str, Any]]) -> dict[str, Any]:
        """完成批次：固化唯一可查询的有效边集。"""
        receipt["valid_edges"] = [self._edge_brief(edge) for edge in valid_edges]
        receipt["receipt_status"] = STATUS_COMPLETED
        receipt["updated_at"] = _now()
        receipt["completed_at"] = _now()
        return receipt

    def record_lineage_ref(self, receipt: dict[str, Any], valid_edges: list[dict[str, Any]]) -> None:
        """流式血缘侧保留的回执来源引用，与死信侧回执一一对应。"""
        refs = store.rows(RECEIPT_REF_MODULE)
        ref_id = max((int(row.get("id", 0)) for row in refs), default=0) + 1
        refs.append({
            "id": ref_id,
            "receipt_id": receipt["id"],
            "replay_batch": receipt["replay_batch"],
            "source_topic": receipt["source_record"]["source_topic"],
            "edge_codes": [edge["edge_code"] for edge in valid_edges],
            "edge_set_version": receipt["edge_set_version"],
            "recorded_at": _now(),
        })

    def _edge_brief(self, edge: dict[str, Any]) -> dict[str, Any]:
        """回执只保留可查询的边摘要，避免与边表产生双写不一致。"""
        return {
            "edge_code": edge.get("edge_code"),
            "source_topic": edge.get("source_topic"),
            "target_table": edge.get("target_table"),
            "edge_status": edge.get("edge_status"),
            "edge_version": edge.get("edge_version"),
            "replay_batch": edge.get("replay_batch"),
        }


replay_receipt_service = ReplayReceiptService()
