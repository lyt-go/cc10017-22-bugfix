"""死信处理业务规则：状态流转、字段校验与筛选口径都收在这里。

修复点：「标记重放」不再只改处置状态，而是与流式血缘的有效边在同一次提交里落库——
读取死信来源记录生成重放血缘回执，先停用异常边、再建立重放边，全部成功后才把
处置状态置为已重放；任一步失败整体回滚，杜绝「页面已重放、图上仍异常」的错位。
中断后可凭进行中的回执恢复同一批次，不产生重复边。
"""
from __future__ import annotations

import copy
from typing import Any

from app.services.replay_receipt import replay_receipt_service
from app.services.stream_lineage import StreamLineageService
from app.store import store

MODULE = "dead_letter"
LIST_FIELDS = ["记录编号", "原始主题", "原始分区", "失败原因", "原始消息体", "重试次数", "入库时间", "处置状态"]
REQUIRED_FIELDS = ["记录编号", "原始主题", "原始分区"]
STATUS_ORDER = ["草稿", "待处置", "已重放", "已丢弃"]
ACTION_RULES = {"加入处置": "待处置", "标记重放": "已重放", "标记丢弃": "已丢弃"}
NEGATIVE_ACTIONS = []

# 与处置状态一起参与事务快照/回滚的表
_TX_TABLES = (
    MODULE,
    "stream_lineage_edges",
    "replay_lineage_receipts",
    "replay_lineage_receipt_refs",
)


class DeadLetterService:
    def __init__(self) -> None:
        self.lineage_service = StreamLineageService()

    def list_entries(
        self,
        *,
        filters: dict[str, str] | None = None,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = store.rows(MODULE)
        for field, expected in (filters or {}).items():
            rows = [row for row in rows if expected.casefold() in str(row.get(field, "")).casefold()]
        if keyword:
            rows = [row for row in rows if keyword.casefold() in str(row.get("记录编号", "")).casefold()]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = (page - 1) * size
        return rows[start:start + size], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(MODULE, entry_id)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [field for field in REQUIRED_FIELDS if not str(values.get(field) or "").strip()]
        if missing:
            return None, missing
        rows = store.rows(MODULE)
        entry = {"id": max((int(row.get("id", 0)) for row in rows), default=0) + 1}
        entry.update({field: values.get(field) for field in LIST_FIELDS})
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        rows.append(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"死信记录 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于死信处理可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"

        if action == "标记重放":
            receipt, message = self._replay(entry_id)
            if receipt is None:
                return None, message
            # 路由层的 entry 仍是死信记录本身，回执摘要随记录一起返回
            entry["replay_receipt"] = receipt
            return entry, message

        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"死信记录已{action}"

    # ── 重放血缘回执 ──────────────────────────────────────────

    def list_replay_receipts(self, entry_id: int | None = None) -> list[dict[str, Any]]:
        """死信处理侧查询回执：保留重放批次与有效血缘边。"""
        return replay_receipt_service.list_receipts(dead_letter_id=entry_id)

    def start_replay(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        """建立重放批次并先停用异常边；任务中断时处置状态保持待处置，可凭回执恢复。"""
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"死信记录 {entry_id} 不存在或已归档"
        if entry.get("status") == "已重放":
            receipt = replay_receipt_service.running_receipt(entry_id)
            if receipt is None:
                done = self.list_replay_receipts(entry_id)
                if done:
                    return done[-1], "死信记录已重放，回执可查"
            return receipt, "重放批次已存在，请恢复后继续处理"

        snapshot = self._snapshot()
        try:
            receipt = replay_receipt_service.running_receipt(entry_id)
            if receipt is None:
                receipt = replay_receipt_service.create_receipt(entry)
            # 固定顺序：先停用异常边，再建立重放边；启动阶段只完成第一步
            deactivated = self.lineage_service.deactivate_abnormal_edges(
                entry["原始主题"], receipt["replay_batch"])
            replay_receipt_service.attach_edges(receipt, deactivated=deactivated)
            entry["replay_batch"] = receipt["replay_batch"]
            return receipt, f"重放批次 {receipt['replay_batch']} 已建立，异常边已停用，待恢复处理"
        except Exception as exc:  # pragma: no cover - 防御性回滚
            self._restore(snapshot)
            return None, f"重放批次建立失败，已回滚：{exc}"

    def resume_replay(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        """中断恢复：沿用同一批次回执，补齐重放边并把处置状态置为已重放。"""
        return self._replay(entry_id, resumed=True)

    def replay(self, entry_id: int, *, resumed: bool = False) -> tuple[dict[str, Any] | None, str]:
        """显式重放入口（供恢复/补放场景调用），返回重放血缘回执。"""
        return self._replay(entry_id, resumed=resumed)

    def _replay(self, entry_id: int, *, resumed: bool = False) -> tuple[dict[str, Any] | None, str]:
        """重放处置：处置状态与有效血缘边在同一次提交里同时落库。"""
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"死信记录 {entry_id} 不存在或已归档"

        existing_running = replay_receipt_service.running_receipt(entry_id)
        if entry.get("status") == "已重放" and existing_running is None:
            done = self.list_replay_receipts(entry_id)
            if done:
                return done[-1], "死信记录已重放，重放血缘回执可查"
            return None, "死信记录已重放但缺少重放血缘回执，请重新发起重放"

        snapshot = self._snapshot()
        try:
            receipt = existing_running or replay_receipt_service.create_receipt(entry, resumed=resumed)
            topic = entry["原始主题"]
            batch_no = receipt["replay_batch"]

            # 第一步：停用该主题下仍残留的异常边（幂等，已停用不会重复）
            deactivated = self.lineage_service.deactivate_abnormal_edges(topic, batch_no)
            replay_receipt_service.attach_edges(receipt, deactivated=deactivated)

            # 第二步：建立本批次的重放边，冲突边先停用后建立，保证边集唯一且不重复
            valid_edges = self.lineage_service.apply_replay_receipt(topic, batch_no, entry_id)
            replay_receipt_service.attach_edges(receipt, valid_edges=valid_edges)

            # 第三步：回执固化有效边集，随后处置状态与边在同一次提交生效。
            # 该主题暂时没有已生效血缘记录时，有效边集为空但回执与状态仍然一致落库，
            # 后续血缘记录生效后可再次重放补齐，不会出现「状态已改、边没动」的错位。
            replay_receipt_service.complete(receipt, valid_edges)
            entry["status"] = "已重放"
            entry["pending"] = False
            entry["abnormal"] = False
            entry["处置状态"] = "已重放"
            entry["replay_batch"] = batch_no
            entry["replay_receipt_id"] = receipt["id"]
            edge_note = "，有效血缘边已同步" if valid_edges else "，暂无已生效血缘记录，有效边集为空待补"
            return receipt, f"死信记录已重放，批次 {batch_no}{edge_note}"
        except Exception as exc:
            self._restore(snapshot)
            return None, f"重放失败，处置状态与血缘边已整体回滚：{exc}"

    def _snapshot(self) -> dict[str, list[dict[str, Any]]]:
        return {name: copy.deepcopy(store.rows(name)) for name in _TX_TABLES}

    def _restore(self, snapshot: dict[str, list[dict[str, Any]]]) -> None:
        for name in _TX_TABLES:
            store.rows(name).clear()
            store.rows(name).extend(copy.deepcopy(snapshot[name]))
