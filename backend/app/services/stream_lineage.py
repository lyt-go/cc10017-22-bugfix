"""流式血缘 DAG 图服务：生成节点、边示例结构与流转规则。

修复点：边不再由节点相邻关系临时推导，而是落在独立的 ``stream_lineage_edges``
边表上，区分 有效 / 异常 / 已停用。死信重放通过重放血缘回执驱动这里的边切换，
图页面只渲染有效边，异常边清除后节点不再标异常。
"""
from __future__ import annotations

from typing import Any

from app.services.replay_receipt import replay_receipt_service
from app.store import store

MODULE = "stream_lineage"
EDGE_MODULE = "stream_lineage_edges"
REQUIRED_FIELDS = ["记录编号", "上游主题", "下游表名"]
STATUS_ORDER = ["草稿", "待校验", "已生效", "已停用"]
ACTION_RULES = {"提交校验": "待校验", "标记生效": "已生效", "停用记录": "已停用"}
NEGATIVE_ACTIONS = ["停用记录"]

EDGE_VALID = "有效"
EDGE_ABNORMAL = "异常"
EDGE_DEACTIVATED = "已停用"


def csv_cell(value: Any) -> str:
    text = str(value) if value is not None else ""
    if text and text[0] in "=+-@":
        text = "'" + text
    return '"' + text.replace('"', '""') + '"'


class StreamLineageService:
    # ── 边表读写 ──────────────────────────────────────────────

    def list_edges(self, *, active_only: bool = False) -> list[dict[str, Any]]:
        rows = store.rows(EDGE_MODULE)
        if active_only:
            return [row for row in rows if row.get("active")]
        return rows

    def valid_edges_for_topic(self, topic: str) -> list[dict[str, Any]]:
        """下游当前可查询的有效来源：只看有效且启用中的边。"""
        return [
            row for row in store.rows(EDGE_MODULE)
            if row.get("source_topic") == topic
            and row.get("edge_status") == EDGE_VALID
            and row.get("active")
        ]

    def abnormal_edges_for_topic(self, topic: str) -> list[dict[str, Any]]:
        return [
            row for row in store.rows(EDGE_MODULE)
            if row.get("source_topic") == topic
            and row.get("edge_status") == EDGE_ABNORMAL
            and row.get("active")
        ]

    def deactivate_abnormal_edges(self, topic: str, batch_no: str) -> list[dict[str, Any]]:
        """停用异常边（固定先做这一步），幂等：已停用的边不重复处理。"""
        changed: list[dict[str, Any]] = []
        for edge in self.abnormal_edges_for_topic(topic):
            edge["edge_status"] = EDGE_DEACTIVATED
            edge["active"] = False
            edge["abnormal"] = False
            edge["replay_batch"] = batch_no
            edge["edge_version"] = int(edge.get("edge_version", 1)) + 1
            edge["updated_at"] = batch_no
            changed.append(edge)
        return changed

    def apply_replay_receipt(self, topic: str, batch_no: str, dead_letter_id: int) -> list[dict[str, Any]]:
        """按重放血缘回执建立重放边并切换有效边集。

        冲突判定（同一来源主题 + 同一下游表已存在有效边）：先停用冲突的旧有效边，
        再建立本批次的重放边，保证一个批次只形成一个可查询边集，且不会出现重复边。
        """
        valid_edges: list[dict[str, Any]] = []
        edges = store.rows(EDGE_MODULE)
        # 以该主题的已生效血缘记录为模板，每条记录对应一条边
        templates = [
            row for row in store.rows(MODULE)
            if row.get("上游主题") == topic and row.get("status") == "已生效"
        ]
        for template in templates:
            target = template.get("下游表名")
            # 冲突处理：停用同来源同目标的旧有效边（含此前批次建立的重放边）
            for edge in edges:
                if (
                    edge.get("source_topic") == topic
                    and edge.get("target_table") == target
                    and edge.get("edge_status") == EDGE_VALID
                    and edge.get("active")
                ):
                    edge["edge_status"] = EDGE_DEACTIVATED
                    edge["active"] = False
                    edge["edge_version"] = int(edge.get("edge_version", 1)) + 1
                    edge["updated_at"] = batch_no
            new_edge = self._build_replay_edge(edges, template, batch_no, dead_letter_id)
            edges.append(new_edge)
            valid_edges.append(new_edge)
        # 流式血缘侧保留回执来源引用
        receipt = replay_receipt_service.get_by_batch(batch_no)
        if receipt is not None:
            replay_receipt_service.record_lineage_ref(receipt, valid_edges)
        return valid_edges

    def _build_replay_edge(
        self,
        edges: list[dict[str, Any]],
        template: dict[str, Any],
        batch_no: str,
        dead_letter_id: int,
    ) -> dict[str, Any]:
        edge_id = max((int(row.get("id", 0)) for row in edges), default=0) + 1
        return {
            "id": edge_id,
            "edge_code": f"EDGE-{edge_id:04d}",
            "source_topic": template.get("上游主题"),
            "target_table": template.get("下游表名"),
            "lineage_id": template.get("id"),
            "edge_status": EDGE_VALID,
            "edge_version": 1,
            "source_dead_letter_id": dead_letter_id,
            "replay_batch": batch_no,
            "active": True,
            "abnormal": False,
            "updated_at": batch_no,
        }

    # ── 图与原有 CRUD ─────────────────────────────────────────

    def get_graph(self) -> dict[str, Any]:
        rows = store.rows(MODULE)
        nodes: list[dict] = []
        abnormal_topics = {
            str(edge.get("source_topic")) for edge in store.rows(EDGE_MODULE)
            if edge.get("edge_status") == EDGE_ABNORMAL and edge.get("active")
        }
        for i, row in enumerate(rows):
            node = {
                "id": int(row.get("id", i + 1)),
                "name": str(row.get(list(row.keys())[1] if len(row) > 1 else "name", "")),
                "status": str(row.get("status", STATUS_ORDER[0])),
                **{k: row[k] for k in row if k not in ("id", "status", "pending", "abnormal")},
            }
            # 节点异常标记由有效/异常边推导：仍有异常边的主题标异常，重放清除后自动恢复
            node["abnormal"] = str(row.get("上游主题")) in abnormal_topics
            nodes.append(node)
        edges: list[dict] = []
        node_ids = {node["id"] for node in nodes}
        for edge in store.rows(EDGE_MODULE):
            if not edge.get("active") or edge.get("edge_status") != EDGE_VALID:
                continue
            lineage_id = edge.get("lineage_id")
            if lineage_id not in node_ids:
                continue
            target_node = next(
                (node for node in nodes if node.get("下游表名") == edge.get("target_table")),
                None,
            )
            if target_node is None:
                continue
            edges.append({
                "from": lineage_id,
                "to": target_node["id"],
                "label": f"依赖·{edge.get('replay_batch')}" if edge.get("replay_batch") else "依赖",
                "edge_code": edge.get("edge_code"),
                "edge_status": edge.get("edge_status"),
                "edge_version": edge.get("edge_version"),
                "replay_batch": edge.get("replay_batch"),
            })
        return {"nodes": nodes, "edges": edges}

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(MODULE, entry_id)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [f for f in REQUIRED_FIELDS if not str(values.get(f) or "").strip()]
        if missing:
            return None, missing
        rows = store.rows(MODULE)
        entry = {"id": max((int(r.get("id", 0)) for r in rows), default=0) + 1}
        entry.update({f: values.get(f) for f in REQUIRED_FIELDS})
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        rows.append(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"血缘记录 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于流式血缘可执行范围"
        target = ACTION_RULES[action]
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"血缘记录已{action}"

    def raw_data(self) -> tuple[list[dict], list[dict]]:
        g = self.get_graph()
        return g["nodes"], g["edges"]
