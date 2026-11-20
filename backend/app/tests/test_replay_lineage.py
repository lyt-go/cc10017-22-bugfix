"""死信重放 × 流式血缘联动测试：重放血缘回执、有效边同步与中断恢复。"""
from __future__ import annotations

import unittest

from app.services.dead_letter import DeadLetterService
from app.services.replay_receipt import replay_receipt_service
from app.services.stream_lineage import EDGE_ABNORMAL, EDGE_DEACTIVATED, EDGE_VALID, StreamLineageService
from app.store import store


class ReplayLineageTestBase(unittest.TestCase):
    def setUp(self) -> None:
        # 每个用例从种子定义重建独立数据，避免用例间状态污染
        store.reset_to_seed()
        self.dead_svc = DeadLetterService()
        self.lineage_svc = StreamLineageService()

    def edge_rows(self, topic: str | None = None) -> list[dict]:
        rows = store.rows("stream_lineage_edges")
        if topic is not None:
            rows = [row for row in rows if row.get("source_topic") == topic]
        return rows

    def active_valid_edges(self, topic: str) -> list[dict]:
        return [
            row for row in self.edge_rows(topic)
            if row.get("edge_status") == EDGE_VALID and row.get("active")
        ]


class TestReplayReceiptFlow(ReplayLineageTestBase):
    def test_seed_marks_abnormal_edges_for_unresolved_dead_letter(self) -> None:
        """种子里 TOPI-0001 有待处置死信，对应血缘边应初始化为异常且不在有效图里。"""
        abnormal = [
            row for row in self.edge_rows("TOPI-0001")
            if row.get("edge_status") == EDGE_ABNORMAL
        ]
        self.assertTrue(abnormal)
        graph = self.lineage_svc.get_graph()
        graph_edge_codes = {edge["edge_code"] for edge in graph["edges"]}
        for row in abnormal:
            self.assertNotIn(row["edge_code"], graph_edge_codes)
        # 节点仍带异常标记
        nodes = [n for n in graph["nodes"] if n.get("上游主题") == "TOPI-0001"]
        self.assertTrue(nodes and all(n["abnormal"] for n in nodes))

    def test_replay_updates_status_and_edges_together(self) -> None:
        """标记重放必须同时落库处置状态与有效血缘边，并生成回执。"""
        entry, message = self.dead_svc.run_action(1, "标记重放")
        self.assertIsNotNone(entry, message)
        self.assertEqual(entry["status"], "已重放")
        self.assertEqual(entry["处置状态"], "已重放")
        self.assertFalse(entry["abnormal"])

        receipts = self.dead_svc.list_replay_receipts(1)
        self.assertEqual(len(receipts), 1)
        receipt = receipts[0]
        self.assertEqual(receipt["receipt_status"], "已完成")
        self.assertTrue(receipt["replay_batch"].startswith("RPLB-"))
        # 回执保留重放批次与有效血缘边
        self.assertTrue(receipt["valid_edges"])
        self.assertEqual(
            {edge["edge_status"] for edge in receipt["valid_edges"]}, {EDGE_VALID})

        # 旧异常边被停用，新重放边有效，且无重复有效边
        self.assertEqual(len(self.active_valid_edges("TOPI-0001")), 1)
        statuses = {row["edge_status"] for row in self.edge_rows("TOPI-0001")}
        self.assertIn(EDGE_DEACTIVATED, statuses)
        self.assertNotIn(EDGE_ABNORMAL, statuses)

        # 图页面异常标记被清除，且能查到重放边
        graph = self.lineage_svc.get_graph()
        nodes = [n for n in graph["nodes"] if n.get("上游主题") == "TOPI-0001"]
        self.assertTrue(all(not n["abnormal"] for n in nodes))
        self.assertTrue(any(edge["replay_batch"] for edge in graph["edges"]))

    def test_replay_is_idempotent_without_duplicate_edges(self) -> None:
        """重复重放同一死信不会建立重复边，仍只形成一个可查询边集。"""
        entry, _ = self.dead_svc.run_action(1, "标记重放")
        self.assertIsNotNone(entry)
        again, message = self.dead_svc.run_action(1, "标记重放")
        self.assertIsNotNone(again)
        self.assertIn("已重放", message)
        self.assertEqual(len(self.active_valid_edges("TOPI-0001")), 1)
        self.assertEqual(len(self.dead_svc.list_replay_receipts(1)), 1)

    def test_repeated_replay_batch_supersedes_old_edge(self) -> None:
        """同一订单主题再次重放：先停用冲突旧边再建新边，边集版本递增且仍唯一。"""
        self.dead_svc.run_action(1, "标记重放")
        first = self.active_valid_edges("TOPI-0001")[0]

        # 新增一条同主题死信并重放，模拟同订单的第二个重放批次
        new_entry, missing = self.dead_svc.create_entry({
            "记录编号": "DEAD-0010", "原始主题": "TOPI-0001", "原始分区": "0",
        })
        self.assertEqual(missing, [])
        receipt2, msg = self.dead_svc.replay(new_entry["id"])
        self.assertIsNotNone(receipt2, msg)
        active = self.active_valid_edges("TOPI-0001")
        self.assertEqual(len(active), 1)
        self.assertNotEqual(active[0]["edge_code"], first["edge_code"])
        self.assertFalse(first["active"])
        self.assertEqual(first["edge_status"], EDGE_DEACTIVATED)
        self.assertEqual(active[0]["replay_batch"], receipt2["replay_batch"])

    def test_interrupted_replay_resumes_same_batch(self) -> None:
        """中断后恢复：沿用同一批次回执，完成后无重复边且异常标记清除。"""
        receipt, message = self.dead_svc.start_replay(1)
        self.assertIsNotNone(receipt, message)
        batch_no = receipt["replay_batch"]
        # 中断状态：处置状态仍是待处置，异常边已停用，回执进行中
        entry = self.dead_svc.get_entry(1)
        self.assertEqual(entry["status"], "待处置")
        self.assertEqual(receipt["receipt_status"], "进行中")
        self.assertTrue(receipt["deactivated_edges"])
        self.assertEqual(self.active_valid_edges("TOPI-0001"), [])

        # 恢复重放
        resumed, msg = self.dead_svc.resume_replay(1)
        self.assertIsNotNone(resumed, msg)
        self.assertEqual(resumed["replay_batch"], batch_no)
        self.assertEqual(resumed["receipt_status"], "已完成")
        self.assertEqual(self.dead_svc.get_entry(1)["status"], "已重放")
        self.assertEqual(len(self.active_valid_edges("TOPI-0001")), 1)

        # 只有一份回执（同批次恢复不新建）
        receipts = self.dead_svc.list_replay_receipts(1)
        self.assertEqual(len(receipts), 1)
        self.assertEqual(receipts[0]["replay_batch"], batch_no)

    def test_start_twice_keeps_single_running_batch(self) -> None:
        """中断期间重复发起不会产生第二个进行中批次。"""
        first, _ = self.dead_svc.start_replay(1)
        second, _ = self.dead_svc.start_replay(1)
        self.assertEqual(first["replay_batch"], second["replay_batch"])
        running = [
            row for row in store.rows("replay_lineage_receipts")
            if row["receipt_status"] == "进行中"
        ]
        self.assertEqual(len(running), 1)

    def test_cross_module_receipt_refs(self) -> None:
        """流式血缘侧保留回执来源引用，与死信侧回执一一对应。"""
        receipt, _ = self.dead_svc.run_action(1, "标记重放")
        refs = store.rows("replay_lineage_receipt_refs")
        self.assertEqual(len(refs), 1)
        ref = refs[0]
        self.assertEqual(ref["receipt_id"], receipt["id"])
        self.assertEqual(ref["replay_batch"], receipt["replay_batch"])
        self.assertEqual(ref["source_topic"], "TOPI-0001")
        self.assertEqual(
            set(ref["edge_codes"]),
            {edge["edge_code"] for edge in self.active_valid_edges("TOPI-0001")},
        )

    def test_replay_without_lineage_completes_with_empty_edge_set(self) -> None:
        """没有已生效血缘记录时仍原子完成：回执在、边集为空、状态一致，不留半成品。"""
        # 造一个没有任何已生效血缘记录的主题死信
        entry, _ = self.dead_svc.create_entry({
            "记录编号": "DEAD-0099", "原始主题": "TOPI-UNKNOWN", "原始分区": "0",
        })
        receipt, message = self.dead_svc.replay(entry["id"])
        self.assertIsNotNone(receipt, message)
        self.assertEqual(receipt["receipt_status"], "已完成")
        self.assertEqual(receipt["valid_edges"], [])
        # 处置状态与回执一致落库
        self.assertEqual(self.dead_svc.get_entry(entry["id"])["status"], "已重放")
        self.assertEqual(len(self.dead_svc.list_replay_receipts(entry["id"])), 1)
        self.assertEqual(self.edge_rows("TOPI-UNKNOWN"), [])

    def test_unexpected_failure_rolls_back_status_and_edges(self) -> None:
        """边同步抛异常时整体回滚：处置状态不能单独变成已重放。"""
        entry, _ = self.dead_svc.create_entry({
            "记录编号": "DEAD-0098", "原始主题": "TOPI-0001", "原始分区": "0",
        })
        original = self.dead_svc.lineage_service.apply_replay_receipt

        def boom(*_args, **_kwargs):
            raise RuntimeError("模拟边写入失败")

        self.dead_svc.lineage_service.apply_replay_receipt = boom  # type: ignore[method-assign]
        try:
            result, message = self.dead_svc.replay(entry["id"])
        finally:
            self.dead_svc.lineage_service.apply_replay_receipt = original  # type: ignore[method-assign]
        self.assertIsNone(result)
        self.assertIn("回滚", message)
        self.assertEqual(self.dead_svc.get_entry(entry["id"])["status"], "草稿")
        self.assertEqual(self.dead_svc.list_replay_receipts(entry["id"]), [])
        # 回滚后原有异常边恢复，未被停用
        topic_edges = self.edge_rows("TOPI-0001")
        self.assertTrue(any(edge["edge_status"] == "异常" and edge["active"] for edge in topic_edges))
        self.assertFalse(any(edge["edge_status"] == "已停用" for edge in topic_edges))

    def test_discard_does_not_touch_lineage_edges(self) -> None:
        """标记丢弃不走重放，血缘边保持原样。"""
        before = len(self.edge_rows("TOPI-0001"))
        entry, _ = self.dead_svc.run_action(3, "标记丢弃")
        self.assertEqual(entry["status"], "已丢弃")
        self.assertEqual(len(self.edge_rows("TOPI-0001")), before)
        self.assertEqual(self.dead_svc.list_replay_receipts(3), [])

    def test_receipt_query_endpoint_services_agree(self) -> None:
        """两个模块各自保留的回执记录可互查且批次一致。"""
        receipt, _ = self.dead_svc.run_action(1, "标记重放")
        dead_side = replay_receipt_service.list_receipts(dead_letter_id=1)
        lineage_side = store.rows("replay_lineage_receipt_refs")
        self.assertEqual(dead_side[0]["replay_batch"], lineage_side[0]["replay_batch"])
        self.assertEqual(
            {edge["edge_code"] for edge in dead_side[0]["valid_edges"]},
            set(lineage_side[0]["edge_codes"]),
        )


if __name__ == "__main__":
    unittest.main()
