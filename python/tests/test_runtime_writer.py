"""The writer agent: every record staged as it arrived, never waiting, acknowledged only when safe (spec §7.2)."""

from __future__ import annotations

import json
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))

from lakehouse_support import MATERIALIZATION_ID  # noqa: E402
from runtime_support import TOPIC, TP, RuntimeHarness, run  # noqa: E402

from midstream.runtime_app.handlers.writer import RuntimeShutdownInterrupted  # noqa: E402
from midstream.runtime_app.outbox import outbox  # noqa: E402
from midstream.runtime_app.state import get_partition_state  # noqa: E402


class WriterTests(unittest.TestCase):
    def setUp(self) -> None:
        self.runtime = RuntimeHarness({"order_id": "O-0", "amount": 1})
        self.addCleanup(self.runtime.close)

    def staged_lines(self) -> list[dict]:
        state = get_partition_state("orders", 0)
        batch = state.active_batch
        return [json.loads(line) for line in Path(batch.local_path).read_text(encoding="utf-8").splitlines()]

    def test_a_record_is_staged_as_it_arrived_with_its_kafka_metadata(self) -> None:
        run(self.runtime.write({"order_id": "O-1", "amount": 5}, offset=0, key="customer-7"))
        (line,) = self.staged_lines()
        self.assertEqual({"order_id": "O-1", "amount": 5}, line["v"])
        self.assertEqual(0, line["m"]["o"])
        self.assertEqual("customer-7", line["m"]["k"])
        self.assertEqual([["source", "test"]], line["m"]["h"])
        self.assertEqual("2026-10-18T10:00:00.000Z", line["m"]["t"])

    def test_a_skipped_tombstone_is_acknowledged_without_a_row(self) -> None:
        async def scenario():
            message = self.runtime.message(partition=0, offset=0)
            await self.runtime.write_message(None, message, skip_tombstones=True)
            return message

        message = run(scenario())
        self.assertTrue(message.acked)
        state = get_partition_state("orders", 0)
        self.assertIsNone(state.active_batch)
        self.assertEqual(0, state.records_skipped)

    def test_a_tombstone_kept_is_a_row_without_a_payload(self) -> None:
        async def scenario():
            await self.runtime.write(None, offset=0)
            await self.runtime.write({"order_id": "O-1"}, offset=1)
            await self.runtime.write({"order_id": "O-2"}, offset=2)
            await self.runtime.cycle()

        run(scenario())
        rows = self.runtime.rows()
        self.assertIsNone(rows[0]["order_id"])
        self.assertIsNone(rows[0]["_unmapped"])
        self.assertEqual(["O-1", "O-2"], [row["order_id"] for row in rows[1:]])

    def test_a_value_that_is_not_an_object_is_kept_in_unmapped(self) -> None:
        async def scenario():
            await self.runtime.write("plain text", offset=0)
            await self.runtime.write([1, 2], offset=1)
            await self.runtime.write({"order_id": "O-2"}, offset=2)
            await self.runtime.cycle()

        run(scenario())
        rows = self.runtime.rows()
        self.assertEqual({"$": "plain text"}, json.loads(rows[0]["_unmapped"]))
        self.assertEqual({"$": [1, 2]}, json.loads(rows[1]["_unmapped"]))

    def test_new_fields_are_requested_once_with_their_shapes_only(self) -> None:
        async def scenario():
            await self.runtime.write({"order_id": "O-0", "coupon": "SAVE10"}, offset=0)
            await self.runtime.write({"order_id": "O-1", "coupon": "SAVE20"}, offset=1)
            return list(outbox.drain())

        (request,) = run(scenario())
        self.assertEqual("schema_change_request", request["kind"])
        self.assertEqual([{"path": ["coupon"], "observed": "string"}], request["observations"])
        self.assertNotIn("SAVE10", json.dumps(request))
        self.assertEqual(0, request["base_schema_id"])

    def test_a_record_the_schema_already_takes_requests_nothing(self) -> None:
        async def scenario():
            await self.runtime.write({"order_id": "O-0", "amount": 7}, offset=0)
            return list(outbox.drain())

        self.assertEqual([], run(scenario()))

    def test_shutdown_leaves_the_record_unacknowledged(self) -> None:
        self.runtime.app.should_stop = True

        async def scenario():
            message = self.runtime.message(partition=0, offset=0)
            with self.assertRaises(RuntimeShutdownInterrupted):
                await self.runtime.write_message({"order_id": "O-0"}, message)
            return message

        self.assertFalse(run(scenario()).acked)

    def test_an_unexpected_failure_restarts_the_member_and_leaves_the_record_unacknowledged(self) -> None:
        crashes = []

        async def crash(error):
            crashes.append(error)

        self.runtime.app.crash = crash

        async def scenario():
            message = self.runtime.message(partition=0, offset=0)
            with patch(
                "midstream.runtime_app.handlers.writer.append_row_to_batch",
                side_effect=OSError("disk full"),
            ):
                with self.assertRaises(OSError):
                    await self.runtime.write_message({"order_id": "O-0"}, message)
            import asyncio

            await asyncio.sleep(0)
            return message

        message = run(scenario())
        self.assertFalse(message.acked)
        self.assertEqual(1, len(crashes))

    def test_a_full_backlog_pauses_the_source(self) -> None:
        self.runtime.configure(max_pending_upload_batches=1)

        async def scenario():
            for offset in range(9):
                await self.runtime.write({"order_id": f"O-{offset}"}, offset=offset)

        with patch("midstream.runtime_app.pipeline.BatchPipeline.schedule"):
            run(scenario())
        self.assertTrue(self.runtime.app.consumer.paused)
        self.assertIn(TP(TOPIC, 0), self.runtime.app.consumer.paused[0])

    def test_a_record_fetched_again_in_its_session_leaves_the_acknowledgement_to_the_first(self) -> None:
        async def scenario():
            first = await self.runtime.write({"order_id": "O-0"}, offset=0)
            again = await self.runtime.write({"order_id": "O-0"}, offset=0)
            return first, again, self.staged_lines()

        first, again, lines = run(scenario())
        self.assertEqual(1, len(lines))
        self.assertFalse(again.acked)

    def test_with_a_committed_offset_only_the_batch_guard_is_held(self) -> None:
        consumer = self.runtime.app.consumer
        consumer._committed_offset = {TP(TOPIC, 0): 1}
        consumer._gap = {}

        async def scenario():
            # The table holds nothing yet, but Kafka has a committed offset: the guard holds the batch.
            first = await self.runtime.write({"order_id": "O-1"}, offset=1)
            second = await self.runtime.write({"order_id": "O-2"}, offset=2)
            return first, second

        first, second = run(scenario())
        self.assertFalse(first.acked)
        self.assertTrue(second.acked)


if __name__ == "__main__":
    unittest.main()
