"""The runtime end to end: records staged by the writer become rows of the table exactly once.

Each test runs the writer, the batch pipeline and the committer of one member against a table in an
in-memory object store (`runtime_support.RuntimeHarness`), and checks the table's rows and the
source records' acknowledgements - the two things exactly-once is about (spec §7.8).
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
import json
from pathlib import Path
import sys
import time
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from runtime_support import BASE_TIMESTAMP, MEMBER_ID, RuntimeHarness, run  # noqa: E402

from midstream.lakehouse.offsets import committed_offsets  # noqa: E402
from midstream.runtime_app.committer import reset_committers  # noqa: E402
from midstream.runtime_app.handlers.status import build_runtime_heartbeat  # noqa: E402
from midstream.runtime_app.state import get_partition_state  # noqa: E402
from midstream.runtime_app.table_state import (  # noqa: E402
    committed_offsets_key,
    file_decisions_key,
    schema_key,
)
from midstream.runtime_app.writer import current_session_epoch  # noqa: E402
from lakehouse_support import MATERIALIZATION_ID  # noqa: E402


class RuntimeFlowTests(unittest.TestCase):
    def setUp(self) -> None:
        self.runtime = RuntimeHarness({"order_id": "O-0", "amount": 1})
        self.addCleanup(self.runtime.close)

    def test_records_become_rows_and_are_acknowledged_once_committed(self) -> None:
        async def scenario():
            messages = [
                await self.runtime.write({"order_id": f"O-{offset}", "amount": offset}, offset=offset)
                for offset in range(3)
            ]
            # Rolled at three rows; nothing is acknowledged before a commit holds the rows.
            self.assertEqual([], [m.offset for m in messages if m.acked and m.offset == 0])
            events = await self.runtime.cycle()
            return messages, events

        messages, events = run(scenario())
        rows = self.runtime.rows()
        self.assertEqual([0, 1, 2], [row["_kafka_offset"] for row in rows])
        self.assertEqual(["O-0", "O-1", "O-2"], [row["order_id"] for row in rows])
        self.assertEqual([0, 1, 2], [row["amount"] for row in rows])
        self.assertTrue(all(message.acked for message in messages))
        committed = [event for event in events if event["kind"] == "table_committed"]
        self.assertEqual(1, len(committed))
        self.assertEqual({"0": 2}, committed[0]["payload"]["offsets"])
        self.assertEqual(3, committed[0]["payload"]["records_added"])
        self.assertEqual(2, self.runtime.state_table[committed_offsets_key("orders")]["offsets"]["0"])
        self.assertEqual({0: 2}, committed_offsets(self.runtime.table().metadata).offsets)

    def test_the_first_record_of_a_batch_is_held_until_the_commit(self) -> None:
        async def scenario():
            first = await self.runtime.write({"order_id": "O-0"}, offset=0)
            second = await self.runtime.write({"order_id": "O-1"}, offset=1)
            held = (first.acked, second.acked)
            await self.runtime.write({"order_id": "O-2"}, offset=2)
            await self.runtime.cycle()
            return held, first

        (first_acked, second_acked), first = run(scenario())
        # The guard holds the partition's committed offset below the batch until it is durable.
        self.assertFalse(first_acked)
        self.assertTrue(first.acked)

    def test_a_replayed_record_the_table_holds_is_acknowledged_without_a_row(self) -> None:
        async def scenario():
            for offset in range(3):
                await self.runtime.write({"order_id": f"O-{offset}"}, offset=offset)
            await self.runtime.cycle()
            # A new process: everything in memory is gone; the table remembers.
            self.runtime.app.rebalancing_count += 1
            replay = await self.runtime.write({"order_id": "O-1"}, offset=1)
            return replay

        replay = run(scenario())
        self.assertTrue(replay.acked)
        self.assertEqual(3, len(self.runtime.rows()))
        self.assertIsNone(get_partition_state("orders", 0).active_batch)

    def test_a_new_field_evolves_the_schema_before_its_batch_seals(self) -> None:
        async def scenario():
            await self.runtime.write({"order_id": "O-0", "amount": 1}, offset=0)
            await self.runtime.write({"order_id": "O-1", "amount": 2, "coupon": "SAVE10"}, offset=1)
            await self.runtime.write({"order_id": "O-2", "amount": 3}, offset=2)
            # The batch waits for the schema; the request is in the outbox.
            first = await self.runtime.pipeline()
            kinds = [message["kind"] for message in first]
            await self.runtime.deliver(first, tick=False)
            self.runtime.close_schema_window()
            schema_events = await self.runtime.deliver([], tick=True)
            events = await self.runtime.cycle()
            return kinds, schema_events, events

        kinds, schema_events, events = run(scenario())
        self.assertEqual(["schema_change_request"], kinds)
        changed = [event for event in schema_events if event["kind"] == "schema_changed"]
        self.assertEqual(1, len(changed))
        self.assertEqual(1, changed[0]["payload"]["schema_id"])
        self.assertEqual("added", changed[0]["payload"]["changes"][0]["kind"])
        self.assertEqual(1, self.runtime.state_table[schema_key("orders")]["schema_id"])
        rows = self.runtime.rows()
        self.assertEqual([None, "SAVE10", None], [row["coupon"] for row in rows])
        self.assertTrue(all(row["_unmapped"] is None for row in rows))

    def test_a_batch_that_cannot_wait_for_the_schema_keeps_new_values_in_unmapped(self) -> None:
        self.runtime.configure(seal_schema_wait_seconds=0)

        async def scenario():
            await self.runtime.write({"order_id": "O-0", "coupon": "SAVE10"}, offset=0)
            await self.runtime.write({"order_id": "O-1"}, offset=1)
            await self.runtime.write({"order_id": "O-2"}, offset=2)
            messages = await self.runtime.pipeline()
            # Requests go to the committer; the seal did not wait for them.
            self.runtime.allow_commit()
            return await self.runtime.deliver(messages, tick=True)

        run(scenario())
        rows = self.runtime.rows()
        self.assertEqual(3, len(rows))
        self.assertIn("SAVE10", rows[0]["_unmapped"])
        self.assertEqual(1, get_partition_state("orders", 0).unmapped_values)

    def test_rows_are_split_by_the_day_of_their_kafka_timestamp(self) -> None:
        async def scenario():
            day_one = BASE_TIMESTAMP
            day_two = BASE_TIMESTAMP + timedelta(days=1)
            await self.runtime.write({"order_id": "O-0"}, offset=0, at=day_one)
            await self.runtime.write({"order_id": "O-1"}, offset=1, at=day_two)
            await self.runtime.write({"order_id": "O-2"}, offset=2, at=day_one)
            return await self.runtime.cycle()

        events = run(scenario())
        committed = [event for event in events if event["kind"] == "table_committed"]
        self.assertEqual(2, committed[0]["payload"]["files_added"])
        files = self.runtime.table().inspect.files().to_pylist()
        self.assertEqual({"ts_day=2026-10-18", "ts_day=2026-10-19"}, {Path(f["file_path"]).parent.name for f in files})
        self.assertEqual(3, len(self.runtime.rows()))

    def test_a_lost_committer_learns_of_uploaded_files_from_their_announcement_again(self) -> None:
        async def scenario():
            for offset in range(3):
                await self.runtime.write({"order_id": f"O-{offset}"}, offset=offset)
            announcements = await self.runtime.pipeline()
            # The committer that received the announcement goes away before its next commit.
            await self.runtime.deliver([], tick=True)
            self.runtime.committer.last_commit_at = time.monotonic()
            await self.runtime.deliver(announcements, tick=False)
            reset_committers()
            nothing = await self.runtime.pipeline()
            self.runtime.configure(announce_retry_seconds=1)
            await asyncio.sleep(1.1)
            again = await self.runtime.pipeline()
            events = await self.runtime.deliver(again)
            return nothing, again, events

        nothing, again, events = run(scenario())
        self.assertEqual([], nothing)
        self.assertEqual(["data_file_ready"], [message["kind"] for message in again])
        self.assertEqual(2, again[0]["attempt"])
        self.assertEqual(["table_committed"], [event["kind"] for event in events])
        self.assertEqual(3, len(self.runtime.rows()))

    def test_announcements_name_the_process_and_announcing_again_keeps_it(self) -> None:
        async def scenario():
            for offset in range(3):
                await self.runtime.write({"order_id": f"O-{offset}"}, offset=offset)
            first = await self.runtime.pipeline()
            self.runtime.configure(announce_retry_seconds=1)
            await asyncio.sleep(1.1)
            again = await self.runtime.pipeline()
            return first, again

        first, again = run(scenario())
        self.assertEqual([1, 2], [message["attempt"] for message in first + again])
        self.assertEqual({current_session_epoch()}, {message["session_epoch"] for message in first + again})
        self.assertEqual({1}, {message["session_generation"] for message in first + again})

    def test_a_restarted_member_goes_on_committing_to_a_committer_that_ran_on(self) -> None:
        """A member restarts - a rolling update, an OOM kill - and its rebalance count starts again at 1,
        while the committer on another member runs on. That committer took the restarted member's every
        announcement for an older session of the process it knew, and never committed its partitions
        again (`commits_stalled` on develop, 2026-10-22)."""

        async def scenario():
            self.runtime.app.rebalancing_count = 3
            for offset in range(3):
                await self.runtime.write({"order_id": f"O-{offset}"}, offset=offset)
            await self.runtime.cycle()
            self.runtime.restart_member()
            messages = [await self.runtime.write({"order_id": f"O-{offset}"}, offset=offset) for offset in range(3, 6)]
            events = await self.runtime.cycle()
            return messages, events

        messages, events = run(scenario())
        self.assertEqual(["table_committed"], [event["kind"] for event in events])
        self.assertEqual(list(range(6)), [row["_kafka_offset"] for row in self.runtime.rows()])
        self.assertTrue(all(message.acked for message in messages))
        self.assertEqual(0, self.runtime.committer.stale_announcements)

    def test_a_seal_overlapping_the_table_is_sealed_again_without_its_committed_rows(self) -> None:
        async def scenario():
            for offset in range(3):
                await self.runtime.write({"order_id": f"O-{offset}"}, offset=offset)
            announcements = await self.runtime.pipeline()
            # Meanwhile another member committed offsets 0-1 of the partition (a stale owner).
            other = self.runtime.lake.write_seal(partition=0, after_offset=-1, offsets=[0, 1], member="member-9")
            await self.runtime.deliver([item.to_dict() for item in other])
            self.runtime.allow_commit()
            await self.runtime.deliver(announcements)
            decisions = dict(self.runtime.state_table.get(file_decisions_key("orders")) or {})
            events = await self.runtime.cycle()
            return decisions, events

        decisions, events = run(scenario())
        self.assertEqual({"overlap"}, {decision["decision"] for decision in decisions.values()})
        rows = self.runtime.rows()
        self.assertEqual([0, 1, 2], [row["_kafka_offset"] for row in rows])
        self.assertEqual(["table_committed"], [event["kind"] for event in events])
        self.assertEqual({0: 2}, committed_offsets(self.runtime.table().metadata).offsets)

    def test_an_uploaded_file_that_disappears_is_sealed_again_by_its_member(self) -> None:
        async def scenario():
            messages = [await self.runtime.write({"order_id": f"O-{offset}"}, offset=offset) for offset in range(3)]
            announcements = await self.runtime.pipeline()
            for announcement in announcements:
                self.runtime.store.objects.pop(announcement["file_path"])
            refused = await self.runtime.deliver(announcements)
            events = await self.runtime.cycle()
            return messages, refused, events

        messages, refused, events = run(scenario())
        self.assertEqual([], [event for event in refused if event["kind"] == "table_committed"])
        self.assertEqual(["table_committed"], [event["kind"] for event in events])
        self.assertEqual([0, 1, 2], [row["_kafka_offset"] for row in self.runtime.rows()])
        self.assertTrue(all(message.acked for message in messages))

    def test_a_rebalance_abandons_staged_rows_and_reads_them_again(self) -> None:
        async def scenario():
            first = await self.runtime.write({"order_id": "O-0"}, offset=0)
            self.runtime.app.rebalancing_count += 1
            # Faust seeks back to the committed offset and delivers offset 0 again.
            again = await self.runtime.write({"order_id": "O-0"}, offset=0)
            await self.runtime.write({"order_id": "O-1"}, offset=1)
            await self.runtime.write({"order_id": "O-2"}, offset=2)
            await self.runtime.cycle()
            return first, again

        first, again = run(scenario())
        self.assertFalse(first.acked)
        self.assertTrue(again.acked)
        self.assertEqual([0, 1, 2], [row["_kafka_offset"] for row in self.runtime.rows()])

    def test_a_commit_is_logged_when_it_starts_and_when_it_completes(self) -> None:
        self.runtime.configure(protocol_logging_enabled=True)

        async def scenario():
            for offset in range(3):
                await self.runtime.write({"order_id": f"O-{offset}"}, offset=offset)
            await self.runtime.cycle()

        with self.assertLogs("midstream.runtime_app.committer", level="INFO") as logs:
            run(scenario())
        events = [line.split("protocol_event ", 1)[1] for line in logs.output if "protocol_event " in line]
        names = [json.loads(event)["event"] for event in events]
        self.assertIn("runtime_commit_started", names)
        self.assertLess(names.index("runtime_commit_started"), names.index("runtime_commit_completed"))
        started = json.loads(events[names.index("runtime_commit_started")])
        self.assertEqual((1, 1), (started["seals"], started["files"]))

    def test_the_committer_member_reports_the_table_on_its_heartbeat(self) -> None:
        async def scenario():
            for offset in range(3):
                await self.runtime.write({"order_id": f"O-{offset}"}, offset=offset)
            await self.runtime.cycle()
            return build_runtime_heartbeat(
                self.runtime.state_table,
                materialization_id=MATERIALIZATION_ID,
                runtime_app_ref="kaspr-mat-a1b2c3d4e5f6",
                agent_name="writer",
                tables=["orders"],
            )

        heartbeat = run(scenario())
        self.assertEqual(MEMBER_ID, heartbeat["member_id"])
        self.assertEqual("processing", heartbeat["phase_reason"])
        block = heartbeat["tables"]["orders"]
        self.assertEqual(3, block["total_records"])
        self.assertEqual({"0": 2}, block["committed_offsets"])
        self.assertEqual(0, block["pending_files"])
        self.assertEqual(0, block["stale_announcements"])
        self.assertEqual("orders", block["source_topic"])
        self.assertEqual(MEMBER_ID, block["member_id"], "the table block names the committer member")
        self.assertEqual(0, heartbeat["files_pending_commit"])


if __name__ == "__main__":
    unittest.main()
