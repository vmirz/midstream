"""One runtime reading several topics: each topic's records become rows of its own table, exactly once.

The member's pipeline and backlog are shared by every table, so a runtime that reads more topics
never seals or uploads more at once (`runtime_support.RuntimeHarness` with `topics`). Maintenance is
the maintenance app's (`test_maintenance_app.py`).
"""

from __future__ import annotations

import asyncio
from pathlib import Path
import sys
import time
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))

from lakehouse_support import MATERIALIZATION_ID  # noqa: E402
from runtime_support import MEMBER_ID, TP, RuntimeHarness, run  # noqa: E402

from midstream.lakehouse.maintenance import live_data_files  # noqa: E402
from midstream.lakehouse.offsets import committed_offsets  # noqa: E402
from midstream.lakehouse.store import StoreError  # noqa: E402
from midstream.runtime_app.handlers.status import build_runtime_heartbeat  # noqa: E402
from midstream.runtime_app.pipeline import BatchPipeline, batch_pipeline  # noqa: E402
from midstream.runtime_app.state import get_partition_state, list_partition_states  # noqa: E402
from midstream.runtime_app.table_state import committed_offsets_key, current_view, schema_key  # noqa: E402

TOPICS = ("orders", "payments")
SEEDS = {"orders": ({"order_id": "O-0", "amount": 1},), "payments": ({"payment_id": "P-0", "total": 1.5},)}


def order(offset: int, **extra) -> dict:
    return {"order_id": f"O-{offset}", "amount": offset, **extra}


def payment(offset: int) -> dict:
    return {"payment_id": f"P-{offset}", "total": offset + 0.5}


def committed(events: list[dict]) -> dict[str, dict]:
    """Each table's `table_committed` payload among `events`."""
    return {event["table"]: event["payload"] for event in events if event["kind"] == "table_committed"}


class MultiTopicFlowTests(unittest.TestCase):
    def setUp(self) -> None:
        self.runtime = RuntimeHarness(topics=TOPICS, seeds_by_topic=SEEDS)
        self.addCleanup(self.runtime.close)

    async def write_both(self, offsets=range(3), *, partition: int = 0):
        orders = [await self.runtime.write(order(offset), offset=offset, partition=partition) for offset in offsets]
        payments = [
            await self.runtime.write(payment(offset), offset=offset, partition=partition, topic="payments")
            for offset in offsets
        ]
        return orders, payments

    def test_each_topic_becomes_rows_of_its_own_table_exactly_once(self) -> None:
        async def scenario():
            messages = await self.write_both()
            return messages, await self.runtime.cycle()

        (orders, payments), events = run(scenario())

        self.assertEqual(["O-0", "O-1", "O-2"], [row["order_id"] for row in self.runtime.rows("orders")])
        self.assertEqual(["P-0", "P-1", "P-2"], [row["payment_id"] for row in self.runtime.rows("payments")])
        self.assertNotIn("payment_id", self.runtime.rows("orders")[0])
        self.assertTrue(all(message.acked for message in orders + payments))
        acked = sorted(self.runtime.app.consumer.acked_by_topic)
        self.assertEqual([("orders", 0, 0), ("orders", 0, 1), ("orders", 0, 2), ("payments", 0, 0), ("payments", 0, 1), ("payments", 0, 2)], acked)
        by_table = committed(events)
        self.assertEqual({"orders", "payments"}, set(by_table))
        self.assertEqual(({"0": 2}, 3), (by_table["payments"]["offsets"], by_table["payments"]["records_added"]))
        for name in TOPICS:
            self.assertEqual(2, self.runtime.state_table[committed_offsets_key(name)]["offsets"]["0"])
            self.assertEqual({0: 2}, committed_offsets(self.runtime.table(name).metadata).offsets)

    def test_a_partition_of_each_topic_is_a_partition_of_its_own(self) -> None:
        async def scenario():
            await self.runtime.write(order(0), offset=0)
            await self.runtime.write(payment(5), offset=5, topic="payments")

        run(scenario())

        orders, payments = get_partition_state("orders", 0), get_partition_state("payments", 0)
        self.assertEqual((0, 5), (orders.handled_offset, payments.handled_offset))
        self.assertNotEqual(orders.active_batch.batch_id, payments.active_batch.batch_id)
        self.assertIn("-payments-", payments.active_batch.batch_id)
        self.assertEqual({"orders", "payments"}, {state.table for state in list_partition_states(owner_member_id=MEMBER_ID)})

    def test_a_replay_is_recognised_by_the_table_its_topic_writes(self) -> None:
        async def scenario():
            await self.write_both()
            await self.runtime.cycle()
            # A new process: everything in memory is gone; each table remembers its own offsets.
            self.runtime.app.rebalancing_count += 1
            replays = [
                await self.runtime.write(order(1), offset=1),
                await self.runtime.write(payment(1), offset=1, topic="payments"),
            ]
            for offset in (3, 4, 5):
                await self.runtime.write(payment(offset), offset=offset, topic="payments")
            await self.runtime.cycle()
            return replays

        replays = run(scenario())

        self.assertTrue(all(message.acked for message in replays))
        self.assertEqual(3, len(self.runtime.rows("orders")))
        self.assertEqual([0, 1, 2, 3, 4, 5], [row["_kafka_offset"] for row in self.runtime.rows("payments")])

    def test_a_record_of_a_topic_the_runtime_does_not_read_is_acknowledged_and_dropped(self) -> None:
        with self.assertLogs("midstream.runtime_app.handlers.writer", level="WARNING") as logs:
            message = run(self.runtime.write({"customer_id": "C-1"}, offset=0, topic="customers"))

        self.assertTrue(message.acked)
        self.assertIn("customers", logs.output[0])
        self.assertEqual([], list_partition_states(owner_member_id=MEMBER_ID))

    def test_a_new_field_evolves_only_its_own_tables_schema(self) -> None:
        async def scenario():
            await self.runtime.write(order(0), offset=0)
            await self.runtime.write(order(1, coupon="SAVE10"), offset=1)
            await self.runtime.write(order(2), offset=2)
            for offset in range(3):
                await self.runtime.write(payment(offset), offset=offset, topic="payments")
            first = await self.runtime.pipeline()
            await self.runtime.deliver(first, tick=False)
            self.runtime.close_schema_window()
            schema_events = await self.runtime.deliver([], tick=True)
            await self.runtime.cycle()
            return first, schema_events

        first, schema_events = run(scenario())

        requests = [message for message in first if message["kind"] == "schema_change_request"]
        self.assertEqual(["orders"], [message["table"] for message in requests])
        changed = [event for event in schema_events if event["kind"] == "schema_changed"]
        self.assertEqual([("orders", 1)], [(event["table"], event["payload"]["schema_id"]) for event in changed])
        self.assertEqual(1, self.runtime.state_table[schema_key("orders")]["schema_id"])
        self.assertEqual(0, current_view(self.runtime.state_table, "payments").schema_id)
        self.assertEqual([None, "SAVE10", None], [row["coupon"] for row in self.runtime.rows("orders")])
        self.assertNotIn("coupon", self.runtime.table("payments").schema().column_names)
        self.assertEqual(3, len(self.runtime.rows("payments")))

    def test_a_table_whose_commits_fail_does_not_hold_back_the_others(self) -> None:
        def outage(operation, uri):
            if operation == "put" and "/payments/metadata/" in uri and uri.endswith(".metadata.json"):
                return StoreError("the store is down", uri=uri, status=503)
            return None

        async def scenario():
            orders, payments = await self.write_both()
            announced = await self.runtime.pipeline()
            await self.runtime.deliver([], tick=True)
            self.runtime.store.fail = outage
            self.runtime.allow_commit()
            failed = await self.runtime.deliver(announced)
            await batch_pipeline.drain(self.runtime.app, self.runtime.state_table)
            held = [message.acked for message in payments]
            self.runtime.store.fail = None
            self.runtime.allow_commit()
            recovered = await self.runtime.deliver([], tick=True)
            await batch_pipeline.drain(self.runtime.app, self.runtime.state_table)
            return orders, payments, failed, held, recovered

        orders, payments, failed, held, recovered = run(scenario())

        self.assertEqual({"orders"}, set(committed(failed)))
        errors = [event for event in failed if event["kind"] == "table_error"]
        self.assertEqual([("payments", "commit_failed")], [(event["table"], event["payload"]["reason"]) for event in errors])
        self.assertTrue(all(message.acked for message in orders), "orders waited for payments")
        self.assertFalse(any(held), "payments were acknowledged before a commit held them")
        self.assertEqual({"payments"}, set(committed(recovered)))
        self.assertTrue(all(message.acked for message in payments))
        self.assertEqual(3, len(self.runtime.rows("payments")))

    def test_the_heartbeat_reports_every_table(self) -> None:
        async def scenario():
            await self.write_both()
            await self.runtime.cycle()
            return build_runtime_heartbeat(
                self.runtime.state_table,
                materialization_id=MATERIALIZATION_ID,
                runtime_app_ref="kaspr-mat-a1b2c3d4e5f6",
                agent_name="writer",
                tables=list(TOPICS),
            )

        heartbeat = run(scenario())

        self.assertEqual(("healthy", "processing"), (heartbeat["health"], heartbeat["phase_reason"]))
        self.assertEqual({"orders": 0, "payments": 0}, heartbeat["schema_versions"])
        self.assertEqual({"orders": {"0": 2}, "payments": {"0": 2}}, heartbeat["latest_offsets"])
        self.assertEqual({"orders": 3, "payments": 3}, {name: counters["rows_written"] for name, counters in heartbeat["table_counters"].items()})
        self.assertEqual(3, heartbeat["tables"]["payments"]["total_records"])
        self.assertEqual("payments", heartbeat["tables"]["payments"]["source_topic"])
        self.assertEqual(MEMBER_ID, heartbeat["tables"]["orders"]["member_id"])

    def test_a_member_that_has_not_loaded_every_table_says_which_it_is_loading(self) -> None:
        run(self.runtime.write(order(0), offset=0))

        heartbeat = build_runtime_heartbeat(
            self.runtime.state_table,
            materialization_id=MATERIALIZATION_ID,
            runtime_app_ref="kaspr-mat-a1b2c3d4e5f6",
            agent_name="writer",
            tables=list(TOPICS),
        )

        self.assertEqual(("unknown", "waiting_for_table"), (heartbeat["health"], heartbeat["phase_reason"]))
        self.assertIn("payments", heartbeat["phase_message"])
        self.assertNotIn("orders", heartbeat["phase_message"])


class SharedPipelineTests(unittest.TestCase):
    """The member's seals, uploads and backlog are bounded for all its tables together."""

    def setUp(self) -> None:
        self.runtime = RuntimeHarness(topics=TOPICS, seeds_by_topic=SEEDS, partitions=(0, 1))
        self.addCleanup(self.runtime.close)

    async def roll_everywhere(self) -> None:
        """Roll a batch of three rows on both partitions of both topics: four batches."""
        for partition in (0, 1):
            for offset in range(3):
                await self.runtime.write(order(offset), offset=offset, partition=partition)
                await self.runtime.write(payment(offset), offset=offset, partition=partition, topic="payments")

    def test_seals_and_uploads_never_exceed_the_members_workers_whatever_the_tables(self) -> None:
        self.runtime.configure(seal_worker_count=1, upload_worker_count=1)
        running = {"seals": 0, "uploads": 0}
        peak = {"seals": 0, "uploads": 0}

        def counted(kind: str, function):
            async def wrapper(*args, **kwargs):
                running[kind] += 1
                peak[kind] = max(peak[kind], running[kind])
                try:
                    await asyncio.sleep(0.01)
                    return await function(*args, **kwargs)
                finally:
                    running[kind] -= 1

            return wrapper

        batch_pipeline.sealer = counted("seals", batch_pipeline.sealer)
        upload = BatchPipeline._upload

        async def scenario():
            with patch.object(BatchPipeline, "_upload", counted("uploads", upload)):
                await self.roll_everywhere()
                # A table commits once a seal arrives and a commit is due; the rest go in its next.
                await self.runtime.cycle()
                await self.runtime.cycle()

        run(scenario())

        self.assertEqual({"seals": 1, "uploads": 1}, peak)
        for name in TOPICS:
            self.assertEqual(6, len(self.runtime.rows(name)))

    def test_more_workers_seal_batches_of_several_tables_at_once(self) -> None:
        self.runtime.configure(seal_worker_count=4)
        started: list[str] = []
        real = batch_pipeline.sealer

        async def gated(job_path):
            started.append(str(job_path))
            await asyncio.sleep(0.05)
            return await real(job_path)

        batch_pipeline.sealer = gated

        async def scenario():
            with patch.object(BatchPipeline, "schedule"):
                await self.roll_everywhere()
            batch_pipeline.schedule(self.runtime.app, self.runtime.state_table)
            await asyncio.sleep(0)
            inflight = batch_pipeline.inflight()["seals"]
            await self.runtime.cycle()
            return inflight

        self.assertEqual(4, run(scenario()))
        self.assertEqual(4, len(started))

    def test_a_full_backlog_pauses_every_topic_and_a_drained_one_resumes_them(self) -> None:
        self.runtime.configure(max_pending_upload_batches=1)

        async def fill():
            # Only orders is written to: payments is paused all the same.
            for offset in range(9):
                await self.runtime.write(order(offset), offset=offset)

        with patch("midstream.runtime_app.pipeline.BatchPipeline.schedule"):
            run(fill())

        paused = set().union(*self.runtime.app.consumer.paused)
        self.assertEqual({TP(topic, partition) for topic in TOPICS for partition in (0, 1)}, paused)

        async def drain():
            for _ in range(4):
                await self.runtime.cycle()

        run(drain())
        resumed = set().union(*self.runtime.app.consumer.resumed)
        self.assertEqual(paused, resumed)

    def test_the_backlog_counts_every_tables_batches_together(self) -> None:
        self.runtime.configure(max_pending_upload_batches=2)

        async def scenario():
            with patch("midstream.runtime_app.pipeline.BatchPipeline.schedule"):
                for offset in range(3):
                    await self.runtime.write(order(offset), offset=offset)
                for offset in range(3):
                    await self.runtime.write(payment(offset), offset=offset, topic="payments")
                # Two batches pending: one per table, at the member's limit.
                self.assertEqual([], self.runtime.app.consumer.paused)
                for offset in range(3, 6):
                    await self.runtime.write(order(offset), offset=offset)

        run(scenario())

        self.assertTrue(self.runtime.app.consumer.paused)
        orders = get_partition_state("orders", 0)
        # The batch that found the backlog full keeps filling rather than rolling a third.
        self.assertEqual("open", orders.active_batch.state)


if __name__ == "__main__":
    unittest.main()
