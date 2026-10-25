"""The tail app when things fail: Kafka, the control plane, odd messages, crowded serving (handbook 19 §10)."""

from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))

from tail_support import (  # noqa: E402
    DATASET_ID,
    TP,
    TailHarness,
    reader_on,
    target,
    tick,
)

from midstream.tail_app import worker as worker_module  # noqa: E402
from midstream.tail_app.handlers import handle_tail_message  # noqa: E402
from midstream.tail_app.views import tail_head_view, tail_rows_view  # noqa: E402
from midstream.tail_app.worker import reset_worker  # noqa: E402
from midstream.web.errors import WebError  # noqa: E402

import io  # noqa: E402

import pyarrow.parquet as pq  # noqa: E402


def offsets_of(answer: dict) -> list[tuple[int, int]]:
    table = pq.read_table(io.BytesIO(answer["body"]))
    return sorted(zip(table.column("_kafka_partition").to_pylist(), table.column("_kafka_offset").to_pylist()))


class FailureCase(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.harness = TailHarness(members=1)
        self.target = target()
        self.harness.table(self.target)
        self.token = self.harness.control_plane.token("tok-rita", reader_on(DATASET_ID))
        self.worker = self.harness.worker

    async def tail(self, after=None, **query) -> dict:
        return await tail_rows_view(self.harness.request(token=self.token, after=after if after is not None else {}, **query), "mat-1", "orders", worker=self.worker)

    async def activate(self) -> None:
        try:
            await self.tail()
        except WebError:
            pass
        await self.harness.settle()

    @property
    def table(self):
        return self.worker.tables["tail:mat-1:orders"]


class KafkaFailureTests(FailureCase):
    async def test_a_failed_fetch_keeps_the_buffer_and_the_next_consumer_resumes_with_no_gap_or_duplicate(self) -> None:
        self.harness.deliver(tick(self.target))
        await self.activate()
        self.harness.produce("orders", 0, 5)
        await self.harness.fetch()
        source = self.harness.clients[0].sources[-1]
        source.getmany_failures.append(ConnectionError("broker went away"))
        self.harness.produce("orders", 0, 5)

        with self.assertRaises(ConnectionError):
            await self.worker.fetch_once()
        self.assertEqual(5, self.table.buffer.rows(), "what was read stays")
        await self.worker.reset_source()
        await self.harness.fetch(2)

        self.assertEqual(2, len(self.harness.clients[0].sources), "a new consumer")
        self.assertEqual([(0, offset) for offset in range(10)], offsets_of(await self.tail()))

    async def test_the_fetch_loop_carries_on_after_a_failure(self) -> None:
        self.worker.background = True
        self.harness.deliver(tick(self.target))
        self.harness.produce("orders", 1, 3)
        self.worker.idle_sleep_seconds = 0.01
        with patch.object(worker_module, "LOOP_RETRY_SECONDS", (0.01,)):
            try:
                await self.tail()
            except WebError:
                pass
            await self.harness.step()
            await asyncio.sleep(0.05)
            self.harness.clients[0].sources[-1].getmany_failures.append(RuntimeError("fetch failed"))
            for _ in range(20):
                await asyncio.sleep(0.02)
                await self.harness.step()
            answer = await self.tail()
            await self.worker.stop()
        self.assertEqual([(1, 0), (1, 1), (1, 2)], offsets_of(answer))

    async def test_a_partition_without_a_leader_holds_back_only_itself_and_reports_the_lag(self) -> None:
        self.harness.deliver(tick(self.target))
        await self.activate()
        source = self.harness.clients[0].sources[-1]
        source.stalled.add(TP("orders", 2))
        self.harness.produce("orders", 0, 3)
        self.harness.produce("orders", 2, 3)
        await self.harness.fetch()
        self.harness.clock.advance(5)
        await self.harness.fetch()

        answer = await self.tail()

        self.assertEqual([(0, 0), (0, 1), (0, 2)], offsets_of(answer), "the others keep coming")
        self.assertGreaterEqual(int(answer["headers"]["X-Tail-Lag-Ms"]), 5_000)
        source.stalled.clear()
        await self.harness.fetch(2)
        answer = await self.tail()
        self.assertEqual(6, int(answer["headers"]["X-Tail-Rows"]))
        self.assertEqual("0", answer["headers"]["X-Tail-Lag-Ms"])

    async def test_a_deleted_topic_makes_the_table_unavailable_and_it_recovers_when_the_topic_returns(self) -> None:
        other = target(table="payments", topic="payments")
        self.harness.table(other)
        self.harness.deliver(tick(self.target))
        self.harness.deliver(tick(other))
        await self.activate()
        try:
            await tail_rows_view(self.harness.request(token=self.token, after={}), "mat-1", "payments", worker=self.worker)
        except WebError:
            pass
        await self.harness.settle()
        self.harness.kafka.delete("orders")
        self.harness.clock.advance(31)

        await self.harness.step()

        self.assertEqual(("unavailable", "topic_missing"), (self.table.state, self.table.reason))
        with self.assertRaises(WebError) as caught:
            await self.tail()
        self.assertEqual((503, "topic_unavailable"), (caught.exception.status_code, caught.exception.details["reason"]))
        self.assertTrue(self.worker.tables["tail:mat-1:payments"].active, "nothing else on the member is affected")
        self.harness.kafka.create("orders", 3)
        self.harness.produce("orders", 0, 1)
        self.harness.clock.advance(31)
        await self.activate()
        self.assertEqual([(0, 0)], offsets_of(await self.tail()), "a request a while later reads it again")


class ControlPlaneFailureTests(FailureCase):
    async def test_a_seed_that_cannot_read_the_schema_anywhere_is_retried_and_not_ready_meanwhile(self) -> None:
        self.harness.control_plane.schema_down = True
        self.harness.deliver(tick(self.target))
        with patch("midstream.runtime_app.table_state.load_table", side_effect=OSError("no GCS either")):
            await self.activate()
            with self.assertRaises(WebError) as caught:
                await self.tail()
        self.assertEqual("tail_not_ready", caught.exception.details["reason"])
        self.assertIn("no GCS either", self.table.last_error)
        self.harness.control_plane.schema_down = False
        self.harness.clock.advance(2)
        await self.harness.settle()
        self.assertEqual("live", (await self.tail())["headers"]["X-Tail-State"])


class MessageTests(FailureCase):
    def test_malformed_messages_are_logged_and_dropped(self) -> None:
        reset_worker(self.worker)
        self.addCleanup(reset_worker, None)
        with self.assertLogs("midstream.tail_app.handlers", level=logging.WARNING):
            handle_tail_message({"kind": "tail_tick"})
        with self.assertLogs("midstream.tail_app.handlers", level=logging.WARNING):
            handle_tail_message({"kind": "tail_cancel", "target": self.target.to_dict(), "issued_at": "t", "reason": "bored"})
        with self.assertLogs("midstream.tail_app.worker", level=logging.WARNING):
            self.worker.apply_event(b"{not json")
        self.worker.apply_event(json.dumps(["not", "an", "event"]).encode())
        self.assertEqual({}, self.worker.tables)

    async def test_duplicate_and_late_ticks_change_nothing(self) -> None:
        self.harness.produce("orders", 0, 4)
        self.harness.deliver(tick(self.target, offsets={0: 1}))
        await self.activate()
        before = offsets_of(await self.tail({0: 1}))

        for _ in range(3):
            self.harness.deliver(tick(self.target, offsets={0: 1}, issued_at="2026-10-23T11:00:00Z"))
        self.harness.deliver(tick(self.target, offsets={0: 0}, issued_at="2026-10-23T10:00:00Z"))
        await self.harness.settle()

        self.assertEqual(before, offsets_of(await self.tail({0: 1})))
        self.assertEqual(1, self.table.committed[0])
        self.assertEqual(1, self.table.counters.seeds)

    async def test_a_commit_ahead_of_ingest_trims_nothing_unread(self) -> None:
        self.harness.deliver(tick(self.target, retention_seconds=60))
        await self.activate()
        self.harness.produce("orders", 0, 10)
        # The table is already past what the tail has read.
        self.harness.table_event("table_committed", "mat-1", "orders", {"offsets": {"0": 7}})
        await self.harness.fetch()
        await self.harness.step()

        # A run pinned before that commit, at offset 3, still finds its tail within the retention.
        self.assertEqual([(0, offset) for offset in range(4, 10)], offsets_of(await self.tail({0: 3})))
        self.assertEqual(7, self.table.buffer.partitions[0].committed)

    async def test_events_of_other_tables_and_of_older_schemas_change_nothing(self) -> None:
        self.harness.deliver(tick(self.target))
        await self.activate()
        schema_id = self.table.schema.schema_id
        self.harness.table_event("table_committed", "mat-9", "orders", {"offsets": {"0": 99}})
        self.harness.table_event("schema_changed", "mat-1", "orders", {"schema_id": 0, "schema": {"type": "struct", "fields": []}, "fork_map": {}})
        self.harness.table_event("table_committed", "mat-1", "orders", {"offsets": "nonsense"})
        self.assertEqual(schema_id, self.table.schema.schema_id)
        self.assertEqual({}, self.table.committed)


class ServingTests(FailureCase):
    async def test_more_requests_than_serve_slots_queue_and_all_answer(self) -> None:
        self.harness.produce("orders", 0, 50)
        self.harness.deliver(tick(self.target))
        await self.activate()

        answers = await asyncio.gather(*(self.tail() for _ in range(self.worker.config.serve_concurrency * 3)))

        self.assertEqual({"50"}, {answer["headers"]["X-Tail-Rows"] for answer in answers})

    async def test_a_request_for_a_table_that_never_catches_up_answers_at_its_deadline(self) -> None:
        self.harness.deliver(tick(self.target))
        started = asyncio.get_running_loop().time()

        with self.assertRaises(WebError) as caught:
            await self.tail(wait_ms=300)

        self.assertGreaterEqual(asyncio.get_running_loop().time() - started, 0.25)
        self.assertEqual("tail_not_ready", caught.exception.details["reason"])

    async def test_a_failing_peek_consumer_leaves_the_tails_own_alone(self) -> None:
        self.harness.produce("orders", 0, 3_000)
        self.harness.deliver(tick(self.target, max_rows_per_table=1_000))
        await self.activate()
        self.harness.clients[0].fail_peek = True

        with self.assertRaises(WebError) as caught:
            await tail_head_view(self.harness.request(token=self.token, rows=5), "mat-1", "orders", worker=self.worker)

        self.assertEqual((503, "peek_failed"), (caught.exception.status_code, caught.exception.details["reason"]))
        self.assertEqual("backlogged", self.table.state)
        with self.assertRaises(WebError) as tail_error:
            await self.tail()
        self.assertEqual("tail_backlogged", tail_error.exception.details["reason"])


class ForwardedRefusalTests(unittest.IsolatedAsyncioTestCase):
    async def test_an_owner_answering_rebalancing_is_passed_through_unchanged(self) -> None:
        harness = TailHarness(members=2)
        tail_target = target()
        harness.table(tail_target)
        token = harness.control_plane.token("tok", reader_on(DATASET_ID))
        owner = harness.owner_of("mat-1", "orders")
        other = next(worker for worker in harness.workers if worker is not owner)
        for worker in harness.workers:
            await worker.refresh_member_map()
        # The owner lost the partition a moment ago, and the map has not caught up.
        owner_app = harness.apps[harness.workers.index(owner)]
        owner_app.partitions = set()

        with self.assertRaises(WebError) as caught:
            await tail_rows_view(harness.request(token=token, after={}), "mat-1", "orders", worker=other)

        self.assertEqual((409, "tail_rebalancing"), (caught.exception.status_code, caught.exception.details["reason"]))


if __name__ == "__main__":
    unittest.main()
