"""Discovery samples both ends of every partition, within its budget and its deadlines (spec §8.8)."""

from __future__ import annotations

import asyncio
from collections import namedtuple
import json
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from materializer_control_plane.models import Materialization, TableSpec
from materializer_control_plane.schema.discovery import (
    FETCH_MAX_BYTES,
    MAX_PARTITION_FETCH_BYTES,
    KafkaTopicDiscoveryService,
)
from materializer_control_plane.table_names import table_names_for_topics
from materializer_control_plane.utils.background_loop import AdapterTimeoutError

TP = namedtuple("TopicPartition", ["topic", "partition"])


def finish_within(seconds: float, function):
    """Run `function` on a thread and return its result, failing if it is still running after `seconds`."""
    outcome: dict = {}

    def target() -> None:
        try:
            outcome["result"] = function()
        except BaseException as exc:  # noqa: BLE001 - re-raised on the test's thread
            outcome["error"] = exc

    thread = threading.Thread(target=target, daemon=True)
    thread.start()
    thread.join(seconds)
    if thread.is_alive():
        raise AssertionError(f"still waiting after {seconds}s: the wait has no deadline")
    if "error" in outcome:
        raise outcome["error"]
    return outcome["result"]


class FakeCluster:
    """Cluster metadata, as aiokafka's client fetches it for every topic."""

    def __init__(self, topics) -> None:
        self.topics = topics

    def partitions_for_topic(self, topic: str):
        return set(self.topics[topic]) if topic in self.topics else None


class FakeConsumer:
    """Topics' partitions as offset -> value, served the way aiokafka's `getmany` serves them.

    `partitions` are the `orders` topic's; `topics` gives several topics' instead.
    """

    def __init__(
        self,
        partitions: dict[int, dict[int, bytes | None]] | None = None,
        *,
        batch: int = 50,
        topics: dict[str, dict[int, dict[int, bytes | None]]] | None = None,
    ) -> None:
        if topics is None:
            topics = {} if partitions is None else {"orders": partitions}
        self.topics = topics
        self.batch = batch
        self.positions: dict = {}
        self.started = 0
        self.stopped = False
        self.assigned: list = []
        # (partition, offset) of the `orders` topic, and (topic, partition, offset) of every one.
        self.seeks: list[tuple[int, int]] = []
        self.topic_seeks: list[tuple[str, int, int]] = []
        self._client = SimpleNamespace(fetch_all_metadata=self._fetch_all_metadata)

    async def _fetch_all_metadata(self) -> FakeCluster:
        return FakeCluster(self.topics)

    def _log(self, tp) -> dict[int, bytes | None]:
        return self.topics[tp.topic][tp.partition]

    async def start(self) -> None:
        self.started += 1

    def partitions_for_topic(self, topic: str):
        # As aiokafka's: a consumer subscribed to nothing knows no topic's partitions, however the
        # cluster's metadata was fetched. Discovery once read them here and sampled nothing.
        return None

    def assign(self, tps) -> None:
        self.assigned = list(tps)

    async def beginning_offsets(self, tps):
        return {tp: min(self._log(tp), default=0) for tp in tps}

    async def end_offsets(self, tps):
        return {tp: max(self._log(tp), default=-1) + 1 for tp in tps}

    def seek(self, tp, offset: int) -> None:
        self.positions[tp] = offset
        self.topic_seeks.append((tp.topic, tp.partition, offset))
        if tp.topic == "orders":
            self.seeks.append((tp.partition, offset))

    async def getmany(self, *tps, timeout_ms: int = 0):
        result = {}
        for tp in tps:
            position = self.positions.get(tp, 0)
            log = self._log(tp)
            offsets = sorted(offset for offset in log if offset >= position)[: self.batch]
            if offsets:
                result[tp] = [SimpleNamespace(offset=offset, value=log[offset]) for offset in offsets]
                self.positions[tp] = offsets[-1] + 1
        if not result:
            await asyncio.sleep(timeout_ms / 1000)
        return result

    async def stop(self) -> None:
        self.stopped = True


def log_of(values: list) -> dict[int, bytes | None]:
    return {offset: None if value is None else json.dumps(value).encode() for offset, value in enumerate(values)}


def build_materialization(*topics: str) -> Materialization:
    topics = topics or ("orders",)
    return Materialization(
        materialization_id="mat-1",
        namespace="midstream",
        name="shop",
        tables=tuple(TableSpec(name=name, source_topic=topic, dataset="shop", dataset_id="ds-000000000001") for name, topic in zip(table_names_for_topics(topics), topics)),
        destination_uri="gs://lake",
        desired_phase="running",
        created_at="2026-07-12T00:00:00Z",
        updated_at="2026-07-12T00:00:00Z",
    )


class DiscoverySampleTests(unittest.TestCase):
    def service(self, consumer: FakeConsumer) -> KafkaTopicDiscoveryService:
        service = KafkaTopicDiscoveryService(
            bootstrap_servers="kafka:9092",
            consumer_factory=lambda *args, **kwargs: consumer,
            timestamp_factory=lambda: "2026-07-18T15:00:00Z",
        )
        self.addCleanup(service._runner.close)
        return service

    def test_both_ends_of_every_partition_are_sampled(self) -> None:
        consumer = FakeConsumer(
            {
                0: log_of([{"v": 1, "old": True}] * 50 + [{"v": 2, "new": "x"}] * 50),
                1: log_of([{"v": 1, "old": True}] * 50 + [{"v": 2, "new": "y"}] * 50),
            }
        )

        result = self.service(consumer).discover(build_materialization(), sample_records=20, sample_seconds=2)

        self.assertEqual(20, len(result.records("orders")))
        self.assertEqual(10, sum(1 for record in result.records("orders") if "old" in record))
        self.assertEqual(10, sum(1 for record in result.records("orders") if "new" in record))
        self.assertEqual("2026-07-18T15:00:00Z", result.discovered_at)
        self.assertEqual(2, result.samples["orders"].partitions)
        self.assertIn((0, 95), consumer.seeks)
        self.assertTrue(consumer.started and consumer.stopped)

    def test_a_short_partition_is_read_once_whole(self) -> None:
        consumer = FakeConsumer({0: log_of([{"v": index} for index in range(6)])})

        result = self.service(consumer).discover(build_materialization(), sample_records=200, sample_seconds=2)

        self.assertEqual([{"v": index} for index in range(6)], result.records("orders"))

    def test_tombstones_and_values_that_are_not_json_are_counted_not_sampled(self) -> None:
        log = log_of([{"v": 1}, None])
        log[2] = b"not json"
        log[3] = b"\xff\xfe"
        consumer = FakeConsumer({0: log})

        result = self.service(consumer).discover(build_materialization(), sample_records=10, sample_seconds=2)

        self.assertEqual([{"v": 1}], result.records("orders"))
        self.assertEqual(1, result.samples["orders"].tombstones)
        self.assertEqual(2, result.samples["orders"].undecodable)

    def test_an_empty_topic_yields_no_records(self) -> None:
        consumer = FakeConsumer({0: {}, 1: {}})

        result = self.service(consumer).discover(build_materialization())

        self.assertEqual([], result.records("orders"))
        self.assertEqual(2, result.samples["orders"].empty_partitions)
        self.assertTrue(consumer.stopped)

    def test_a_topic_kafka_does_not_know_yields_no_records(self) -> None:
        consumer = FakeConsumer({})

        result = self.service(consumer).discover(build_materialization())

        self.assertEqual([], result.records("orders"))

    def test_sampling_stops_at_its_time_budget_with_what_it_read(self) -> None:
        class Trickle(FakeConsumer):
            async def getmany(self, *tps, timeout_ms: int = 0):
                await asyncio.sleep(0.05)
                return await FakeConsumer.getmany(self, *tps, timeout_ms=timeout_ms)

        consumer = Trickle({0: log_of([{"v": index} for index in range(1000)])}, batch=1)

        started = time.monotonic()
        result = self.service(consumer).discover(build_materialization(), sample_records=200, sample_seconds=0.3)

        self.assertLess(time.monotonic() - started, 2)
        self.assertTrue(result.samples["orders"].timed_out)
        self.assertGreater(len(result.records("orders")), 0)
        self.assertLess(len(result.records("orders")), 200)


class MultiTopicDiscoveryTests(unittest.TestCase):
    def service(self, consumer: FakeConsumer, calls: list | None = None) -> KafkaTopicDiscoveryService:
        def factory(*args, **kwargs):
            if calls is not None:
                calls.append(kwargs)
            return consumer

        service = KafkaTopicDiscoveryService(
            bootstrap_servers="kafka:9092",
            consumer_factory=factory,
            timestamp_factory=lambda: "2026-07-18T15:00:00Z",
        )
        self.addCleanup(service._runner.close)
        return service

    def test_every_topic_is_sampled_at_once_by_one_small_fetching_consumer(self) -> None:
        consumer = FakeConsumer(
            topics={
                "orders": {0: log_of([{"order_id": index} for index in range(30)]), 1: log_of([{"order_id": "x"}] * 30)},
                "payments": {0: log_of([{"payment_id": index} for index in range(4)])},
            }
        )
        calls: list = []

        result = self.service(consumer, calls).discover(build_materialization("orders", "payments"), sample_records=10, sample_seconds=2)

        self.assertEqual(1, consumer.started)
        self.assertEqual(1, len(calls))
        self.assertEqual((FETCH_MAX_BYTES, MAX_PARTITION_FETCH_BYTES), (calls[0]["fetch_max_bytes"], calls[0]["max_partition_fetch_bytes"]))
        self.assertEqual({("orders", 0), ("orders", 1), ("payments", 0)}, {(tp.topic, tp.partition) for tp in consumer.assigned})
        self.assertEqual(10, len(result.records("orders")))
        self.assertTrue(all("order_id" in record for record in result.records("orders")))
        self.assertEqual([{"payment_id": index} for index in range(4)], result.records("payments"))
        self.assertEqual((2, 1), (result.samples["orders"].partitions, result.samples["payments"].partitions))

    def test_a_topic_kafka_does_not_know_yields_no_records_and_the_others_are_still_read(self) -> None:
        consumer = FakeConsumer(topics={"payments": {0: log_of([{"payment_id": 1}])}})

        result = self.service(consumer).discover(build_materialization("orders", "payments"))

        self.assertEqual([], result.records("orders"))
        self.assertEqual(0, result.samples["orders"].partitions)
        self.assertEqual([{"payment_id": 1}], result.records("payments"))

    def test_each_topic_is_sampled_from_both_ends_of_its_own_partitions(self) -> None:
        consumer = FakeConsumer(
            topics={
                "orders": {0: log_of([{"v": 1}] * 100)},
                "payments": {0: log_of([{"v": 1}] * 40), 1: log_of([{"v": 1}] * 40)},
            }
        )

        self.service(consumer).discover(build_materialization("orders", "payments"), sample_records=20, sample_seconds=2)

        # 20 records: orders' one partition reads 10 at each end, each of payments' two reads 5.
        self.assertIn(("orders", 0, 90), consumer.topic_seeks)
        self.assertIn(("payments", 0, 35), consumer.topic_seeks)
        self.assertIn(("payments", 1, 35), consumer.topic_seeks)

    def test_the_message_names_every_topic_discovery_could_not_reach(self) -> None:
        consumer = SlowConsumer(hang_in="start")
        service = KafkaTopicDiscoveryService(
            bootstrap_servers="kafka:9092",
            consumer_factory=lambda *args, **ignored: consumer,
            connect_timeout_seconds=0.05,
        )
        self.addCleanup(service._runner.close)

        with self.assertRaises(AdapterTimeoutError) as raised:
            finish_within(10, lambda: service.discover(build_materialization("orders", "payments", "refunds", "customers")))

        self.assertEqual(
            "connecting to Kafka to sample topics orders, payments, refunds and 1 more timed out after 0.05s",
            str(raised.exception),
        )


class SlowConsumer(FakeConsumer):
    """A consumer whose connect, poll or stop take `delay` seconds, or never finish."""

    def __init__(self, partitions=None, *, delay: float = 0.0, hang_in: str | None = None) -> None:
        super().__init__(partitions)
        self.delay = delay
        self.hang_in = hang_in
        self.cancelled = threading.Event()
        self.threads: set[str] = set()

    async def start(self) -> None:
        await self._wait("start")
        await super().start()

    async def getmany(self, *tps, timeout_ms: int = 0):
        await self._wait("getmany")
        return await super().getmany(*tps, timeout_ms=timeout_ms)

    async def stop(self) -> None:
        await self._wait("stop")
        await super().stop()

    async def _wait(self, step: str) -> None:
        self.threads.add(threading.current_thread().name)
        if self.hang_in != step:
            await asyncio.sleep(self.delay)
            return
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            self.cancelled.set()
            raise


class DiscoveryDeadlineTests(unittest.TestCase):
    def service(self, consumer, **kwargs) -> KafkaTopicDiscoveryService:
        service = KafkaTopicDiscoveryService(
            bootstrap_servers="kafka:9092",
            consumer_factory=lambda *args, **ignored: consumer,
            timestamp_factory=lambda: "2026-07-18T15:00:00Z",
            **kwargs,
        )
        self.addCleanup(service._runner.close)
        return service

    def test_a_connection_that_never_completes_is_not_an_empty_topic(self) -> None:
        for awaited in (False, True):
            with self.subTest(awaited=awaited):
                consumer = SlowConsumer(hang_in="start")
                service = self.service(consumer, connect_timeout_seconds=0.05)

                with self.assertRaises(AdapterTimeoutError) as raised:
                    if awaited:
                        finish_within(10, lambda: asyncio.run(service.discover_async(build_materialization())))
                    else:
                        finish_within(10, lambda: service.discover(build_materialization()))

                self.assertEqual(
                    str(raised.exception),
                    "connecting to Kafka to sample topic orders timed out after 0.05s",
                )
                self.assertTrue(consumer.cancelled.is_set())
                self.assertTrue(consumer.stopped, "the consumer that never connected was not stopped")

    def test_awaited_discovery_matches_discover_and_leaves_the_loop_running(self) -> None:
        def consumer() -> SlowConsumer:
            return SlowConsumer({0: log_of([{"id": 1, "name": "a"}])}, delay=0.1)

        expected = self.service(consumer()).discover(build_materialization())
        slow = consumer()
        service = self.service(slow)
        turns = 0

        async def scenario():
            nonlocal turns
            done = False

            async def tick() -> None:
                nonlocal turns
                while not done:
                    await asyncio.sleep(0.005)
                    turns += 1

            ticking = asyncio.ensure_future(tick())
            result = await service.discover_async(build_materialization())
            done = True
            await ticking
            return result

        result = asyncio.run(scenario())
        self.assertEqual(expected.records("orders"), result.records("orders"))
        self.assertEqual(slow.threads, {"schema-discovery"})
        # Connecting, polling and stopping take 0.3 s on the service's loop; a held loop would not turn.
        self.assertGreaterEqual(turns, 15)

    def test_a_stop_that_never_finishes_keeps_what_was_read(self) -> None:
        consumer = SlowConsumer({0: log_of([{"id": 1}])}, hang_in="stop")
        service = self.service(consumer)

        with patch("materializer_control_plane.schema.discovery.STOP_TIMEOUT_SECONDS", 0.05):
            result = finish_within(10, lambda: service.discover(build_materialization()))

        self.assertEqual([{"id": 1}], result.records("orders"))
        self.assertTrue(consumer.cancelled.wait(2))

    def test_a_consumer_that_blocks_the_clients_loop_still_meets_the_deadline(self) -> None:
        """The bounds inside the read need the client's loop to run; the caller's deadline does not."""

        class BlockingConsumer(FakeConsumer):
            async def start(self) -> None:
                time.sleep(1.0)

        service = self.service(BlockingConsumer({0: log_of([{"id": 1}])}), connect_timeout_seconds=0.05)

        with patch("materializer_control_plane.schema.discovery.STOP_TIMEOUT_SECONDS", 0.05), patch(
            "materializer_control_plane.schema.discovery.DEADLINE_MARGIN_SECONDS", 0.05
        ):
            started = time.monotonic()
            with self.assertRaises(AdapterTimeoutError) as raised:
                service.discover(build_materialization(), sample_seconds=0.05)
            waited = time.monotonic() - started

        self.assertEqual(str(raised.exception), "schema discovery from topic orders timed out after 0.2s")
        self.assertLess(waited, 0.9, "the caller waited for the blocked loop instead of its deadline")

    def test_the_deadline_covers_connecting_sampling_and_stopping(self) -> None:
        service = self.service(FakeConsumer())

        self.assertEqual(service.connect_timeout_seconds, 60.0)
        self.assertEqual(service.deadline_seconds(5), 60.0 + 5.0 + 5.0 + 5.0)
        with self.assertRaises(ValueError):
            KafkaTopicDiscoveryService(bootstrap_servers="kafka:9092", connect_timeout_seconds=0)


if __name__ == "__main__":
    unittest.main()
