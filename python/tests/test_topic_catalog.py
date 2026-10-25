from __future__ import annotations

import asyncio
from collections import namedtuple
from collections.abc import Callable
import threading
import time
import types
import unittest
from unittest.mock import patch

from midstream.config import ControlPlaneConfig
from midstream.kafka.metadata import (
    DEFAULT_REQUEST_TIMEOUT_SECONDS,
    AIOKafkaMetadataClient,
    TopicListing,
    topic_listing_from_description,
)
from midstream.services.topic_catalog import (
    TopicCatalogCache,
    TopicCatalogUnavailable,
    classify_topic,
)
from midstream.web.errors import WebError
from midstream.web.topics import analyze_topics_view_async


class FakeClock:
    def __init__(self) -> None:
        self.now = 1_000.0

    def __call__(self) -> float:
        return self.now


def listing(name: str, *, partitions: int = 3, internal: bool = False) -> TopicListing:
    return TopicListing(name=name, partitions=partitions, replication_factor=2, internal=internal)


def description(name: str, *, partitions: int = 1, error_code: int = 0, internal: bool = False) -> dict:
    return {
        "topic": name,
        "error_code": error_code,
        "is_internal": internal,
        "partitions": [{"partition": index, "replicas": [0, 1]} for index in range(partitions)],
    }


class TopicListingTests(unittest.TestCase):
    def test_listing_reads_partitions_replication_and_internal_flag(self) -> None:
        self.assertEqual(
            topic_listing_from_description(description("__consumer_offsets", partitions=50, internal=True)),
            TopicListing(name="__consumer_offsets", partitions=50, replication_factor=2, internal=True),
        )

    def test_listing_leaves_out_a_topic_this_client_may_not_describe(self) -> None:
        self.assertIsNone(topic_listing_from_description(description("payroll", error_code=29)))

    def test_listing_keeps_a_topic_whose_partitions_are_temporarily_unavailable(self) -> None:
        self.assertEqual(
            topic_listing_from_description(description("orders", partitions=0, error_code=5)),
            TopicListing(name="orders", partitions=0, replication_factor=0),
        )

    def test_metadata_client_lists_every_topic_from_one_request_sorted_by_name(self) -> None:
        requests: list[object] = []

        class FakeAdmin:
            def __init__(self, **_: object) -> None:
                pass

            async def start(self) -> None:
                pass

            async def describe_topics(self, topics: list[str] | None = None) -> list[dict]:
                requests.append(topics)
                return [description("orders", partitions=3), description("payroll", error_code=29),
                        description("__consumer_offsets", internal=True)]

        client = AIOKafkaMetadataClient(bootstrap_servers="kafka:9092", admin_factory=FakeAdmin)

        listings = asyncio.run(client.list_topics_async())

        self.assertEqual(requests, [None])
        self.assertEqual([item.name for item in listings], ["__consumer_offsets", "orders"])


class ClassifyTopicTests(unittest.TestCase):
    def test_labels_match_the_topics_seen_on_a_real_cluster(self) -> None:
        config = ControlPlaneConfig(
            default_runtime_name_prefix="develop",
            materialization_requests_topic="develop-materialization-requests",
            materialization_status_topic="develop-materialization-status",
            kaspr_app_name="develop-materializer-control-plane-app",
        )
        cases = {
            "__consumer_offsets": "kafka",
            "__transaction_state": "kafka",
            "develop-materialization-requests": "materializer",
            "develop-materialization-status": "materializer",
            "develop-mat-229a798b6179-signals": "materializer",
            "develop-mat-229a798b6179-app-assignor-leader": "materializer",
            "develop-materializer-control-plane-app-develop-state-changelog": "materializer",
            "develop-materializer-control-plane-app-schedule-actions": "materializer",
            "joiner-app-orders-changelog": "stream",
            "joiner-app-assignor-leader": "stream",
            "billing-__assignor-__leader": "stream",
            "orders": None,
            "products": None,
            "materializer-loadtest-orders": None,
            # Only exact control-topic names and real runtime ids are claimed.
            "develop-materialization-requests-archive": None,
            "develop-mat-notahexid-signals": None,
        }
        for name, category in cases.items():
            self.assertEqual(classify_topic(listing(name), config=config), category, name)

    def test_kafka_flag_marks_an_internal_topic_whatever_its_name(self) -> None:
        self.assertEqual(classify_topic(listing("audit", internal=True), config=ControlPlaneConfig()), "kafka")


class TopicCatalogCacheTests(unittest.TestCase):
    def setUp(self) -> None:
        self.config = ControlPlaneConfig(topic_catalog_ttl_seconds=15, topic_catalog_min_refresh_seconds=5)
        self.clock = FakeClock()
        self.cache = TopicCatalogCache(clock=self.clock)
        self.loads = 0
        self.failure: Exception | None = None

    async def _load(self) -> list[TopicListing]:
        self.loads += 1
        await asyncio.sleep(0)  # let concurrent callers arrive while this load is in flight
        if self.failure is not None:
            raise self.failure
        return [listing("orders"), listing("__consumer_offsets", partitions=50, internal=True)]

    def _get(self, *, force: bool = False):
        return asyncio.run(self.cache.get(self._load, config=self.config, force=force))

    def test_concurrent_callers_share_a_single_load(self) -> None:
        async def fifty_callers():
            return await asyncio.gather(*(self.cache.get(self._load, config=self.config) for _ in range(50)))

        results = asyncio.run(fifty_callers())

        self.assertEqual(self.loads, 1)
        self.assertTrue(all(catalog is results[0][0] and error is None for catalog, error in results))

    def test_catalog_items_are_in_wire_form_with_categories(self) -> None:
        catalog, error = self._get()

        self.assertIsNone(error)
        self.assertEqual(
            catalog.items,
            (
                {"topic": "orders", "partitions": 3, "replication_factor": 2},
                {"topic": "__consumer_offsets", "partitions": 50, "replication_factor": 2, "category": "kafka"},
            ),
        )

    def test_catalog_is_reused_within_the_ttl_and_reloaded_after_it(self) -> None:
        self._get()
        self.clock.now += 14
        self._get()
        self.assertEqual(self.loads, 1)

        self.clock.now += 2
        self._get()
        self.assertEqual(self.loads, 2)

    def test_forced_refreshes_are_spaced_however_often_they_are_asked_for(self) -> None:
        self._get()
        for _ in range(10):
            self.clock.now += 0.4
            self._get(force=True)
        self.assertEqual(self.loads, 1)

        self.clock.now += 1.1
        self._get(force=True)
        self.assertEqual(self.loads, 2)

    def test_failed_reload_serves_the_previous_catalog_and_backs_off(self) -> None:
        first, _ = self._get()
        self.clock.now += 20
        self.failure = RuntimeError("broker unreachable")

        catalog, error = self._get()
        self.assertIs(catalog, first)
        self.assertEqual(error, "broker unreachable")

        self.clock.now += 1
        self.assertEqual(self._get(), (first, "broker unreachable"))
        self.assertEqual(self.loads, 2)

        self.clock.now += 5
        self.failure = None
        catalog, error = self._get()
        self.assertIsNone(error)
        self.assertEqual(self.loads, 3)

    def test_failure_with_nothing_cached_raises_and_is_not_retried_at_once(self) -> None:
        self.failure = RuntimeError("broker unreachable")
        with self.assertRaisesRegex(RuntimeError, "broker unreachable"):
            self._get()

        self.clock.now += 1
        with self.assertRaises(TopicCatalogUnavailable):
            self._get()
        self.assertEqual(self.loads, 1)

        self.clock.now += 5
        self.failure = None
        self.assertIsNone(self._get()[1])
        self.assertEqual(self.loads, 2)

    def test_a_load_that_outlives_the_timeout_fails_with_a_readable_reason(self) -> None:
        async def never_answers() -> list[TopicListing]:
            await asyncio.sleep(5)
            return []

        with patch("midstream.services.topic_catalog.LOAD_TIMEOUT_SECONDS", 0.01):
            with self.assertRaisesRegex(TimeoutError, "Kafka did not answer within 0.01s"):
                asyncio.run(self.cache.get(never_answers, config=self.config))


TopicPartition = namedtuple("TopicPartition", "topic partition")
#: A deadline under test: short, yet long enough that the held request has surely started when it
#: passes, however loaded the machine.
DEADLINE = 0.5
HOLD_SECONDS = 10.0


class FakeKafka:
    """aiokafka's admin client and consumer, as the metadata client drives them on its own loop.

    A request kind in `hold` waits — on the client's loop, as an unresponsive broker would — until
    it is cancelled, which `cancelled` records, or for `HOLD_SECONDS`, far past any deadline under
    test. `block`, when set, runs inside `describe_topics` and blocks the client's loop outright.
    """

    def __init__(self) -> None:
        self.partitions = {"orders": 2}
        self.committed = {"orders": {0: 40, 1: 90}}
        self.hold: set[str] = set()
        self.block: Callable[[], None] | None = None
        self.start_seconds = 0.0
        self.cancelled = threading.Event()
        self.started: list[str] = []
        self.closed: list[str] = []
        #: The topics each metadata request of the admin client named; None asks for every topic.
        self.describe_requests: list[list[str] | None] = []

    async def request(self, kind: str) -> None:
        if kind not in self.hold:
            return
        try:
            await asyncio.sleep(HOLD_SECONDS)
        except asyncio.CancelledError:
            self.cancelled.set()
            raise

    def admin(self, **_: object) -> "FakeAdmin":
        return FakeAdmin(self)

    def consumer(self, **_: object) -> "FakeConsumer":
        return FakeConsumer(self)


class FakeAdmin:
    def __init__(self, kafka: FakeKafka) -> None:
        self.kafka = kafka

    async def start(self) -> None:
        self.kafka.started.append("admin")
        await asyncio.sleep(self.kafka.start_seconds)
        await self.kafka.request("admin.start")

    async def close(self) -> None:
        self.kafka.closed.append("admin")

    async def describe_topics(self, topics: list[str] | None = None) -> list[dict]:
        if self.kafka.block is not None:
            self.kafka.block()
        await self.kafka.request("describe")
        self.kafka.describe_requests.append(None if topics is None else list(topics))
        names = sorted(self.kafka.partitions) if topics is None else topics
        return [
            description(name, partitions=self.kafka.partitions[name])
            if name in self.kafka.partitions
            else description(name, partitions=0, error_code=3)
            for name in names
        ]

    async def list_consumer_group_offsets(self, group_id: str) -> dict:
        await self.kafka.request("group")
        return {
            TopicPartition(topic, partition): types.SimpleNamespace(offset=offset)
            for topic, offsets in self.kafka.committed.items()
            for partition, offset in offsets.items()
        }


class FakeTrackingClient:
    """aiokafka's client as far as it tracks topics: every metadata refresh names the topics it
    tracks, or asks for every topic while it tracks none."""

    def __init__(self) -> None:
        self._topics: set[str] = set()

    def set_topics(self, topics: list[str]) -> None:
        self._topics = set(topics)


class FakeConsumer:
    def __init__(self, kafka: FakeKafka) -> None:
        self.kafka = kafka
        self._client = FakeTrackingClient()

    async def start(self) -> None:
        self.kafka.started.append("consumer")
        await asyncio.sleep(self.kafka.start_seconds)
        await self.kafka.request("consumer.start")

    async def stop(self) -> None:
        self.kafka.closed.append("consumer")

    async def beginning_offsets(self, partitions: list[TopicPartition]) -> dict:
        # As aiokafka does for a partition its metadata does not know yet: it tracks the topic.
        self._client._topics.update(partition.topic for partition in partitions)
        await self.kafka.request("offsets")
        return {partition: 0 for partition in partitions}

    async def end_offsets(self, partitions: list[TopicPartition]) -> dict:
        self._client._topics.update(partition.topic for partition in partitions)
        await self.kafka.request("offsets")
        return {partition: 100 * (partition.partition + 1) for partition in partitions}


class LoopHandshake:
    """Proves an event loop ran while a blocking call was in flight on another thread.

    `block`, called inside the blocking call, waits until the loop has noticed the call started
    and answered. Were the caller blocking its own loop on the call, nothing could answer: it
    would wait out `timeout` and record that the loop never ran meanwhile.
    """

    def __init__(self, *, timeout: float = 5.0) -> None:
        self.timeout = timeout
        self.started = threading.Event()
        self.answered = threading.Event()
        self.answered_in_flight: bool | None = None

    def block(self) -> None:
        self.started.set()
        self.answered_in_flight = self.answered.wait(self.timeout)

    async def answer(self) -> None:
        deadline = time.monotonic() + self.timeout
        while not self.started.is_set() and time.monotonic() < deadline:
            await asyncio.sleep(0.005)
        self.answered.set()


def eventually(predicate: Callable[[], bool], *, timeout: float = 5.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return predicate()


class MetadataClientDeadlineTests(unittest.TestCase):
    """The metadata client's requests run on its own loop; callers await them, with a deadline.

    Its synchronous calls once blocked whoever made them — the maintenance agent and the analyze
    view, on the loop that serves the API — with no deadline at all.
    """

    def setUp(self) -> None:
        self.kafka = FakeKafka()
        patcher = patch(
            "midstream.kafka.metadata._topic_partition_type",
            return_value=TopicPartition,
        )
        patcher.start()
        self.addCleanup(patcher.stop)

    def client(self, *, timeout: float = DEFAULT_REQUEST_TIMEOUT_SECONDS) -> AIOKafkaMetadataClient:
        client = AIOKafkaMetadataClient(
            bootstrap_servers="kafka:9092",
            admin_factory=self.kafka.admin,
            consumer_factory=self.kafka.consumer,
            request_timeout_seconds=timeout,
        )
        self.addCleanup(client._runner.close)
        return client

    def warm_client(self, *, timeout: float = DEADLINE) -> AIOKafkaMetadataClient:
        """A client whose admin client and consumer have started, so a deadline under test covers
        nothing but the request that is held."""
        client = self.client(timeout=timeout)
        client.describe_topics(["orders"])
        client.consumer_group_offsets("kaspr-mat-1-app")
        return client

    def test_no_request_names_a_topic_so_a_broker_makes_none(self) -> None:
        # A metadata request that names a topic makes a broker that creates topics on demand create
        # it: describing a misspelt topic made it, and on the lab 52 deleted topics came back at
        # once, from the topics the offset lookups had left tracked.
        client = self.client()

        described = client.describe_topics(["orders", "ghost"])

        self.assertEqual(self.kafka.describe_requests, [None])
        self.assertTrue(described["orders"].exists)
        self.assertEqual(described["orders"].end_offsets, {0: 100, 1: 200})
        self.assertFalse(described["ghost"].exists)
        self.assertEqual(client._consumer._client._topics, set(), "the offset lookups left a topic tracked")

    def test_awaited_calls_answer_as_the_blocking_ones_do(self) -> None:
        client = self.client()

        described = asyncio.run(client.describe_topics_async(["orders", "ghost"]))
        offsets = asyncio.run(client.consumer_group_offsets_async("kaspr-mat-1-app"))

        self.assertEqual(described, client.describe_topics(["orders", "ghost"]))
        self.assertEqual(described["orders"].end_offsets, {0: 100, 1: 200})
        self.assertFalse(described["ghost"].exists)
        self.assertEqual(offsets, client.consumer_group_offsets("kaspr-mat-1-app"))
        self.assertEqual(offsets, {"orders": {0: 40, 1: 90}})

    def test_the_callers_loop_keeps_running_while_kafka_is_asked(self) -> None:
        handshake = LoopHandshake()
        self.kafka.block = handshake.block
        client = self.client()

        async def scenario():
            answering = asyncio.create_task(handshake.answer())
            described = await client.describe_topics_async(["orders"])
            await answering
            return described

        described = asyncio.run(scenario())

        self.assertTrue(handshake.answered_in_flight, "the caller's loop stood still while Kafka was asked")
        self.assertTrue(described["orders"].exists)

    def test_an_awaited_call_gives_up_at_its_deadline_and_cancels_the_request(self) -> None:
        for kind, call in (
            ("describe", lambda client: client.describe_topics_async(["orders"])),
            ("group", lambda client: client.consumer_group_offsets_async("kaspr-mat-1-app")),
            ("describe", lambda client: client.list_topics_async()),
        ):
            with self.subTest(kind=kind):
                client = self.warm_client()
                self.kafka.hold = {kind}
                self.kafka.cancelled.clear()

                with self.assertRaisesRegex(TimeoutError, rf"^Kafka did not answer within {DEADLINE:g}s$"):
                    asyncio.run(call(client))
                # Not merely abandoned: the request on the client's own loop was cancelled.
                self.assertTrue(self.kafka.cancelled.wait(5))
                self.kafka.hold = set()

    def test_a_blocking_call_gives_up_at_its_deadline_and_cancels_the_request(self) -> None:
        for kind, call in (
            ("describe", lambda client: client.describe_topics(["orders"])),
            ("group", lambda client: client.consumer_group_offsets("kaspr-mat-1-app")),
        ):
            with self.subTest(kind=kind):
                client = self.warm_client()
                self.kafka.hold = {kind}
                self.kafka.cancelled.clear()

                with self.assertRaisesRegex(TimeoutError, rf"^Kafka did not answer within {DEADLINE:g}s$"):
                    call(client)
                self.assertTrue(self.kafka.cancelled.wait(5))
                self.kafka.hold = set()

    def test_the_client_answers_again_after_a_deadline(self) -> None:
        client = self.warm_client()
        self.kafka.hold = {"offsets"}
        with self.assertRaises(TimeoutError):
            asyncio.run(client.describe_topics_async(["orders"]))

        self.kafka.hold = set()

        self.assertEqual(asyncio.run(client.describe_topics_async(["orders"]))["orders"].end_offsets, {0: 100, 1: 200})
        self.assertEqual(self.kafka.started, ["admin", "consumer"])

    def test_a_start_cut_short_by_a_deadline_is_closed_and_started_again(self) -> None:
        """A part-started aiokafka client may hold connections and background tasks."""
        client = self.client(timeout=DEADLINE)
        self.kafka.hold = {"consumer.start"}
        with self.assertRaises(TimeoutError):
            asyncio.run(client.describe_topics_async(["orders"]))
        self.assertTrue(eventually(lambda: self.kafka.closed == ["consumer"]), self.kafka.closed)

        self.kafka.hold = set()
        described = asyncio.run(client.describe_topics_async(["orders"]))

        self.assertTrue(described["orders"].exists)
        self.assertEqual(self.kafka.started, ["admin", "consumer", "consumer"])
        self.assertEqual(self.kafka.closed, ["consumer"])

    def test_concurrent_first_calls_start_each_client_once(self) -> None:
        self.kafka.start_seconds = 0.05
        client = self.client()

        async def scenario():
            return await asyncio.gather(*(client.describe_topics_async(["orders"]) for _ in range(5)))

        results = asyncio.run(scenario())

        self.assertTrue(all(result["orders"].exists for result in results))
        self.assertEqual(self.kafka.started, ["admin", "consumer"])

    def test_the_deadline_must_be_positive(self) -> None:
        for timeout in (0, -1):
            with self.subTest(timeout=timeout), self.assertRaises(ValueError):
                AIOKafkaMetadataClient(bootstrap_servers="kafka:9092", request_timeout_seconds=timeout)


class AnalyzeViewEventLoopTests(unittest.TestCase):
    """`POST /topics/analyze` with the real metadata client: Kafka is awaited, never waited on."""

    def setUp(self) -> None:
        self.kafka = FakeKafka()
        patcher = patch(
            "midstream.kafka.metadata._topic_partition_type",
            return_value=TopicPartition,
        )
        patcher.start()
        self.addCleanup(patcher.stop)
        self.client = AIOKafkaMetadataClient(
            bootstrap_servers="kafka:9092",
            admin_factory=self.kafka.admin,
            consumer_factory=self.kafka.consumer,
        )
        self.addCleanup(self.client._runner.close)

    def analyze(self, body: dict) -> dict:
        return analyze_topics_view_async(body, metadata_client=self.client, config=ControlPlaneConfig())

    def test_the_loop_keeps_serving_while_an_analysis_waits_on_kafka(self) -> None:
        handshake = LoopHandshake()
        self.kafka.block = handshake.block

        async def scenario():
            answering = asyncio.create_task(handshake.answer())
            payload = await self.analyze({"topics": ["orders"]})
            await answering
            return payload

        payload = asyncio.run(scenario())

        self.assertTrue(handshake.answered_in_flight, "the loop that serves the API stood still")
        self.assertEqual(payload["valid"], 1)
        self.assertEqual(payload["items"][0]["partitions"], 2)

    def test_an_analysis_kafka_does_not_answer_is_a_503_and_its_request_is_cancelled(self) -> None:
        asyncio.run(self.analyze({"topics": ["orders"]}))  # both clients started
        self.kafka.hold = {"describe"}

        with patch("midstream.services.topic_analysis.ANALYZE_TIMEOUT_SECONDS", DEADLINE):
            with self.assertRaises(WebError) as raised:
                asyncio.run(self.analyze({"topics": ["orders"]}))

        self.assertEqual(raised.exception.status_code, 503)
        self.assertEqual(raised.exception.details, {"reason": f"Kafka did not answer within {DEADLINE:g}s"})
        self.assertTrue(self.kafka.cancelled.wait(5))


if __name__ == "__main__":
    unittest.main()
