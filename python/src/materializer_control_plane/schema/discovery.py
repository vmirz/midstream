"""Discovery: sample a materialization's source topics so each new table starts with the shapes its
topic already holds.

`start_discovery` reads up to `discovery_sample_records` records of every topic within
`discovery_sample_seconds`: half from the earliest offsets of every partition, half from just before
the latest (spec §8.8). Every topic is read at once, by one consumer, so the time it takes does not
grow with the number of topics; its fetches are kept small (`FETCH_MAX_BYTES`), so neither does the
memory. The samples go through the same evolution policy the committer uses, so a topic with several
historical shapes starts with its forks already in place, and the first minutes of running bring no
churn. An empty topic yields the metadata columns only.

The consumer runs on the service's own event loop, on a thread of its own: connecting may take
`connect_timeout_seconds`, sampling the budget given; `discover` waits for both plus a margin, then
cancels the read and raises `AdapterTimeoutError`. `discover_async` awaits the same read from
another event loop.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass, field
from importlib import import_module
import json
import logging
import math
import os
import time
from typing import Any

from materializer_control_plane.models import Materialization
from materializer_control_plane.utils.background_loop import AdapterTimeoutError, BackgroundLoop

LOGGER = logging.getLogger(__name__)

#: Seconds the discovery consumer may take to connect and load the topic's metadata. aiokafka gives
#: up on each request after 40 s but can retry a bootstrap or a metadata refresh for longer; 60 s
#: leaves a slow but healthy cluster room to answer, and still ends a connection that never
#: completes, which must not pass for an empty topic.
CONNECT_TIMEOUT_SECONDS = 60.0
#: Seconds to wait for the consumer to stop. Stopping is cleanup: past this it is abandoned, and
#: discovery returns what it read.
STOP_TIMEOUT_SECONDS = 5.0
#: Added to the bounds above to make the caller's deadline, so that they expire first and the
#: deadline catches only what they cannot, such as a consumer that blocks the client's loop.
DEADLINE_MARGIN_SECONDS = 5.0
DEFAULT_SAMPLE_RECORDS = 200
DEFAULT_SAMPLE_SECONDS = 5.0
#: One fetch waits at most this long for records, so the sampling budget is checked often.
POLL_TIMEOUT_MS = 500
#: Bytes one fetch returns at most, in all and per partition. Discovery needs a few records of every
#: partition, not a megabyte each: with many topics' partitions assigned at once, the defaults
#: (50 MiB, 1 MiB a partition) could hold hundreds of megabytes on the control plane. Kafka returns a
#: record larger than the limit anyway, so a large record never stalls sampling.
FETCH_MAX_BYTES = 8 * 1024 * 1024
MAX_PARTITION_FETCH_BYTES = 64 * 1024


def _utc_now_timestamp() -> str:
    from datetime import UTC, datetime

    return datetime.now(tz=UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


@dataclass(frozen=True, slots=True)
class DiscoverySample:
    """What discovery read: decoded record values, and how much of the topic it saw."""

    records: list[Any] = field(default_factory=list)
    partitions: int = 0
    empty_partitions: int = 0
    tombstones: int = 0
    undecodable: int = 0
    timed_out: bool = False


class KafkaTopicDiscoveryService:
    def __init__(
        self,
        *,
        bootstrap_servers: str,
        security_protocol: str | None = None,
        sasl_mechanism: str | None = None,
        sasl_username: str | None = None,
        sasl_password: str | None = None,
        connect_timeout_seconds: float = CONNECT_TIMEOUT_SECONDS,
        consumer_factory: Callable[..., Any] | None = None,
        timestamp_factory: Callable[[], str] | None = None,
    ) -> None:
        if not bootstrap_servers.strip():
            raise ValueError("bootstrap_servers is required for topic discovery")
        if connect_timeout_seconds <= 0:
            raise ValueError("connect_timeout_seconds must be > 0")
        self.bootstrap_servers = bootstrap_servers.strip()
        self.security_protocol = None if security_protocol in (None, "") else security_protocol.strip()
        self.sasl_mechanism = None if sasl_mechanism in (None, "") else sasl_mechanism.strip()
        self.sasl_username = None if sasl_username in (None, "") else sasl_username
        self.sasl_password = None if sasl_password in (None, "") else sasl_password
        self.connect_timeout_seconds = connect_timeout_seconds
        self.consumer_factory = _build_kafka_consumer if consumer_factory is None else consumer_factory
        self.timestamp_factory = _utc_now_timestamp if timestamp_factory is None else timestamp_factory
        self._runner = BackgroundLoop(name="schema-discovery")

    @classmethod
    def from_environment(cls) -> "KafkaTopicDiscoveryService":
        bootstrap_servers = os.getenv("K_KAFKA_BOOTSTRAP_SERVERS", "")
        if not bootstrap_servers.strip():
            raise RuntimeError("K_KAFKA_BOOTSTRAP_SERVERS is required for topic discovery")
        return cls(
            bootstrap_servers=bootstrap_servers,
            security_protocol=os.getenv("K_KAFKA_SECURITY_PROTOCOL"),
            sasl_mechanism=os.getenv("K_KAFKA_SASL_MECHANISM"),
            sasl_username=os.getenv("K_KAFKA_AUTH_USERNAME"),
            sasl_password=os.getenv("K_KAFKA_AUTH_PASSWORD"),
        )

    def deadline_seconds(self, sample_seconds: float) -> float:
        """The longest discovery waits: connecting, sampling and stopping, and a margin."""
        return self.connect_timeout_seconds + sample_seconds + STOP_TIMEOUT_SECONDS + DEADLINE_MARGIN_SECONDS

    def discover(
        self,
        materialization: Materialization,
        *,
        sample_records: int = DEFAULT_SAMPLE_RECORDS,
        sample_seconds: float = DEFAULT_SAMPLE_SECONDS,
    ):
        samples = self._runner.run(
            self._read_samples(materialization, sample_records=sample_records, sample_seconds=sample_seconds),
            timeout=self.deadline_seconds(sample_seconds),
            description=f"schema discovery from {_describe_topics(materialization.source_topics)}",
        )
        return self._result(samples)

    async def discover_async(
        self,
        materialization: Materialization,
        *,
        sample_records: int = DEFAULT_SAMPLE_RECORDS,
        sample_seconds: float = DEFAULT_SAMPLE_SECONDS,
    ):
        """`discover`, awaited without blocking the caller's event loop."""
        samples = await self._runner.run_async(
            self._read_samples(materialization, sample_records=sample_records, sample_seconds=sample_seconds),
            timeout=self.deadline_seconds(sample_seconds),
            description=f"schema discovery from {_describe_topics(materialization.source_topics)}",
        )
        return self._result(samples)

    def _read_samples(self, materialization: Materialization, *, sample_records: int, sample_seconds: float):
        return sample_topics(
            self.consumer_factory,
            topics=list(materialization.source_topics),
            bootstrap_servers=self.bootstrap_servers,
            security_protocol=self.security_protocol,
            sasl_mechanism=self.sasl_mechanism,
            sasl_username=self.sasl_username,
            sasl_password=self.sasl_password,
            sample_records=sample_records,
            sample_seconds=sample_seconds,
            connect_timeout_seconds=self.connect_timeout_seconds,
        )

    def _result(self, samples: dict[str, DiscoverySample]):
        from materializer_control_plane.services.workflow import DiscoveryResult

        return DiscoveryResult(
            records_by_topic={topic: list(sample.records) for topic, sample in samples.items()},
            discovered_at=self.timestamp_factory(),
            samples=samples,
        )


class StubDiscoveryService:
    """Discovery from fixed records, for tests and tools: the same records for every topic, or each
    topic's own from `records_by_topic`."""

    def __init__(self, records: list[Any] | None = None, *, records_by_topic: dict[str, list[Any]] | None = None) -> None:
        self.records = [] if records is None else list(records)
        self.records_by_topic = {} if records_by_topic is None else {topic: list(items) for topic, items in records_by_topic.items()}
        self.calls: list[dict[str, Any]] = []

    def discover(self, materialization: Materialization, **budget: Any):
        from materializer_control_plane.services.workflow import DiscoveryResult

        self.calls.append({"materialization_id": materialization.materialization_id, **budget})
        records = {topic: list(self.records_by_topic.get(topic, self.records)) for topic in materialization.source_topics}
        return DiscoveryResult(
            records_by_topic=records,
            discovered_at=materialization.updated_at,
            samples={topic: DiscoverySample(records=list(items)) for topic, items in records.items()},
        )


def _describe_topics(topics) -> str:
    topics = list(topics)
    if len(topics) == 1:
        return f"topic {topics[0]}"
    shown = ", ".join(topics[:3])
    return f"topics {shown}" + (f" and {len(topics) - 3} more" if len(topics) > 3 else "")


def _build_kafka_consumer(*topics: str, **kwargs):
    try:
        AIOKafkaConsumer = import_module("aiokafka").AIOKafkaConsumer
    except ImportError as exc:
        raise RuntimeError("aiokafka must be installed for topic discovery") from exc
    return AIOKafkaConsumer(*topics, **kwargs)


def _topic_partition(topic: str, partition: int):
    try:
        TopicPartition = import_module("aiokafka").TopicPartition
    except ImportError:  # tests without aiokafka
        from collections import namedtuple

        return namedtuple("TopicPartition", ["topic", "partition"])(topic, partition)
    return TopicPartition(topic, partition)


async def sample_topics(
    consumer_factory: Callable[..., Any],
    *,
    topics: list[str],
    bootstrap_servers: str,
    security_protocol: str | None,
    sasl_mechanism: str | None,
    sasl_username: str | None,
    sasl_password: str | None,
    sample_records: int,
    sample_seconds: float,
    connect_timeout_seconds: float = CONNECT_TIMEOUT_SECONDS,
    clock: Callable[[], float] = time.monotonic,
) -> dict[str, DiscoverySample]:
    """Read up to `sample_records` of every topic from both ends of its partitions, for at most
    `sample_seconds` in all, with one consumer.

    Only the budget running out ends sampling early; a connection that takes longer than
    `connect_timeout_seconds` raises `AdapterTimeoutError`, because a generic schema would then be
    deployed against a topic discovery never reached.
    """
    consumer_kwargs: dict[str, Any] = {
        "bootstrap_servers": bootstrap_servers,
        "auto_offset_reset": "earliest",
        "enable_auto_commit": False,
        "group_id": None,
        "fetch_max_bytes": FETCH_MAX_BYTES,
        "max_partition_fetch_bytes": MAX_PARTITION_FETCH_BYTES,
    }
    if security_protocol is not None:
        consumer_kwargs["security_protocol"] = security_protocol
    if sasl_mechanism is not None:
        consumer_kwargs["sasl_mechanism"] = sasl_mechanism
    if sasl_username is not None:
        consumer_kwargs["sasl_plain_username"] = sasl_username
    if sasl_password is not None:
        consumer_kwargs["sasl_plain_password"] = sasl_password

    described = _describe_topics(topics)
    consumer = consumer_factory(**consumer_kwargs)
    try:
        try:
            partitions = await asyncio.wait_for(_start(consumer, topics), timeout=connect_timeout_seconds)
        except TimeoutError:
            raise AdapterTimeoutError(f"connecting to Kafka to sample {described}", connect_timeout_seconds) from None
        return await _sample(
            consumer,
            partitions,
            sample_records=sample_records,
            sample_seconds=sample_seconds,
            clock=clock,
        )
    finally:
        await _stop_quietly(consumer, topic=described)


async def sample_topic(consumer_factory: Callable[..., Any], *, topic: str, **options: Any) -> DiscoverySample:
    """`sample_topics` for one topic."""
    samples = await sample_topics(consumer_factory, topics=[topic], **options)
    return samples[topic]


async def _start(consumer: Any, topics: list[str]) -> dict[str, list[int]]:
    """Start the consumer and read every topic's partitions from the cluster's metadata.

    A consumer that subscribed to nothing knows no topic's partitions (aiokafka's
    `partitions_for_topic` answers None), and subscribing would make a broker that auto-creates
    topics create a missing one. A metadata request for every topic creates nothing.
    """
    await consumer.start()
    cluster = await consumer._client.fetch_all_metadata()
    return {topic: sorted(cluster.partitions_for_topic(topic) or ()) for topic in topics}


async def _sample(
    consumer: Any,
    partitions: dict[str, list[int]],
    *,
    sample_records: int,
    sample_seconds: float,
    clock,
) -> dict[str, DiscoverySample]:
    samples = {topic: DiscoverySample() for topic in partitions}
    tps_by_topic = {
        topic: [_topic_partition(topic, partition) for partition in topic_partitions]
        for topic, topic_partitions in partitions.items()
        if topic_partitions
    }
    tps = [tp for topic_tps in tps_by_topic.values() for tp in topic_tps]
    if not tps:
        return samples
    consumer.assign(tps)
    beginnings = await consumer.beginning_offsets(tps)
    ends = await consumer.end_offsets(tps)
    # Each partition reads its head range, then its tail range: [(start, stop)], stop exclusive.
    plans: dict[Any, list[tuple[int, int]]] = {}
    empty: dict[str, int] = {topic: 0 for topic in tps_by_topic}
    for topic, topic_tps in tps_by_topic.items():
        per_end = max(1, math.ceil(sample_records / 2 / len(topic_tps)))
        for tp in topic_tps:
            start, stop = int(beginnings[tp]), int(ends[tp])
            if stop <= start:
                empty[topic] += 1
                continue
            head = (start, min(stop, start + per_end))
            tail_start = max(head[1], stop - per_end)
            plans[tp] = [head] if tail_start >= stop else [head, (tail_start, stop)]
    records: dict[str, list[Any]] = {topic: [] for topic in tps_by_topic}
    tombstones: dict[str, int] = {topic: 0 for topic in tps_by_topic}
    undecodable: dict[str, int] = {topic: 0 for topic in tps_by_topic}
    for tp, ranges in plans.items():
        consumer.seek(tp, ranges[0][0])
    active = set(plans)
    deadline = clock() + sample_seconds
    timed_out = False
    while active:
        remaining = deadline - clock()
        if remaining <= 0:
            timed_out = True
            break
        batch = await consumer.getmany(*active, timeout_ms=int(min(POLL_TIMEOUT_MS, remaining * 1000)) or 1)
        for tp, messages in batch.items():
            if tp not in active:
                continue
            topic = tp.topic
            ranges = plans[tp]
            for message in messages:
                if not ranges:
                    break
                start, stop = ranges[0]
                if message.offset < start:
                    continue
                if message.offset >= stop:
                    ranges.pop(0)
                    if ranges:
                        consumer.seek(tp, ranges[0][0])
                    break
                value = _deserialize_message_value(message.value)
                if value is None:
                    tombstones[topic] += 1
                elif value is _UNDECODABLE:
                    undecodable[topic] += 1
                else:
                    records[topic].append(value)
                if message.offset >= stop - 1:
                    ranges.pop(0)
                    if ranges:
                        consumer.seek(tp, ranges[0][0])
                    break
            if not ranges:
                active.discard(tp)
        # A topic with its sample stops reading; the others go on.
        for topic, topic_records in records.items():
            if len(topic_records) >= sample_records:
                active.difference_update(tps_by_topic[topic])
    for topic, topic_tps in tps_by_topic.items():
        unfinished = any(tp in active for tp in topic_tps)
        samples[topic] = DiscoverySample(
            records=records[topic][:sample_records],
            partitions=len(topic_tps),
            empty_partitions=empty[topic],
            tombstones=tombstones[topic],
            undecodable=undecodable[topic],
            timed_out=timed_out and unfinished,
        )
    return samples


async def _stop_quietly(consumer: Any, *, topic: str) -> None:
    """Stop the consumer, for at most `STOP_TIMEOUT_SECONDS`.

    A failure here does not replace what discovery read, or the error it is already raising.
    """
    stop = getattr(consumer, "stop", None)
    if not callable(stop):
        return
    try:
        await asyncio.wait_for(stop(), timeout=STOP_TIMEOUT_SECONDS)
    except Exception as exc:  # noqa: BLE001 - stopping is cleanup; discovery's own outcome stands
        LOGGER.warning("discovery consumer for topic %s did not stop cleanly: %r", topic, exc)


_UNDECODABLE = object()


def _deserialize_message_value(value: Any) -> Any:
    """A record's value as JSON; None for a tombstone; `_UNDECODABLE` for anything else."""
    if value is None:
        return None
    if isinstance(value, (bytes, bytearray)):
        try:
            value = value.decode("utf-8")
        except UnicodeDecodeError:
            return _UNDECODABLE
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return _UNDECODABLE
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return _UNDECODABLE
    return value
