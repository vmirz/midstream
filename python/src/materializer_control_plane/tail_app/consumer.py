"""The member's Kafka clients, and the plan that shares its one source consumer between tables.

**One source consumer per member.** Every active table's partitions are assigned to one aiokafka
consumer - `group_id=None`, nothing committed, the runtime's isolation level (`read_committed`: a
Kaspr app is transactional, so Faust reads committed records only) - so a member tailing hundreds
of tables keeps one set of broker connections and one fetch loop. Records are handed to tables by
topic and partition.

**Shared topics.** Two materializations may read one topic, and one member may own both tables.
A topic partition is then read once, from the lowest next offset of the tables that want it, and
each table takes the records at or past its own next offset. A table that is paused (at its bound,
its ingest ceiling, or the member's budget) does not want its partitions; one nobody wants is
paused on the consumer. When a paused table resumes behind the partition's position, the consumer
seeks back for it, and the other table skips what it has already. `plan_fetch` decides all of it;
the worker applies the plan (`worker.py`).

aiokafka forgets every partition's position and pause when the assignment changes, so a change of
the assigned set seeks every partition again: a hiccup of some hundred milliseconds for the member's
other tables.

**No topic named in vain.** A metadata request that names a topic makes a broker that creates
topics on demand create it if it is missing, so the consumers that only look things up never leave
a topic tracked (`kafka/metadata.py::track_no_topics`), and partitions are read from a metadata
request for every topic (`partitions_for`). The source consumer names the topics it reads, as
every consumer of them does.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from importlib import import_module
import logging
import os
from typing import Any, NamedTuple

from materializer_control_plane.kafka.metadata import track_no_topics

LOGGER = logging.getLogger(__name__)

#: Fetch sizes for the source consumer: a partition at a time is bounded, and a fetch as a whole.
MAX_PARTITION_FETCH_BYTES = 1024 * 1024
FETCH_MAX_BYTES = 32 * 1024 * 1024
MAX_POLL_RECORDS = 2_000
#: The source consumer's least fetch: the broker holds a fetch until this much is new or the fetch
#: wait (`live_tail_fetch_wait_ms`) has passed, so a member following many quiet tables wakes a few
#: times a second rather than for every record, each time paying for all of them; a busy table
#: fills it at once.
SOURCE_FETCH_MIN_BYTES = 64 * 1024
#: The isolation level the runtime reads with: Kaspr apps run Faust's exactly-once processing, which
#: reads committed records only (`faust/transport/drivers/aiokafka.py::_create_worker_consumer`).
ISOLATION_LEVEL = "read_committed"


class TP(NamedTuple):
    topic: str
    partition: int


# -- the plan ------------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Need:
    """One table's want of one topic partition: from where, and whether it wants records now."""

    tp: TP
    table: str
    next_offset: int
    wanting: bool = True


@dataclass(slots=True)
class FetchPlan:
    #: Every topic partition some table needs, wanting records or not.
    assignment: set[TP] = field(default_factory=set)
    #: Where each wanted partition must be read from: the lowest next offset of the tables wanting it.
    positions: dict[TP, int] = field(default_factory=dict)
    #: Partitions no table wants records of now.
    paused: set[TP] = field(default_factory=set)


def plan_fetch(needs: Iterable[Need]) -> FetchPlan:
    plan = FetchPlan()
    for need in needs:
        plan.assignment.add(need.tp)
        if not need.wanting:
            continue
        current = plan.positions.get(need.tp)
        plan.positions[need.tp] = need.next_offset if current is None else min(current, need.next_offset)
    plan.paused = {tp for tp in plan.assignment if tp not in plan.positions}
    return plan


# -- the clients ---------------------------------------------------------------------------------


class ConsumerAdapter:
    """An aiokafka consumer behind the calls the tail makes, in this module's `TP`s."""

    def __init__(self, consumer: Any) -> None:
        self.consumer = consumer
        self._tp = _topic_partition_type()

    async def start(self) -> None:
        await self.consumer.start()

    async def stop(self) -> None:
        await self.consumer.stop()

    def assign(self, tps: Iterable[TP]) -> None:
        self.consumer.assign([self._tp(tp.topic, tp.partition) for tp in tps])

    def seek(self, tp: TP, offset: int) -> None:
        self.consumer.seek(self._tp(tp.topic, tp.partition), offset)

    def pause(self, *tps: TP) -> None:
        if tps:
            self.consumer.pause(*[self._tp(tp.topic, tp.partition) for tp in tps])

    def resume(self, *tps: TP) -> None:
        if tps:
            self.consumer.resume(*[self._tp(tp.topic, tp.partition) for tp in tps])

    async def position(self, tp: TP) -> int:
        return int(await self.consumer.position(self._tp(tp.topic, tp.partition)))

    def head(self, tp: TP) -> int | None:
        """The offset past the newest record the consumer may read: the last stable offset when it
        reads committed records only and the broker reports one, the high watermark otherwise."""
        topic_partition = self._tp(tp.topic, tp.partition)
        try:
            stable = self.consumer.last_stable_offset(topic_partition)
        except (AttributeError, AssertionError):
            stable = None
        if stable is not None and stable >= 0:
            return int(stable)
        highwater = self.consumer.highwater(topic_partition)
        return None if highwater is None else int(highwater)

    def last_poll(self, tp: TP) -> int | None:
        """When a fetch response last came for the partition (epoch ms), records or not."""
        try:
            stamp = self.consumer.last_poll_timestamp(self._tp(tp.topic, tp.partition))
        except (AttributeError, AssertionError):
            return None
        return None if stamp is None else int(stamp)

    async def topics_partitions(self, topics: Iterable[str]) -> dict[str, list[int] | None]:
        """Each topic's partitions from one read of the cluster's metadata; None for a missing one."""
        cluster = await self.consumer._client.fetch_all_metadata()
        answer: dict[str, list[int] | None] = {}
        for topic in topics:
            partitions = cluster.partitions_for_topic(topic)
            answer[topic] = None if partitions is None else sorted(partitions)
        return answer

    async def getmany(self, *, timeout_ms: int, max_records: int) -> dict[TP, list[Any]]:
        batch = await self.consumer.getmany(timeout_ms=timeout_ms, max_records=max_records)
        return {TP(tp.topic, tp.partition): records for tp, records in batch.items()}

    async def beginning_offsets(self, tps: Iterable[TP]) -> dict[TP, int]:
        try:
            answer = await self.consumer.beginning_offsets([self._tp(tp.topic, tp.partition) for tp in tps])
        finally:
            self._track_no_topics()
        return {TP(tp.topic, tp.partition): int(offset) for tp, offset in answer.items()}

    async def end_offsets(self, tps: Iterable[TP]) -> dict[TP, int]:
        try:
            answer = await self.consumer.end_offsets([self._tp(tp.topic, tp.partition) for tp in tps])
        finally:
            self._track_no_topics()
        return {TP(tp.topic, tp.partition): int(offset) for tp, offset in answer.items()}

    def _track_no_topics(self) -> None:
        """After a lookup by a consumer that reads nothing, stop tracking the topics it looked up."""
        assignment = getattr(self.consumer, "assignment", None)
        if assignment is not None and assignment():
            return
        track_no_topics(self.consumer)

    async def partitions_for(self, topic: str) -> list[int] | None:
        """A topic's partitions from the cluster's metadata, never by subscribing: a broker that
        creates topics on demand must not create a missing one (`schema/discovery.py::_start`)."""
        cluster = await self.consumer._client.fetch_all_metadata()
        partitions = cluster.partitions_for_topic(topic)
        return None if partitions is None else sorted(partitions)

    def set_fetch_wait(self, fetch_wait_ms: int) -> None:
        """Apply a new fetch wait to the running consumer: aiokafka reads it for every request."""
        fetcher = getattr(self.consumer, "_fetcher", None)
        if fetcher is not None and hasattr(fetcher, "_fetch_max_wait_ms"):
            fetcher._fetch_max_wait_ms = int(fetch_wait_ms)


def kafka_settings(environ: Mapping[str, str] | None = None) -> dict[str, Any]:
    """The Kafka client settings of this app, from Kaspr's `K_KAFKA_*` (`kafka/metadata.py`)."""
    env = os.environ if environ is None else environ
    bootstrap = (env.get("K_KAFKA_BOOTSTRAP_SERVERS") or "").strip()
    if not bootstrap:
        raise RuntimeError("K_KAFKA_BOOTSTRAP_SERVERS is required for the tail's consumers")
    return {
        "bootstrap_servers": bootstrap,
        "security_protocol": (env.get("K_KAFKA_SECURITY_PROTOCOL") or "PLAINTEXT").strip(),
        "sasl_mechanism": (env.get("K_KAFKA_SASL_MECHANISM") or "PLAIN").strip(),
        "sasl_plain_username": env.get("K_KAFKA_AUTH_USERNAME") or None,
        "sasl_plain_password": env.get("K_KAFKA_AUTH_PASSWORD") or None,
    }


def build_consumer(*, fetch_wait_ms: int = 200, settings: Mapping[str, Any] | None = None, **overrides: Any) -> ConsumerAdapter:
    """A manually assigned, groupless consumer that reads as the runtime does. Call it on the loop."""
    try:
        consumer_type = import_module("aiokafka").AIOKafkaConsumer
    except (ImportError, AttributeError) as error:
        raise RuntimeError("aiokafka must be installed for the live tail") from error
    options: dict[str, Any] = {
        **(kafka_settings() if settings is None else dict(settings)),
        "group_id": None,
        "enable_auto_commit": False,
        "isolation_level": ISOLATION_LEVEL,
        "auto_offset_reset": "earliest",
        "fetch_max_wait_ms": int(fetch_wait_ms),
        "fetch_min_bytes": 1,
        "max_partition_fetch_bytes": MAX_PARTITION_FETCH_BYTES,
        "fetch_max_bytes": FETCH_MAX_BYTES,
        "max_poll_records": MAX_POLL_RECORDS,
        **overrides,
    }
    return ConsumerAdapter(consumer_type(**options))


def _topic_partition_type() -> Any:
    try:
        return import_module("aiokafka").TopicPartition
    except (ImportError, AttributeError):
        return TP
