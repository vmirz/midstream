"""Kafka metadata reads used by topic analysis, the topic list and consumer-group lag refresh.

All calls go through a single long-lived admin/consumer pair so repeated lookups reuse
warm connections and metadata, which keeps analyze latency low.

No request names a topic. A metadata request that names one makes a broker that creates topics on
demand (Kafka's default) create it if it is missing: describing a misspelt topic would make it, and
a deleted topic would come back, empty, at the next lookup. Topics are described from a read of
every topic, and the consumer that looks up offsets is kept from tracking the topics it looked up
(`track_no_topics`).

The pair lives on the client's own event loop, on the `AsyncRunner` thread. The control plane's
loop also serves the API and runs every agent, so callers there use the `*_async` methods, which
await the request instead of blocking on it. Every call waits at most `request_timeout_seconds`;
past that the request is cancelled on the client's loop and the caller gets `TimeoutError`.
"""

from __future__ import annotations

import asyncio
from collections.abc import Coroutine
from concurrent.futures import Future
from dataclasses import dataclass, field
from functools import lru_cache
import logging
import os
from importlib import import_module
from typing import Any, Protocol, TypeVar

from midstream.kafka.admin import AsyncRunner

LOGGER = logging.getLogger(__name__)

UNKNOWN_TOPIC_ERROR_CODES = frozenset({3, 17})
TOPIC_AUTHORIZATION_FAILED = 29
#: Seconds one call waits for Kafka before it is cancelled. aiokafka bounds each request at 40 s,
#: but a call can make several (describe, then both offset lookups) and a first call starts the
#: client too; this bounds the call as a whole.
DEFAULT_REQUEST_TIMEOUT_SECONDS = 30.0

T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class TopicMetadata:
    """Partition and offset facts for a single Kafka topic."""

    name: str
    exists: bool
    partitions: int = 0
    beginning_offsets: dict[int, int] = field(default_factory=dict)
    end_offsets: dict[int, int] = field(default_factory=dict)
    error: str | None = None

    @property
    def estimated_records(self) -> int | None:
        """Estimate retained records as the sum of per-partition offset spans.

        This over-counts when compaction or tombstones are in play and under-counts
        nothing, which is the right bias for capacity sizing.
        """
        if not self.exists or not self.end_offsets:
            return None
        total = 0
        for partition, end_offset in self.end_offsets.items():
            begin_offset = self.beginning_offsets.get(partition, 0)
            total += max(end_offset - begin_offset, 0)
        return total

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "topic": self.name,
            "exists": self.exists,
            "partitions": self.partitions,
        }
        if self.exists:
            payload["estimated_records"] = self.estimated_records
            payload["end_offsets"] = {str(k): v for k, v in sorted(self.end_offsets.items())}
        if self.error is not None:
            payload["error"] = self.error
        return payload


@dataclass(frozen=True, slots=True)
class TopicListing:
    """Name-level facts for one topic, taken from a single cluster-wide metadata request."""

    name: str
    partitions: int
    replication_factor: int
    #: Kafka's own flag, set for broker-internal topics such as `__consumer_offsets`.
    internal: bool = False


class KafkaMetadataClient(Protocol):
    def describe_topics(self, topics: list[str]) -> dict[str, TopicMetadata]: ...

    async def describe_topics_async(self, topics: list[str]) -> dict[str, TopicMetadata]: ...

    async def list_topics_async(self) -> list[TopicListing]: ...

    def consumer_group_offsets(self, group_id: str) -> dict[str, dict[int, int]]: ...

    async def consumer_group_offsets_async(self, group_id: str) -> dict[str, dict[int, int]]: ...


class AIOKafkaMetadataClient:
    """The aiokafka metadata and offset APIs, run on a loop of their own, each with a deadline.

    The `*_async` methods leave the caller's event loop free while Kafka answers; the others block
    their caller, and are for tests and tools.
    """

    def __init__(
        self,
        *,
        bootstrap_servers: str,
        security_protocol: str | None = None,
        sasl_mechanism: str | None = None,
        sasl_username: str | None = None,
        sasl_password: str | None = None,
        admin_factory: Any | None = None,
        consumer_factory: Any | None = None,
        request_timeout_seconds: float = DEFAULT_REQUEST_TIMEOUT_SECONDS,
    ) -> None:
        if not bootstrap_servers.strip():
            raise ValueError("bootstrap_servers is required for kafka metadata")
        if request_timeout_seconds <= 0:
            raise ValueError("request_timeout_seconds must be > 0")
        self.bootstrap_servers = bootstrap_servers.strip()
        self.security_protocol = None if security_protocol in (None, "") else security_protocol.strip()
        self.sasl_mechanism = None if sasl_mechanism in (None, "") else sasl_mechanism.strip()
        self.sasl_username = None if sasl_username in (None, "") else sasl_username
        self.sasl_password = None if sasl_password in (None, "") else sasl_password
        self.request_timeout_seconds = float(request_timeout_seconds)
        self._admin_factory = _build_admin_client if admin_factory is None else admin_factory
        self._consumer_factory = _build_consumer if consumer_factory is None else consumer_factory
        self._runner = AsyncRunner()
        self._admin: Any = None
        self._consumer: Any = None
        # Used only on the runner's loop: concurrent first calls share one start of each client.
        self._admin_lock = asyncio.Lock()
        self._consumer_lock = asyncio.Lock()
        # Clients whose start was cut short, being closed; held so the tasks are not collected.
        self._closing: set[asyncio.Task[None]] = set()

    @classmethod
    def from_environment(cls) -> "AIOKafkaMetadataClient":
        bootstrap_servers = os.getenv("K_KAFKA_BOOTSTRAP_SERVERS", "")
        if not bootstrap_servers.strip():
            raise RuntimeError("K_KAFKA_BOOTSTRAP_SERVERS is required for kafka metadata")
        return cls(
            bootstrap_servers=bootstrap_servers,
            security_protocol=os.getenv("K_KAFKA_SECURITY_PROTOCOL"),
            sasl_mechanism=os.getenv("K_KAFKA_SASL_MECHANISM"),
            sasl_username=os.getenv("K_KAFKA_AUTH_USERNAME"),
            sasl_password=os.getenv("K_KAFKA_AUTH_PASSWORD"),
        )

    def describe_topics(self, topics: list[str]) -> dict[str, TopicMetadata]:
        """Return partition counts and offset bounds for each requested topic, blocking the caller."""
        return self._wait(self._describe_topics(topics))

    async def describe_topics_async(self, topics: list[str]) -> dict[str, TopicMetadata]:
        """`describe_topics`, awaitable from another event loop without blocking it."""
        return await self._await(self._describe_topics(topics))

    def consumer_group_offsets(self, group_id: str) -> dict[str, dict[int, int]]:
        """Return committed offsets for a consumer group keyed by topic and partition."""
        return self._wait(self._consumer_group_offsets(group_id))

    async def consumer_group_offsets_async(self, group_id: str) -> dict[str, dict[int, int]]:
        """`consumer_group_offsets`, awaitable from another event loop without blocking it."""
        return await self._await(self._consumer_group_offsets(group_id))

    async def list_topics_async(self) -> list[TopicListing]:
        """Every topic in the cluster, sorted by name, from one metadata request.

        Awaitable from another event loop without blocking it: the request runs on the client's
        own loop, where its broker connections live.
        """
        return await self._await(self._list_topics())

    def _wait(self, coroutine: Coroutine[Any, Any, T]) -> T:
        """Run a request on the client's loop and block until it answers or the deadline passes."""
        future = self._runner.submit(coroutine)
        try:
            return future.result(timeout=self.request_timeout_seconds)
        except TimeoutError:
            # Cancelling the future cancels the request on the client's loop as well.
            if future.cancel():
                raise self._deadline_error() from None
            # It finished as the deadline passed, or raised `TimeoutError` itself.
            return future.result()

    async def _await(self, coroutine: Coroutine[Any, Any, T]) -> T:
        """Run a request on the client's loop and await it, at most until the deadline."""
        future: Future[T] = self._runner.submit(coroutine)
        try:
            # On timeout `wait_for` cancels the wrapper, which cancels `future` and with it the
            # request on the client's loop.
            return await asyncio.wait_for(asyncio.wrap_future(future), self.request_timeout_seconds)
        except TimeoutError:
            if future.done() and not future.cancelled():
                # It finished as the deadline passed, or raised `TimeoutError` itself.
                return future.result()
            raise self._deadline_error() from None

    def _deadline_error(self) -> TimeoutError:
        return TimeoutError(f"Kafka did not answer within {self.request_timeout_seconds:g}s")

    async def _list_topics(self) -> list[TopicListing]:
        admin = await self._get_admin()
        # `None` asks the broker for every topic; an empty list would ask for none.
        descriptions = await admin.describe_topics(None)
        listings = [
            listing
            for listing in (topic_listing_from_description(description) for description in descriptions)
            if listing is not None
        ]
        return sorted(listings, key=lambda listing: listing.name)

    async def _describe_topics(self, topics: list[str]) -> dict[str, TopicMetadata]:
        unique_topics = list(dict.fromkeys(topic for topic in topics if topic))
        if not unique_topics:
            return {}
        admin = await self._get_admin()
        # Every topic, then the ones asked for: naming them could create them (above). A topic this
        # client may not describe is left out of the answer, and so reads as missing.
        wanted = set(unique_topics)
        descriptions = [
            description for description in await admin.describe_topics(None) if description.get("topic") in wanted
        ]
        partitions_by_topic: dict[str, list[int]] = {}
        results: dict[str, TopicMetadata] = {}
        for description in descriptions:
            name = description.get("topic")
            if name is None:
                continue
            error_code = description.get("error_code", 0)
            if error_code:
                results[name] = TopicMetadata(
                    name=name,
                    exists=False,
                    error=(
                        "topic does not exist"
                        if error_code in UNKNOWN_TOPIC_ERROR_CODES
                        else f"kafka error code {error_code}"
                    ),
                )
                continue
            partition_ids = sorted(
                partition["partition"] for partition in description.get("partitions", [])
            )
            partitions_by_topic[name] = partition_ids
            results[name] = TopicMetadata(name=name, exists=True, partitions=len(partition_ids))

        for topic in unique_topics:
            if topic not in results:
                results[topic] = TopicMetadata(name=topic, exists=False, error="topic does not exist")

        if partitions_by_topic:
            bounds = await self._offset_bounds(partitions_by_topic)
            for name, (beginning, ending) in bounds.items():
                results[name] = TopicMetadata(
                    name=name,
                    exists=True,
                    partitions=results[name].partitions,
                    beginning_offsets=beginning,
                    end_offsets=ending,
                )
        return results

    async def _offset_bounds(
        self,
        partitions_by_topic: dict[str, list[int]],
    ) -> dict[str, tuple[dict[int, int], dict[int, int]]]:
        topic_partition_cls = _topic_partition_type()
        topic_partitions = [
            topic_partition_cls(topic, partition)
            for topic, partitions in partitions_by_topic.items()
            for partition in partitions
        ]
        if not topic_partitions:
            return {}
        consumer = await self._get_consumer()
        try:
            beginning, ending = await asyncio.gather(
                consumer.beginning_offsets(topic_partitions),
                consumer.end_offsets(topic_partitions),
            )
        finally:
            track_no_topics(consumer)
        bounds: dict[str, tuple[dict[int, int], dict[int, int]]] = {
            topic: ({}, {}) for topic in partitions_by_topic
        }
        for topic_partition, offset in beginning.items():
            bounds[topic_partition.topic][0][topic_partition.partition] = offset
        for topic_partition, offset in ending.items():
            bounds[topic_partition.topic][1][topic_partition.partition] = offset
        return bounds

    async def _consumer_group_offsets(self, group_id: str) -> dict[str, dict[int, int]]:
        admin = await self._get_admin()
        committed = await admin.list_consumer_group_offsets(group_id)
        offsets: dict[str, dict[int, int]] = {}
        for topic_partition, offset_metadata in (committed or {}).items():
            offset = getattr(offset_metadata, "offset", offset_metadata)
            if offset is None or offset < 0:
                continue
            offsets.setdefault(topic_partition.topic, {})[topic_partition.partition] = offset
        return offsets

    async def _get_admin(self) -> Any:
        if self._admin is not None:
            return self._admin
        async with self._admin_lock:
            if self._admin is None:
                admin = self._admin_factory(**self._client_kwargs())
                await self._start(admin, close="close")
                self._admin = admin
        return self._admin

    async def _get_consumer(self) -> Any:
        if self._consumer is not None:
            return self._consumer
        async with self._consumer_lock:
            if self._consumer is None:
                consumer = self._consumer_factory(
                    **self._client_kwargs(),
                    enable_auto_commit=False,
                )
                await self._start(consumer, close="stop")
                self._consumer = consumer
        return self._consumer

    async def _start(self, client: Any, *, close: str) -> None:
        """Start a Kafka client, and close it again if the start does not finish.

        A caller's deadline can cancel a start part-way, when the client may already hold broker
        connections and background tasks. It is closed in the background rather than leaked, and
        the next call starts a new one.
        """
        try:
            await client.start()
        except BaseException:
            closer = getattr(client, close, None)
            if closer is not None:
                task = asyncio.ensure_future(_close_quietly(closer))
                self._closing.add(task)
                task.add_done_callback(self._closing.discard)
            raise

    def _client_kwargs(self) -> dict[str, Any]:
        return {
            "bootstrap_servers": self.bootstrap_servers,
            "security_protocol": self.security_protocol or "PLAINTEXT",
            "sasl_mechanism": self.sasl_mechanism or "PLAIN",
            "sasl_plain_username": self.sasl_username,
            "sasl_plain_password": self.sasl_password,
        }


def track_no_topics(consumer: Any) -> None:
    """Have an aiokafka consumer's metadata refreshes ask for every topic again, naming none.

    Looking up the offsets of a partition its metadata does not know yet - a topic made since its
    last refresh - makes aiokafka track that topic, and every refresh from then on (every five
    minutes, and whenever a lookup misses a leader) names the topics it tracks. A broker that creates
    topics on demand then creates any of them deleted since: on the lab, 52 topics deleted after
    their materializations came back together, empty. Tracking none, the refreshes ask for every
    topic, which creates nothing. Only for a consumer with no assignment, which tracks its
    partitions' topics.
    """
    client = getattr(consumer, "_client", None)
    if client is not None and getattr(client, "_topics", None):
        client.set_topics([])


def topic_listing_from_description(description: dict[str, Any]) -> TopicListing | None:
    """Build a listing from one entry of a metadata response, or None for an unlisted topic.

    A topic this client may not describe is left out rather than shown without facts. Other errors
    (a leader election, say) keep the topic with whatever partitions the broker reported.
    """
    name = description.get("topic")
    if not name or description.get("error_code", 0) == TOPIC_AUTHORIZATION_FAILED:
        return None
    partitions = description.get("partitions") or []
    return TopicListing(
        name=name,
        partitions=len(partitions),
        replication_factor=max((len(partition.get("replicas") or []) for partition in partitions), default=0),
        internal=bool(description.get("is_internal", False)),
    )


@lru_cache(maxsize=1)
def get_metadata_client() -> AIOKafkaMetadataClient:
    """Return the process-wide metadata client so connections and metadata stay warm."""
    return AIOKafkaMetadataClient.from_environment()


async def _close_quietly(close: Any) -> None:
    try:
        await close()
    except Exception as exc:  # noqa: BLE001 - the client is being discarded either way
        LOGGER.debug("closing a part-started kafka client failed: %s", exc)


def _build_admin_client(**kwargs: Any) -> Any:
    try:
        return import_module("aiokafka.admin").AIOKafkaAdminClient(**kwargs)
    except (ImportError, AttributeError) as exc:
        raise RuntimeError("aiokafka must be installed for kafka metadata reads") from exc


def _build_consumer(**kwargs: Any) -> Any:
    try:
        return import_module("aiokafka").AIOKafkaConsumer(**kwargs)
    except (ImportError, AttributeError) as exc:
        raise RuntimeError("aiokafka must be installed for kafka metadata reads") from exc


def _topic_partition_type() -> Any:
    try:
        return import_module("aiokafka").TopicPartition
    except (ImportError, AttributeError) as exc:
        raise RuntimeError("aiokafka must be installed for kafka metadata reads") from exc
