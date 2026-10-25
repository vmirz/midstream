"""Cluster-wide topic catalog used to choose a materialization's source topic."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
import re
import time
from typing import Any

from materializer_control_plane.config import ControlPlaneConfig
from materializer_control_plane.kafka.metadata import TopicListing

TOPIC_CATEGORIES = ("kafka", "materializer", "stream")
# Bounds how long a request can wait on Kafka; the admin client's own timeout is 40 seconds.
LOAD_TIMEOUT_SECONDS = 10

# Suffixes stream processors give the topics they create to keep their own state.
STREAM_STATE_SUFFIXES = (
    "-changelog",  # Kafka Streams, Faust, and Kaspr table changelogs
    "-repartition",  # Kafka Streams and Faust repartitioning
    "-assignor-leader",  # Kaspr leader election
    "-__assignor-__leader",  # Faust leader election
    "-schedule-actions",  # Kaspr scheduler
    "-schedule-requests",
    "-schedule-rejections",
)


class TopicCatalogUnavailable(RuntimeError):
    """Raised when no catalog has ever loaded and Kafka cannot be asked right now."""


def classify_topic(listing: TopicListing, *, config: ControlPlaneConfig) -> str | None:
    """Label a topic that is not ordinary data, or return None for one that is.

    `kafka` is broker-internal, `materializer` belongs to this control plane or one of its
    runtimes, and `stream` is state another stream processor keeps in Kafka. A label only lets a
    client hide the topic by default; every topic stays selectable.
    """
    name = listing.name
    if listing.internal or name.startswith("__"):
        return "kafka"
    if name in _control_plane_topics(config) or _runtime_topic_pattern(config).match(name):
        return "materializer"
    if config.kaspr_app_name and name.startswith(f"{config.kaspr_app_name}-"):
        return "materializer"
    if name.endswith(STREAM_STATE_SUFFIXES):
        return "stream"
    return None


def topic_catalog_item(listing: TopicListing, *, config: ControlPlaneConfig) -> dict[str, Any]:
    """The wire form of one topic; `category` is omitted for ordinary data topics."""
    item: dict[str, Any] = {
        "topic": listing.name,
        "partitions": listing.partitions,
        "replication_factor": listing.replication_factor,
    }
    category = classify_topic(listing, config=config)
    if category is not None:
        item["category"] = category
    return item


@dataclass(frozen=True, slots=True)
class TopicCatalog:
    """One load of the cluster's topics, already in wire form."""

    items: tuple[dict[str, Any], ...]
    fetched_at: str
    loaded_at: float


class TopicCatalogCache:
    """Process-wide topic list shared by every request the control plane serves.

    Kafka answers for the whole cluster in one metadata request, so the cost worth bounding is how
    many of those requests clients can cause, whatever they do:

    - concurrent callers share one in-flight load;
    - a loaded catalog is reused for `topic_catalog_ttl_seconds`;
    - after any load attempt, forced or failed, Kafka is not asked again for
      `topic_catalog_min_refresh_seconds`; callers get the last catalog, with the last error if
      that attempt failed.

    During an outage that means a stale catalog is answered from memory immediately instead of
    every request waiting out a load that will fail.
    """

    def __init__(self, *, clock: Callable[[], float] = time.monotonic) -> None:
        self._clock = clock
        self._catalog: TopicCatalog | None = None
        self._loading: asyncio.Future[TopicCatalog] | None = None
        self._last_load_started: float | None = None
        self._last_error: str | None = None

    async def get(
        self,
        load: Callable[[], Awaitable[list[TopicListing]]],
        *,
        config: ControlPlaneConfig,
        force: bool = False,
    ) -> tuple[TopicCatalog, str | None]:
        """Return the catalog and, when the latest load failed, why it did."""
        now = self._clock()
        if self._loading is None:
            recently_attempted = (
                self._last_load_started is not None
                and now - self._last_load_started < config.topic_catalog_min_refresh_seconds
            )
            catalog = self._catalog
            if catalog is not None:
                fresh = now - catalog.loaded_at < config.topic_catalog_ttl_seconds
                if (fresh and not force) or recently_attempted:
                    return catalog, self._last_error
            elif recently_attempted and self._last_error is not None:
                raise TopicCatalogUnavailable(self._last_error)
            self._last_load_started = now
            self._loading = asyncio.ensure_future(self._load(load, config=config))
        try:
            # Shielded so a caller that disconnects mid-load does not cancel it for the others.
            return await asyncio.shield(self._loading), None
        except Exception:
            if self._catalog is None:
                raise
            return self._catalog, self._last_error

    async def _load(
        self,
        load: Callable[[], Awaitable[list[TopicListing]]],
        *,
        config: ControlPlaneConfig,
    ) -> TopicCatalog:
        try:
            try:
                listings = await asyncio.wait_for(load(), timeout=LOAD_TIMEOUT_SECONDS)
            except asyncio.TimeoutError:
                raise TimeoutError(f"Kafka did not answer within {LOAD_TIMEOUT_SECONDS}s") from None
            catalog = TopicCatalog(
                items=tuple(topic_catalog_item(listing, config=config) for listing in listings),
                fetched_at=datetime.now(tz=UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
                loaded_at=self._clock(),
            )
        except Exception as error:
            self._last_error = str(error) or type(error).__name__
            raise
        else:
            self._catalog = catalog
            self._last_error = None
            return catalog
        finally:
            self._loading = None


def _control_plane_topics(config: ControlPlaneConfig) -> frozenset[str]:
    return frozenset(
        {
            config.materialization_requests_topic,
            config.materialization_work_topic,
            config.materialization_status_topic,
            config.schema_authority_updates_topic,
            config.table_events_topic,
            config.settings_requests_topic,
            config.identity_requests_topic,
        }
    )


def _runtime_topic_pattern(config: ControlPlaneConfig) -> re.Pattern[str]:
    # Runtimes are named `<prefix>-mat-<12 hex>` and every topic they own starts with that name.
    return re.compile(rf"^{re.escape(config.default_runtime_name_prefix)}-mat-[0-9a-f]{{12}}-")
