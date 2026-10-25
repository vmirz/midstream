"""WebView-facing Kafka topic handlers: the cluster's topic list and per-topic analysis."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from materializer_control_plane.config import ControlPlaneConfig
from materializer_control_plane.kafka.metadata import KafkaMetadataClient
from materializer_control_plane.services.topic_analysis import (
    analyze_topics,
    analyze_topics_async,
    normalize_topic_list,
)
from materializer_control_plane.services.topic_catalog import TopicCatalogCache
from materializer_control_plane.settings import resolve_config
from materializer_control_plane.storage.table import TableProtocol
from materializer_control_plane.web.errors import WebError, invalid_request, unavailable

# One per process, so every WebView request shares the same catalog and in-flight load.
_TOPIC_CATALOG = TopicCatalogCache()


def analyze_topics_view(
    body: dict[str, Any] | None,
    *,
    metadata_client: KafkaMetadataClient,
    settings_table: TableProtocol | None = None,
    config: ControlPlaneConfig | None = None,
) -> dict[str, Any]:
    """Validate the analyze request body and return per-topic analysis results.

    Blocks while Kafka answers; the topics-analyze view runs `analyze_topics_view_async`. Both
    answer a bad body with `400 invalid_request` and a Kafka failure with `503 unavailable`.
    """
    cfg = resolve_config(settings_table) if config is None else config
    topics = _requested_topics(body, config=cfg)
    try:
        return analyze_topics(topics, metadata_client=metadata_client, config=cfg)
    except Exception as cause:
        raise _analysis_unavailable(cause) from cause


async def analyze_topics_view_async(
    body: dict[str, Any] | None,
    *,
    metadata_client: KafkaMetadataClient,
    settings_table: TableProtocol | None = None,
    config: ControlPlaneConfig | None = None,
) -> dict[str, Any]:
    """`analyze_topics_view` for the topics-analyze view, which shares the API's event loop.

    Kafka is awaited, so the loop serves other requests meanwhile, and for at most
    `ANALYZE_TIMEOUT_SECONDS`: an analysis Kafka does not answer in time is a `503`, as for
    `GET /topics`.
    """
    cfg = resolve_config(settings_table) if config is None else config
    topics = _requested_topics(body, config=cfg)
    try:
        return await analyze_topics_async(topics, metadata_client=metadata_client, config=cfg)
    except Exception as cause:
        raise _analysis_unavailable(cause) from cause


def _requested_topics(body: dict[str, Any] | None, *, config: ControlPlaneConfig) -> list[str]:
    """The request's topics, validated before Kafka is asked anything."""
    if not isinstance(body, dict):
        raise invalid_request("request body must be an object")
    topics = body.get("topics")
    if topics is None:
        topics = body.get("topic")
    if topics is None:
        raise invalid_request("topics is required", field_name="topics")
    try:
        return normalize_topic_list(topics, maximum=config.topic_analysis_max_topics)
    except ValueError as error:
        raise invalid_request(str(error), field_name="topics") from error


def _analysis_unavailable(cause: Exception) -> WebError:
    return unavailable(
        "Kafka topics cannot be analyzed right now",
        reason=str(cause) or type(cause).__name__,
    )


async def list_topics_view(
    query_params: Mapping[str, str] | None,
    *,
    metadata_client: KafkaMetadataClient,
    settings_table: TableProtocol | None = None,
    config: ControlPlaneConfig | None = None,
    cache: TopicCatalogCache | None = None,
) -> dict[str, Any]:
    """Return every topic in the cluster, sorted by name, for a topic picker.

    `?refresh=true` asks for a reload instead of the cached catalog; the cache still spaces
    reloads, so it cannot be used to put load on Kafka. When a reload fails but an earlier catalog
    exists, that catalog is returned with `stale` and the `error` that kept it from refreshing.
    """
    cfg = resolve_config(settings_table) if config is None else config
    force = str((query_params or {}).get("refresh", "")).strip().lower() in {"1", "true", "yes"}
    try:
        catalog, error = await (cache or _TOPIC_CATALOG).get(
            metadata_client.list_topics_async,
            config=cfg,
            force=force,
        )
    except Exception as cause:
        raise unavailable(
            "Kafka topics cannot be listed right now",
            reason=str(cause) or type(cause).__name__,
        ) from cause
    payload: dict[str, Any] = {
        "items": list(catalog.items),
        "returned": len(catalog.items),
        "fetched_at": catalog.fetched_at,
    }
    if error is not None:
        payload["stale"] = True
        payload["error"] = error
    return payload
