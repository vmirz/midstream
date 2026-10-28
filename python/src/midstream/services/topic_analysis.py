"""Topic analysis used to validate source topics and recommend runtime scale."""

from __future__ import annotations

import asyncio
from typing import Any

from midstream.config import ControlPlaneConfig, get_config
from midstream.kafka.metadata import KafkaMetadataClient, TopicMetadata
from midstream.scale import ScaleSizingRules, recommend_combined_scale, recommend_scale

#: Seconds an analysis waits for Kafka, as long as the topic list does: someone is waiting on the
#: answer. The metadata client's own, longer deadline is for background work.
ANALYZE_TIMEOUT_SECONDS = 10


def analyze_topics(
    topics: list[str],
    *,
    metadata_client: KafkaMetadataClient,
    config: ControlPlaneConfig | None = None,
    sizing_rules: ScaleSizingRules | None = None,
) -> dict[str, Any]:
    """Validate topics and return their partitions and volume, and the replicas to read them.

    Every topic is resolved in a single batched metadata round trip so end-to-end
    latency stays flat as the request grows. Blocks while Kafka answers; the topics-analyze view
    runs `analyze_topics_async`.
    """
    cfg = get_config() if config is None else config
    rules = _sizing_rules(sizing_rules, config=cfg)
    requested = normalize_topic_list(topics, maximum=cfg.topic_analysis_max_topics)
    return _analysis(requested, metadata_client.describe_topics(requested), rules=rules)


async def analyze_topics_async(
    topics: list[str],
    *,
    metadata_client: KafkaMetadataClient,
    config: ControlPlaneConfig | None = None,
    sizing_rules: ScaleSizingRules | None = None,
) -> dict[str, Any]:
    """`analyze_topics` for the event loop that also serves the API.

    Invalid input raises `ValueError` before Kafka is asked. Kafka is awaited, for at most
    `ANALYZE_TIMEOUT_SECONDS`; past that the request is cancelled and `TimeoutError` raised.
    """
    cfg = get_config() if config is None else config
    rules = _sizing_rules(sizing_rules, config=cfg)
    requested = normalize_topic_list(topics, maximum=cfg.topic_analysis_max_topics)
    try:
        async with asyncio.timeout(ANALYZE_TIMEOUT_SECONDS) as deadline:
            described = await metadata_client.describe_topics_async(requested)
    except TimeoutError:
        if not deadline.expired():
            raise  # the metadata client's own deadline, which says so itself
        raise TimeoutError(f"Kafka did not answer within {ANALYZE_TIMEOUT_SECONDS}s") from None
    return _analysis(requested, described, rules=rules)


async def missing_topics_async(topics: list[str], *, metadata_client: KafkaMetadataClient) -> list[str]:
    """The topics, in the order given, that the cluster's metadata does not list.

    A topic is missing when it does not exist, and equally when the Kafka user may not describe it:
    the broker leaves such a topic out of a read of every topic, and naming it instead could create
    it (`kafka/metadata.py`). Kafka is awaited for at most `ANALYZE_TIMEOUT_SECONDS`; past that
    `TimeoutError`.
    """
    wanted = list(dict.fromkeys(topics))
    if not wanted:
        return []
    try:
        async with asyncio.timeout(ANALYZE_TIMEOUT_SECONDS) as deadline:
            listings = await metadata_client.list_topics_async()
    except TimeoutError:
        if not deadline.expired():
            raise  # the metadata client's own deadline, which says so itself
        raise TimeoutError(f"Kafka did not answer within {ANALYZE_TIMEOUT_SECONDS}s") from None
    listed = {listing.name for listing in listings}
    return [topic for topic in wanted if topic not in listed]


def missing_topics_message(topics: list[str]) -> str:
    """What it means that Kafka does not list these topics, naming them."""
    if len(topics) == 1:
        return f"Kafka has no topic {topics[0]}, or the deployment's Kafka user may not describe it"
    return f"Kafka has no topics {', '.join(topics)}, or the deployment's Kafka user may not describe them"


def normalize_topic_list(topics: Any, *, maximum: int) -> list[str]:
    """Validate and de-duplicate the requested topic list."""
    if isinstance(topics, str):
        candidates = topics.split(",")
    elif isinstance(topics, (list, tuple)):
        candidates = list(topics)
    else:
        raise ValueError("topics must be a list of topic names")
    normalized: list[str] = []
    for candidate in candidates:
        if not isinstance(candidate, str):
            raise ValueError("topics must be a list of topic names")
        name = candidate.strip()
        if not name:
            continue
        if name not in normalized:
            normalized.append(name)
    if not normalized:
        raise ValueError("at least one topic is required")
    if len(normalized) > maximum:
        raise ValueError(f"at most {maximum} topics can be analyzed per request")
    return normalized


def _sizing_rules(sizing_rules: ScaleSizingRules | None, *, config: ControlPlaneConfig) -> ScaleSizingRules:
    return ScaleSizingRules(max_replicas=config.max_runtime_replicas) if sizing_rules is None else sizing_rules


def _analysis(
    requested: list[str],
    described: dict[str, TopicMetadata],
    *,
    rules: ScaleSizingRules,
) -> dict[str, Any]:
    metadata = [
        described.get(topic) or TopicMetadata(name=topic, exists=False, error="topic does not exist")
        for topic in requested
    ]
    items = [_build_topic_analysis(item, rules=rules) for item in metadata]
    result: dict[str, Any] = {
        "items": items,
        "requested": len(requested),
        "valid": sum(1 for item in items if item["exists"]),
    }
    existing = [item for item in metadata if item.exists]
    if len(existing) > 1:
        # One materialization reads them all with one runtime: what it needs for all of them.
        result["combined"] = recommend_combined_scale([item.partitions for item in existing], rules=rules).to_dict()
    return result


def _build_topic_analysis(
    metadata: TopicMetadata,
    *,
    rules: ScaleSizingRules,
) -> dict[str, Any]:
    payload = metadata.to_dict()
    if not metadata.exists:
        return payload
    payload["recommendation"] = recommend_scale(partitions=metadata.partitions, rules=rules).to_dict()
    return payload
