"""Periodic consumer-group lag refresh for active materializations.

Lag is measured off the hot path on a cron cadence, per source topic of each materialization and
in all. Topic end offsets and consumer group offsets are fetched in batches shared across
materializations, so a fleet of N materializations costs one metadata round trip for every
distinct topic plus one committed offset lookup per distinct consumer group (a runtime's group
reads all of its materialization's topics).

A refresh runs on the `__system__` work event, so it never writes the state table: a write lands in
the shard of the event being handled, and a status saved there would be a second copy of the
record, stranded in a shard that nothing else writes and that no delete reaches. A refresh measures
and returns, for each materialization, a `record_consumer_lag` work event keyed by that
materialization, which `record_consumer_lag` saves in the materialization's own shard.

The maintenance agent runs `refresh_consumer_lag_async`, which awaits Kafka instead of blocking the
event loop that also serves the API — and so lets runtime heartbeats and observations write the same
status records while it waits. The status is therefore read again once Kafka has answered, and the
record step reads it a last time and changes only `consumer_lag`.

A refresh measures up to `consumer_lag_refresh_batch_limit` materializations, starting after the
last one the previous refresh measured (`SweepCursor`), so a fleet beyond the limit is measured in
turn rather than never.
"""

from __future__ import annotations

from dataclasses import replace
import logging

from materializer_control_plane.config import ControlPlaneConfig, get_config
from materializer_control_plane.kafka.metadata import KafkaMetadataClient, TopicMetadata
from materializer_control_plane.models import (
    ConsumerLag,
    EventCause,
    Materialization,
    MaterializationStatus,
    MaterializationWorkEvent,
    TopicLag,
)
from materializer_control_plane.protocol_logging import log_protocol_event
from materializer_control_plane.runtime.naming import (
    resolve_runtime_app_name,
    runtime_consumer_group_name,
)
from materializer_control_plane.services.sweep_cursor import SweepCursor
from materializer_control_plane.storage import (
    decode_envelope,
    get_materialization,
    get_materialization_status,
    materialization_prefix,
    read_payload,
    save_materialization_status,
)
from materializer_control_plane.storage.table import TableProtocol

LOGGER = logging.getLogger(__name__)

MATERIALIZATION_ENTITY_TYPE = "materialization"
# A stopped runtime consumes nothing, but its group keeps its offsets, so its lag still says how
# far behind it will start when resumed.
LAG_TRACKED_PHASES = frozenset({"running", "degraded", "paused", "stopped", "deploying"})

# A materialization whose lag is measured, its consumer group, and its status when collected.
_Target = tuple[Materialization, str, MaterializationStatus | None]


def refresh_consumer_lag(
    table: TableProtocol,
    *,
    metadata_client: KafkaMetadataClient,
    measured_at: str,
    config: ControlPlaneConfig | None = None,
    cursor: SweepCursor | None = None,
) -> list[MaterializationWorkEvent]:
    """Measure the consumer-group lag of active materializations; return the work that records it.

    Measures up to `consumer_lag_refresh_batch_limit` of them, starting after `cursor`, which it
    advances once they are measured; without one it starts at the first. Writes nothing: each
    returned `record_consumer_lag` event is keyed by its materialization, so it is recorded in that
    materialization's own shard. Blocks while Kafka answers. The maintenance agent runs
    `refresh_consumer_lag_async` instead.
    """
    cfg = get_config() if config is None else config
    cursor = SweepCursor() if cursor is None else cursor
    targets = _collect_targets(table, limit=cfg.consumer_lag_refresh_batch_limit, config=cfg, cursor=cursor)
    if not targets:
        return []

    topics = _source_topics(targets)
    topic_metadata = metadata_client.describe_topics(topics)
    group_offsets = {
        group_id: _safe_group_offsets(metadata_client, group_id)
        for group_id in _consumer_groups(targets)
    }
    return _measured(table, targets, topics, topic_metadata, group_offsets, measured_at=measured_at, config=cfg, cursor=cursor)


async def refresh_consumer_lag_async(
    table: TableProtocol,
    *,
    metadata_client: KafkaMetadataClient,
    measured_at: str,
    config: ControlPlaneConfig | None = None,
    cursor: SweepCursor | None = None,
) -> list[MaterializationWorkEvent]:
    """`refresh_consumer_lag` for the event loop that also serves the API and every agent.

    The table is read before Kafka is asked and again once it has answered; only the Kafka calls
    give up the loop, each bounded by the metadata client's deadline. A topic lookup that fails or
    times out raises, as in `refresh_consumer_lag`; a group lookup that does is treated as a group
    with no committed offsets.
    """
    cfg = get_config() if config is None else config
    cursor = SweepCursor() if cursor is None else cursor
    targets = _collect_targets(table, limit=cfg.consumer_lag_refresh_batch_limit, config=cfg, cursor=cursor)
    if not targets:
        return []

    topics = _source_topics(targets)
    topic_metadata = await metadata_client.describe_topics_async(topics)
    group_offsets: dict[str, dict[str, dict[int, int]]] = {}
    for group_id in _consumer_groups(targets):
        group_offsets[group_id] = await _safe_group_offsets_async(metadata_client, group_id)
    # From here on nothing awaits, so the statuses read below are the records as they are now.
    return _measured(table, targets, topics, topic_metadata, group_offsets, measured_at=measured_at, config=cfg, cursor=cursor)


def record_consumer_lag(table: TableProtocol, work_event: MaterializationWorkEvent) -> bool:
    """Save the lag a refresh measured onto its materialization's status; whether it was saved.

    Runs on the `record_consumer_lag` event the refresh keyed by the materialization, so the status
    is written in the materialization's own shard. The status is read as it is now and only
    `consumer_lag` replaced, which keeps a heartbeat or an observation written since the
    measurement. Nothing is saved for a materialization that is gone or whose lag is no longer
    tracked — saving after a purge would leave a record nothing collects — nor over a newer
    measurement.
    """
    consumer_lag = work_event.consumer_lag
    if consumer_lag is None:
        return False
    status = _tracked_status(table, work_event.materialization_id)
    if status is None:
        return False
    recorded = status.consumer_lag
    if recorded is not None and (recorded.measured_at or "") > (consumer_lag.measured_at or ""):
        return False
    save_materialization_status(table, replace(status, consumer_lag=consumer_lag))
    return True


def _collect_targets(
    table: TableProtocol,
    *,
    limit: int,
    config: ControlPlaneConfig,
    cursor: SweepCursor,
) -> list[_Target]:
    """Up to `limit` tracked materializations, in id order from just after the cursor.

    Reads every materialization record, since the table lists them partition by partition, but
    only the statuses of the materializations it considers.
    """
    candidates: list[Materialization] = []
    for _, raw_value in table.prefix_scan(materialization_prefix()):
        envelope = decode_envelope(raw_value, expected_entity_type=MATERIALIZATION_ENTITY_TYPE)
        if envelope is None:
            continue
        materialization = read_payload(envelope, Materialization.from_dict)
        if materialization is not None and materialization.desired_phase in LAG_TRACKED_PHASES:
            candidates.append(materialization)
    targets: list[_Target] = []
    for materialization in cursor.order(candidates, key=_materialization_id):
        status = get_materialization_status(table, materialization.materialization_id)
        if status is not None and status.observed_phase not in LAG_TRACKED_PHASES:
            continue
        runtime_name = resolve_runtime_app_name(materialization, config=config)
        targets.append((materialization, runtime_consumer_group_name(runtime_name), status))
        if len(targets) >= limit:
            break
    return targets


def _materialization_id(materialization: Materialization) -> str:
    return materialization.materialization_id


def _source_topics(targets: list[_Target]) -> list[str]:
    return sorted({topic for materialization, _, _ in targets for topic in materialization.source_topics})


def _consumer_groups(targets: list[_Target]) -> list[str]:
    """Groups to look up, once each, in target order. A target without a status gets no lag."""
    return list(dict.fromkeys(group_id for _, group_id, status in targets if status is not None))


def _measured(
    table: TableProtocol,
    targets: list[_Target],
    topics: list[str],
    topic_metadata: dict[str, TopicMetadata],
    group_offsets: dict[str, dict[str, dict[int, int]]],
    *,
    measured_at: str,
    config: ControlPlaneConfig,
    cursor: SweepCursor,
) -> list[MaterializationWorkEvent]:
    """Each target's lag, as the work that records it in the target's own shard.

    The status collected with the target may be stale: heartbeats and observations write it while
    Kafka is asked. So each is read again, the target is skipped when it is gone or no longer
    tracked, and the lag is measured against its heartbeat offsets as they are now.
    """
    records: list[MaterializationWorkEvent] = []
    for materialization, group_id, collected_status in targets:
        if collected_status is None:
            continue
        status = _tracked_status(table, materialization.materialization_id)
        if status is None:
            continue
        consumer_lag = _build_consumer_lag(
            materialization,
            topic_metadata=topic_metadata,
            committed_offsets=group_offsets[group_id],
            status=status,
            measured_at=measured_at,
        )
        if consumer_lag is None:
            continue
        records.append(_record_event(materialization.materialization_id, consumer_lag, measured_at=measured_at))
    cursor.advance(targets[-1][0].materialization_id)
    log_protocol_event(
        LOGGER,
        enabled=config.protocol_logging_enabled,
        event="control_plane_consumer_lag_refresh_completed",
        evaluated_count=len(targets),
        measured_count=len(records),
        topic_count=len(topics),
        measured_at=measured_at,
    )
    return records


def _record_event(materialization_id: str, consumer_lag: ConsumerLag, *, measured_at: str) -> MaterializationWorkEvent:
    return MaterializationWorkEvent(
        work_id=f"work-record-consumer-lag-{materialization_id}-{measured_at}",
        materialization_id=materialization_id,
        step="record_consumer_lag",
        requested_at=measured_at,
        caused_by=EventCause(kind="scheduler", id="refresh-consumer-lag"),
        dedupe_key=f"record_consumer_lag:{materialization_id}:{measured_at}",
        consumer_lag=consumer_lag,
    )


def _tracked_status(table: TableProtocol, materialization_id: str) -> MaterializationStatus | None:
    """The status as stored now, or None once the materialization no longer has its lag tracked."""
    materialization = get_materialization(table, materialization_id)
    if materialization is None or materialization.desired_phase not in LAG_TRACKED_PHASES:
        return None
    status = get_materialization_status(table, materialization_id)
    if status is None or status.observed_phase not in LAG_TRACKED_PHASES:
        return None
    return status


def _safe_group_offsets(
    metadata_client: KafkaMetadataClient,
    group_id: str,
) -> dict[str, dict[int, int]]:
    try:
        return metadata_client.consumer_group_offsets(group_id)
    except Exception as exc:  # noqa: BLE001 - a missing group must not abort the sweep
        LOGGER.debug("consumer group offsets unavailable for %s: %s", group_id, exc)
        return {}


async def _safe_group_offsets_async(
    metadata_client: KafkaMetadataClient,
    group_id: str,
) -> dict[str, dict[int, int]]:
    try:
        return await metadata_client.consumer_group_offsets_async(group_id)
    except Exception as exc:  # noqa: BLE001 - a missing group must not abort the sweep
        LOGGER.debug("consumer group offsets unavailable for %s: %s", group_id, exc)
        return {}


def _build_consumer_lag(
    materialization: Materialization,
    *,
    topic_metadata: dict[str, TopicMetadata],
    committed_offsets: dict[str, dict[int, int]],
    status: MaterializationStatus,
    measured_at: str,
) -> ConsumerLag | None:
    """The materialization's lag on each of its topics, and in all."""
    topics: dict[str, TopicLag] = {}
    errors: list[str] = []
    for spec in materialization.tables:
        lag = _build_topic_lag(
            metadata=topic_metadata.get(spec.source_topic),
            committed_offsets=committed_offsets.get(spec.source_topic, {}),
            heartbeat_offsets=status.runtime_progress.latest_offsets.get(spec.name, {}),
        )
        if lag is None:
            continue
        topics[spec.source_topic] = lag
        if lag.error is not None:
            errors.append(f"{spec.source_topic}: {lag.error}")
    if not topics:
        return None
    measured = [lag.total_lag for lag in topics.values() if lag.total_lag is not None]
    return ConsumerLag(
        total_lag=sum(measured) if measured else None,
        topics=topics,
        measured_at=measured_at,
        error="; ".join(errors) or None,
    )


def _build_topic_lag(
    *,
    metadata: TopicMetadata | None,
    committed_offsets: dict[int, int],
    heartbeat_offsets: dict[str, int],
) -> TopicLag | None:
    if metadata is None or not metadata.exists:
        return TopicLag(error=None if metadata is None else metadata.error or "topic does not exist")
    if not metadata.end_offsets:
        return None

    # Heartbeat offsets cover partitions the group has processed but not yet committed.
    partition_lag: dict[str, int] = {}
    total_lag = 0
    for partition, end_offset in sorted(metadata.end_offsets.items()):
        position = committed_offsets.get(partition)
        if position is None:
            reported = heartbeat_offsets.get(str(partition))
            position = reported if reported is not None else metadata.beginning_offsets.get(partition, 0)
        lag = max(end_offset - position, 0)
        partition_lag[str(partition)] = lag
        total_lag += lag

    return TopicLag(
        total_lag=total_lag,
        partition_lag=partition_lag,
        end_offsets={str(k): v for k, v in sorted(metadata.end_offsets.items())},
        partitions=metadata.partitions,
    )
