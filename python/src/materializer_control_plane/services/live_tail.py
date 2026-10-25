"""What the tail app is told about each table, every minute (`models/live_tail.py`).

The tail app serves the newest records of a materialization's tables from Kafka before they are
committed (handbook 19). It keeps nothing of its own, so the control plane names every table each
minute, from the same scheduled step that ticks the maintenance app (`handlers/maintenance.py`):

- a `tail_tick` for every table its runtime writes (`active`) of a materialization that is running:
  where the table is, its topic, the live-tail settings as resolved this minute, and what the
  table's record knows of it - committed offsets, schema id, metadata location;
- a `tail_cancel` for every other table still in the catalog: one being dropped, one kept read-only
  after its topic was removed (whose records past its committed offsets will never be committed), or
  one of a materialization being deleted or stopped. The app stops its tail at once rather than when
  the ticks have been missing for its forget window.

Every materialization has a live tail. With `live_tail_enabled` off nothing is sent at all: the
console asks for no tail, and the app forgets every table within its forget window. The step only
reads the state table.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

from materializer_control_plane.config import ControlPlaneConfig, get_config
from materializer_control_plane.models import (
    LiveTailHints,
    LiveTailMessage,
    LiveTailTarget,
    Materialization,
    MaterializationTable,
)
from materializer_control_plane.storage import iter_materializations, list_materialization_tables
from materializer_control_plane.storage.table import TableProtocol

#: The control-plane settings that steer the tail app, which every tick carries.
LIVE_TAIL_SETTING_FIELDS = (
    "live_tail_max_rows_per_table",
    "live_tail_max_bytes_per_table",
    "live_tail_retention_seconds",
    "live_tail_fetch_wait_ms",
    "live_tail_max_records_per_second_per_table",
    "live_tail_peek_max_rows",
    "live_tail_idle_seconds",
    "protocol_logging_enabled",
)

_DELETION_PHASES = frozenset({"deleting", "deleted"})


def live_tail_settings(config: ControlPlaneConfig) -> dict[str, Any]:
    """The live-tail settings of `config`, as a tick carries them."""
    return {name: getattr(config, name) for name in LIVE_TAIL_SETTING_FIELDS}


def live_tail_target(materialization: Materialization, record: MaterializationTable) -> LiveTailTarget:
    return LiveTailTarget(
        materialization_id=record.materialization_id,
        dataset=record.dataset,
        dataset_id=record.dataset_id,
        table=record.name,
        location=record.location,
        source_topic=record.source_topic,
        partition_granularity=record.partition_granularity,
        table_uuid=record.table_uuid,
        skip_tombstones=materialization.skip_tombstones,
    )


def live_tail_hints(record: MaterializationTable) -> LiveTailHints:
    """What the table's record knows: never ahead of the table, at most an event behind it."""
    return LiveTailHints(
        offsets={str(partition): int(offset) for partition, offset in record.offsets.items()},
        schema_id=record.schema_id,
        metadata_location=record.metadata_location,
    )


def build_live_tail_messages(
    table: TableProtocol,
    *,
    config: ControlPlaneConfig | None = None,
    issued_at: str,
) -> list[LiveTailMessage]:
    """This minute's tick for every tailed table, and a cancel for every other table in the catalog.

    Reads only: the state table is global, so this sees every materialization from any shard.
    """
    cfg = get_config() if config is None else config
    if not cfg.live_tail_enabled:
        return []
    settings = live_tail_settings(cfg)
    messages: list[LiveTailMessage] = []
    for materialization in iter_materializations(table):
        for record in list_materialization_tables(table, materialization.materialization_id):
            messages.extend(_messages_for(materialization, record, settings=settings, issued_at=issued_at))
    return messages


def cancel_reason(materialization: Materialization, record: MaterializationTable) -> str | None:
    """Why a table in the catalog is not tailed, or None when it is."""
    if materialization.desired_phase in _DELETION_PHASES:
        return "materialization_deleting"
    if record.state == "dropping":
        return "table_dropping"
    if record.state != "active":
        return "table_retired"
    if materialization.desired_phase != "running":
        return "materialization_stopped"
    return None


def _messages_for(
    materialization: Materialization,
    record: MaterializationTable,
    *,
    settings: dict[str, Any],
    issued_at: str,
) -> Iterator[LiveTailMessage]:
    if record.state == "dropped":
        # Out of the catalog for good: it was cancelled while it was dropped.
        return
    target = live_tail_target(materialization, record)
    reason = cancel_reason(materialization, record)
    if reason is not None:
        yield LiveTailMessage(kind="tail_cancel", target=target, issued_at=issued_at, reason=reason)
        return
    yield LiveTailMessage(
        kind="tail_tick",
        target=target,
        issued_at=issued_at,
        settings=settings,
        hints=live_tail_hints(record),
    )


def live_tail_output(message: LiveTailMessage) -> dict[str, Any]:
    """`message` routed to the tail app's topic, keyed by its table."""
    return {"route": "tail", "key": message.key, "value": message.to_dict()}
