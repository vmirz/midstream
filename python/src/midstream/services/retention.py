"""Retention sweep that permanently removes long-deleted materialization records.

The sweep runs on the `__system__` work event, so it deletes nothing itself. A delete goes to the
changelog shard of the event being handled, while a materialization's records are in its own
shard: a purge run here would leave the changelog copies there, to come back whenever the store is
rebuilt from it. The sweep finds the deleted materializations past the retention window and returns,
for each, a `purge_materialization_records` work event keyed by that materialization, which the
workflow agent handles in the materialization's own shard — the step a deletion ends with when
`purge_records_on_delete` is on.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
import logging

from midstream.config import ControlPlaneConfig, get_config
from midstream.models import (
    EventCause,
    Materialization,
    MaterializationStatus,
    MaterializationWorkEvent,
    RecordEnvelope,
)
from midstream.storage import (
    decode_envelope,
    delete_materialization,
    delete_materialization_schemas,
    delete_materialization_status,
    delete_materialization_table,
    delete_operation_records,
    delete_runtime_autoscale,
    delete_runtime_binding,
    delete_schema_versions,
    get_materialization_status,
    list_materialization_tables,
    materialization_operation_prefix,
    materialization_prefix,
    read_payload,
)
from midstream.storage.operations import ENTITY_TYPE as OPERATION_ENTITY_TYPE
from midstream.storage.table import TableProtocol
from midstream.protocol_logging import log_protocol_event

LOGGER = logging.getLogger(__name__)

MATERIALIZATION_ENTITY_TYPE = "materialization"
PURGEABLE_PHASES = frozenset({"deleted"})


@dataclass(frozen=True, slots=True)
class PurgeCandidate:
    """A deleted materialization whose retention window has elapsed."""

    materialization_id: str
    deleted_at: str


def expired_materialization_purges(
    table: TableProtocol,
    *,
    now: str,
    config: ControlPlaneConfig | None = None,
) -> list[MaterializationWorkEvent]:
    """The purge of each deleted materialization past the retention window, as work for its own shard.

    Deletes nothing: each returned event is keyed by its materialization (see the module docstring).
    At most `deleted_purge_batch_limit` of them; the rest are found by a later sweep.
    """
    cfg = get_config() if config is None else config
    candidates = find_purge_candidates(
        table,
        now=now,
        retention_hours=cfg.deleted_retention_hours,
        limit=cfg.deleted_purge_batch_limit,
    )
    purges = [_purge_event(candidate.materialization_id, requested_at=now) for candidate in candidates]
    log_protocol_event(
        LOGGER,
        enabled=cfg.protocol_logging_enabled,
        event="control_plane_retention_purge_requested",
        requested_count=len(purges),
        retention_hours=cfg.deleted_retention_hours,
        requested_at=now,
    )
    return purges


def find_purge_candidates(
    table: TableProtocol,
    *,
    now: str,
    retention_hours: int,
    limit: int,
) -> list[PurgeCandidate]:
    """Return deleted materializations whose retention window has elapsed, once each.

    A record this version cannot read is a candidate too when it says it was deleted: an earlier,
    incompatible release left it behind, and nothing else will ever remove it.
    """
    cutoff = _parse_timestamp(now) - timedelta(hours=retention_hours)
    candidates: list[PurgeCandidate] = []
    seen: set[str] = set()
    for _, raw_value in table.prefix_scan(materialization_prefix()):
        envelope = decode_envelope(raw_value, expected_entity_type=MATERIALIZATION_ENTITY_TYPE)
        if envelope is None or envelope.entity_id in seen:
            continue
        materialization = read_payload(envelope, Materialization.from_dict)
        deleted_at = (
            _unreadable_deleted_at(envelope)
            if materialization is None
            else _readable_deleted_at(table, materialization)
        )
        if deleted_at is None or _parse_timestamp(deleted_at) > cutoff:
            continue
        if _destination_cleanup_active(table, envelope.entity_id, since=cutoff):
            continue
        seen.add(envelope.entity_id)
        candidates.append(PurgeCandidate(materialization_id=envelope.entity_id, deleted_at=deleted_at))
        if len(candidates) >= limit:
            break
    return candidates


def _readable_deleted_at(table: TableProtocol, materialization: Materialization) -> str | None:
    """When a deleted materialization finished deleting; None while it has not, or is not deleted."""
    if materialization.desired_phase not in PURGEABLE_PHASES:
        return None
    status = get_materialization_status(table, materialization.materialization_id)
    if status is not None and status.observed_phase not in PURGEABLE_PHASES:
        return None
    return _deleted_at(materialization, status)


def _unreadable_deleted_at(envelope: RecordEnvelope) -> str | None:
    """When a record this version cannot read was last written, if it says it was deleted."""
    if envelope.payload.get("desired_phase") not in PURGEABLE_PHASES:
        return None
    return envelope.updated_at or None


def _purge_event(materialization_id: str, *, requested_at: str) -> MaterializationWorkEvent:
    return MaterializationWorkEvent(
        work_id=f"work-retention-purge-{materialization_id}-{requested_at}",
        materialization_id=materialization_id,
        step="purge_materialization_records",
        requested_at=requested_at,
        caused_by=EventCause(kind="scheduler", id="purge-deleted-materializations"),
        dedupe_key=f"purge_materialization_records:{materialization_id}",
    )


def purge_materialization_records(table: TableProtocol, materialization_id: str) -> None:
    """Delete every control-plane record retained for a materialization, every table's included."""
    delete_schema_versions(table, materialization_id)
    delete_operation_records(table, materialization_id)
    delete_materialization_schemas(table, materialization_id)
    for record in list_materialization_tables(table, materialization_id):
        try:
            delete_materialization_table(table, materialization_id, record.name)
        except KeyError:
            continue
    _delete_quietly(delete_runtime_binding, table, materialization_id)
    _delete_quietly(delete_runtime_autoscale, table, materialization_id)
    _delete_quietly(delete_materialization_status, table, materialization_id)
    _delete_quietly(delete_materialization, table, materialization_id)


def _delete_quietly(deleter, table: TableProtocol, materialization_id: str) -> None:
    try:
        deleter(table, materialization_id)
    except KeyError:
        return


def _destination_cleanup_active(
    table: TableProtocol,
    materialization_id: str,
    *,
    since: datetime,
) -> bool:
    """Whether a table drop of this materialization has made progress after `since`.

    The drop reads the table's location from these records, and a large one outlasts the retention
    window; purging them mid-way would strand every object it had not reached. One that has stopped
    making progress is collected a retention window after its last pass.
    """
    for _, raw_value in table.prefix_scan(materialization_operation_prefix(materialization_id)):
        envelope = decode_envelope(raw_value, expected_entity_type=OPERATION_ENTITY_TYPE)
        if envelope is None:
            continue
        if (
            envelope.payload.get("operation_type") in ("drop_table", "cleanup_destination_objects")
            and envelope.payload.get("status") == "in_progress"
            and _parse_timestamp(envelope.updated_at) > since
        ):
            return True
    return False


def _deleted_at(
    materialization: Materialization,
    status: MaterializationStatus | None,
) -> str:
    if status is not None and status.last_transition_time:
        return status.last_transition_time
    return materialization.updated_at


def _parse_timestamp(value: str) -> datetime:
    normalized = value.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)
