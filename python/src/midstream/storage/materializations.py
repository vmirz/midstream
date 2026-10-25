"""Persistence helpers for materialization records and bounded listings."""

from __future__ import annotations

import logging

from collections.abc import Iterable, Iterator
from typing import Any

from midstream.config import normalize_list_limit
from midstream.models import Materialization, MaterializationStatus, RecordEnvelope
from midstream.models.materialization import LIFECYCLE_PHASES
from midstream.storage.envelopes import decode_envelope, encode_payload_envelope, read_payload
from midstream.storage.resolve import resolve_envelope
from midstream.storage.keys import (
    materialization_key,
    materialization_prefix,
    materialization_status_key,
)
from midstream.storage.table import TableProtocol

LOGGER = logging.getLogger(__name__)

ENTITY_TYPE = "materialization"


def save_materialization(
    table: TableProtocol,
    materialization: Materialization,
    *,
    version: int = 1,
) -> dict[str, Any]:
    """Persist a materialization record in the backing Kaspr table."""
    stored = encode_payload_envelope(
        entity_type=ENTITY_TYPE,
        entity_id=materialization.materialization_id,
        version=version,
        updated_at=materialization.updated_at,
        payload=materialization.to_dict(),
    )
    table[materialization_key(materialization.materialization_id)] = stored
    return stored


def get_materialization(table: TableProtocol, materialization_id: str) -> Materialization | None:
    """Load a materialization record by identifier, if one exists and this version can read it."""
    envelope = resolve_envelope(
        table,
        key=materialization_key(materialization_id),
        entity_type=ENTITY_TYPE,
        entity_id=materialization_id,
    )
    if envelope is None:
        return None
    return read_payload(envelope, Materialization.from_dict)


def delete_materialization(table: TableProtocol, materialization_id: str) -> None:
    """Delete a persisted materialization record."""
    del table[materialization_key(materialization_id)]


def list_materializations(
    table: TableProtocol,
    *,
    limit: str | None = None,
    status: str | Iterable[str] | None = None,
    query: str | None = None,
) -> dict[str, Any]:
    """List materializations with optional bounded size, status filter, search, and status fields.

    `query` keeps those whose label, id, topics, datasets or tables (`dataset.table`) contain it,
    without regard to case.
    """
    normalized_limit = normalize_list_limit(limit)
    normalized_status = normalize_status_filter(status)
    search = (query or "").strip().lower()
    max_items = None if normalized_limit == "all" else int(normalized_limit)
    items: list[dict[str, Any]] = []

    for materialization in _scan_materializations(table):
        if search and not matches_search(materialization, search):
            continue
        status_record = _get_materialization_status_for_listing(
            table, materialization.materialization_id
        )
        item = _build_list_item(materialization, status_record)
        if normalized_status is not None and item["observed_phase"] not in normalized_status:
            continue
        items.append(item)
        if max_items is not None and len(items) >= max_items:
            break

    result = {
        "items": items,
        "limit": normalized_limit,
        "status": "all" if normalized_status is None else ",".join(sorted(normalized_status)),
        "returned": len(items),
    }
    if search:
        result["q"] = search
    return result


def matches_search(materialization: Materialization, search: str) -> bool:
    """Whether a lower-case search term is in the materialization's label, id, a topic, a dataset or
    a table's `dataset.table`."""
    haystack = [materialization.label, materialization.materialization_id]
    for spec in materialization.all_tables:
        haystack.extend((spec.source_topic, spec.dataset, spec.name, spec.qualified_name))
    return any(search in value.lower() for value in haystack)


def iter_materializations(table: TableProtocol) -> Iterator[Materialization]:
    """Every materialization record, the newest copy of each, in scan order."""
    return _scan_materializations(table)


def _scan_materializations(table: TableProtocol) -> Iterator[Materialization]:
    """Every materialization record this version can read, once each, in scan order.

    The scan walks every shard, so a record stranded in more than one appears more than once. The
    newest write is the current state, so the scan keeps that copy and discards the rest —
    deterministic where picking whichever copy came first is not. Done inside this one scan
    rather than by re-reading each record, which would make the fleet list O(n) scans. A record
    an earlier, incompatible release left behind is skipped (`read_payload`).
    """
    newest: dict[str, RecordEnvelope] = {}
    order: list[str] = []
    for _, raw_value in table.prefix_scan(materialization_prefix()):
        envelope = decode_envelope(raw_value, expected_entity_type=ENTITY_TYPE)
        if envelope is None:
            continue
        entity_id = envelope.entity_id
        previous = newest.get(entity_id)
        if previous is None:
            order.append(entity_id)
            newest[entity_id] = envelope
        elif (envelope.updated_at or "") > (previous.updated_at or ""):
            newest[entity_id] = envelope
    for entity_id in order:
        materialization = read_payload(newest[entity_id], Materialization.from_dict)
        if materialization is not None:
            yield materialization


def normalize_status_filter(status: str | Iterable[str] | None) -> frozenset[str] | None:
    """Normalize a status filter into a phase set, or None when every phase is allowed."""
    if status is None:
        return None
    raw_values = status.split(",") if isinstance(status, str) else list(status)
    phases: set[str] = set()
    for raw_value in raw_values:
        phase = str(raw_value).strip().lower()
        if phase in ("", "all"):
            return None
        if phase not in LIFECYCLE_PHASES:
            allowed = ", ".join(LIFECYCLE_PHASES)
            raise ValueError(f"status must be 'all' or one of: {allowed}")
        phases.add(phase)
    return None if not phases else frozenset(phases)


def _get_materialization_status_for_listing(
    table: TableProtocol,
    materialization_id: str,
) -> MaterializationStatus | None:
    envelope = decode_envelope(
        table.get(materialization_status_key(materialization_id)),
        expected_entity_type="materialization-status",
    )
    if envelope is None:
        return None
    return MaterializationStatus.from_dict(envelope.payload)


# Lifecycle phases after which a materialization cannot be observed alive.
_DELETION_PHASES = frozenset({"deleting", "deleted"})


def observed_phase_for(
    materialization: Materialization,
    status: MaterializationStatus | None,
) -> tuple[str, str]:
    """Reconcile a status record against the materialization it belongs to.

    The materialization record is the authority on lifecycle: it is written by the request and
    workflow paths, while `observed_phase` is also written from runtime status events. A status
    that still says `running` for a materialization whose desired phase is `deleted` describes a
    runtime that has since been torn down, so it is reported as deleted rather than alive.

    Without this a deleted materialization can appear as Running in the fleet list — which is worse
    than merely stale, because it invites someone to act on something that no longer exists.
    """
    if status is None:
        return materialization.desired_phase, materialization.desired_phase
    if (
        materialization.desired_phase in _DELETION_PHASES
        and status.observed_phase not in _DELETION_PHASES
    ):
        return materialization.desired_phase, "runtime_removed"
    return status.observed_phase, status.phase_reason


def _build_list_item(
    materialization: Materialization,
    status: MaterializationStatus | None,
) -> dict[str, Any]:
    observed_phase, phase_reason = observed_phase_for(materialization, status)
    updated_at = materialization.updated_at if status is None else status.last_transition_time
    item: dict[str, Any] = {
        "materialization_id": materialization.materialization_id,
        "name": materialization.label,
        "source_topics": list(materialization.source_topics),
        # `dataset.table`, as SQL reads each: the written tables, in the order of the topics.
        "tables": [spec.qualified_name for spec in materialization.tables],
        "datasets": sorted({spec.dataset for spec in materialization.all_tables}),
        "destination_uri": materialization.destination_uri,
        "desired_phase": materialization.desired_phase,
        "observed_phase": observed_phase,
        "phase_reason": phase_reason,
        "storage_size": materialization.storage_size,
        "replicas": materialization.replicas,
        # Always true - every materialization has a live tail - and still sent: a 4.1 console offers Live only when it reads it.
        "live_tail": True,
        "updated_at": updated_at,
    }
    if status is not None and status.consumer_lag is not None:
        item["consumer_lag"] = status.consumer_lag.total_lag
    return item