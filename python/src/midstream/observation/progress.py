"""Monotonic aggregation of ephemeral runtime counters into cumulative totals.

Runtime members report process-local counters that reset to zero whenever a pod
restarts, and each member reports independently. This module folds those readings
into per-materialization cumulative totals using the last reading retained for every
member, so a restart contributes its new reading instead of a negative delta.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime
from typing import Any

from midstream.models.status import MemberProgress, RuntimeProgress, SkipReasonTotal

MEMBER_PROGRESS_LIMIT = 64
SKIP_REASON_LIMIT = 20
# Members silent for longer than this (several 30s heartbeats) are treated as gone: scaled
# away or replaced, and no longer part of what the materialization is doing.
MEMBER_STATE_FRESHNESS_SECONDS = 120

_COUNTER_FIELDS = (
    ("rows_written", "cumulative_rows_written"),
    ("bytes_written", "cumulative_bytes_written"),
    ("files_written", "cumulative_files_written"),
    ("records_skipped", "cumulative_records_skipped"),
    ("unmapped_values", "cumulative_unmapped_values"),
)


def merge_member_progress(
    current: RuntimeProgress,
    *,
    member_id: str,
    rows_written: int | None,
    bytes_written: int | None,
    files_written: int | None,
    records_skipped: int | None,
    latest_offsets: dict[str, dict[str, int]],
    reported_at: str | None = None,
    member_limit: int = MEMBER_PROGRESS_LIMIT,
    health: str | None = None,
    health_reason: str | None = None,
    phase_reason: str | None = None,
    phase_message: str | None = None,
    last_error: str | None = None,
    skip_reasons: list[dict[str, Any]] | None = None,
    settings_revision: int | None = None,
    settings_lineage: str | None = None,
    unmapped_values: int | None = None,
    files_pending_commit: int | None = None,
    tables: dict[str, dict[str, Any]] | None = None,
    table_unmapped: dict[str, int] | None = None,
    table_protected: dict[str, int] | None = None,
    table_withheld: dict[str, int] | None = None,
    protection_revisions: dict[str, int] | None = None,
    protection_key_id: str | None = None,
) -> RuntimeProgress:
    """Fold one member heartbeat into cumulative totals without losing prior progress.

    `tables` is the committer member's report of each table; `table_unmapped` this member's
    unmapped-value counter per table.
    """
    previous = current.member_progress.get(member_id, MemberProgress())
    reported_skips = {
        item["category"]: item
        for item in skip_reasons or []
        if isinstance(item.get("category"), str) and isinstance(item.get("count"), int)
    }
    observed = MemberProgress(
        rows_written=rows_written or 0,
        bytes_written=bytes_written or 0,
        files_written=files_written or 0,
        records_skipped=records_skipped or 0,
        reported_at=reported_at,
        health=health,
        health_reason=health_reason,
        phase_reason=phase_reason,
        phase_message=phase_message,
        last_error=last_error,
        unmapped_values=unmapped_values or 0,
        files_pending_commit=files_pending_commit,
        settings_revision=settings_revision,
        settings_lineage=settings_lineage,
        skip_counts=_member_skip_counts(previous, reported_skips),
        table_unmapped=dict(table_unmapped or {}),
        table_protected=dict(table_protected or {}),
        table_withheld=dict(table_withheld or {}),
        protection_revisions=dict(protection_revisions or {}),
        protection_key_id=protection_key_id,
    )

    cumulative: dict[str, int] = {}
    unmapped_last_seen_at = current.unmapped_last_seen_at
    for counter_name, cumulative_name in _COUNTER_FIELDS:
        observed_value = getattr(observed, counter_name)
        previous_value = getattr(previous, counter_name)
        # A reading below the previous one means the member restarted and its counter
        # reset, so the whole new reading is fresh progress.
        delta = observed_value - previous_value if observed_value >= previous_value else observed_value
        cumulative[cumulative_name] = (getattr(current, cumulative_name) or 0) + max(delta, 0)
        if counter_name == "unmapped_values" and delta > 0:
            unmapped_last_seen_at = reported_at or unmapped_last_seen_at

    unmapped_by_table = dict(current.cumulative_unmapped_by_table)
    unmapped_seen_by_table = dict(current.unmapped_last_seen_by_table)
    for table_name, observed_value in observed.table_unmapped.items():
        previous_value = previous.table_unmapped.get(table_name, 0)
        delta = observed_value - previous_value if observed_value >= previous_value else observed_value
        if delta > 0:
            unmapped_by_table[table_name] = unmapped_by_table.get(table_name, 0) + delta
            if reported_at:
                unmapped_seen_by_table[table_name] = reported_at

    protected_by_table = _fold_table_counters(current.cumulative_protected_by_table, previous.table_protected, observed.table_protected)
    withheld_by_table = _fold_table_counters(current.cumulative_withheld_by_table, previous.table_withheld, observed.table_withheld)

    member_progress = dict(current.member_progress)
    member_progress[member_id] = observed
    member_progress = _prune_member_progress(member_progress, limit=member_limit)

    merged_offsets = {table_name: dict(offsets) for table_name, offsets in current.latest_offsets.items()}
    for table_name, offsets in latest_offsets.items():
        merged_offsets.setdefault(table_name, {}).update(offsets)

    # Only the committer member reports the tables; keep each table's last block between its heartbeats.
    merged_tables = dict(current.tables)
    if tables:
        merged_tables.update({name: dict(block) for name, block in tables.items()})

    return replace(
        current,
        rows_written=rows_written,
        bytes_written=bytes_written,
        files_written=files_written,
        records_skipped=records_skipped,
        latest_offsets=merged_offsets,
        member_progress=member_progress,
        unmapped_last_seen_at=unmapped_last_seen_at,
        cumulative_unmapped_by_table=unmapped_by_table,
        unmapped_last_seen_by_table=unmapped_seen_by_table,
        cumulative_protected_by_table=protected_by_table,
        cumulative_withheld_by_table=withheld_by_table,
        tables=merged_tables,
        skip_reasons=_merge_skip_reasons(
            current.skip_reasons,
            previous=previous,
            reported=reported_skips,
            member_id=member_id,
        ),
        **cumulative,
    )


def _fold_table_counters(totals: dict[str, int], previous: dict[str, int], observed: dict[str, int]) -> dict[str, int]:
    """Per-table cumulative totals with one member's new reading folded in; a reading below the
    previous one is a restarted member's, all of it new."""
    folded = dict(totals)
    for table_name, observed_value in observed.items():
        previous_value = previous.get(table_name, 0)
        delta = observed_value - previous_value if observed_value >= previous_value else observed_value
        if delta > 0:
            folded[table_name] = folded.get(table_name, 0) + delta
    return folded


def _member_skip_counts(
    previous: MemberProgress,
    reported: dict[str, dict[str, Any]],
) -> dict[str, int]:
    counts = {category: item["count"] for category, item in reported.items()}
    # A category can drop out of a member's bounded report and come back; remembering its last
    # count keeps that return from being counted twice. Bounded so this cannot grow forever.
    for category, count in previous.skip_counts.items():
        if category not in counts and len(counts) < SKIP_REASON_LIMIT * 2:
            counts[category] = count
    return counts


def _merge_skip_reasons(
    totals: dict[str, SkipReasonTotal],
    *,
    previous: MemberProgress,
    reported: dict[str, dict[str, Any]],
    member_id: str,
) -> dict[str, SkipReasonTotal]:
    merged = dict(totals)
    for category, item in reported.items():
        prior = previous.skip_counts.get(category, 0)
        count = item["count"]
        delta = count - prior if count >= prior else count
        if delta <= 0:
            continue
        existing = merged.get(category)
        merged[category] = SkipReasonTotal(
            category=category,
            count=(0 if existing is None else existing.count) + delta,
            last_reason=item.get("last_reason") or category,
            last_seen_at=item.get("last_seen_at"),
            last_partition=item.get("last_partition"),
            last_offset=item.get("last_offset"),
            last_schema_version=item.get("last_schema_version"),
            last_member_id=member_id,
        )
    if len(merged) > SKIP_REASON_LIMIT:
        retained = sorted(merged.values(), key=lambda reason: reason.last_seen_at or "", reverse=True)
        merged = {reason.category: reason for reason in retained[:SKIP_REASON_LIMIT]}
    return merged


@dataclass(frozen=True, slots=True)
class MemberStateSummary:
    """What a materialization's runtime members are doing, as one status."""

    health: str
    health_reason: str | None
    phase_reason: str | None
    phase_message: str | None
    last_error: str | None
    member_count: int


_STATE_LABELS = {
    "degraded": "report errors",
    "waiting_for_table": "loading tables",
    "processing": "processing source records",
    "waiting_for_source_records": "idle, waiting for source records",
}


def summarize_member_states(
    member_progress: dict[str, MemberProgress],
    *,
    reference_time: str,
) -> MemberStateSummary | None:
    """Reduce every recently reporting member to a single, stable materialization status.

    Each member heartbeats on its own schedule. Taking status from whichever reported last made
    a 16-member runtime flip between "idle" and "processing" as different members spoke, which
    reads as instability when it is really a split. The worst state wins the reason; the message
    says how many members are in each state.
    """
    fresh = {
        member_id: progress
        for member_id, progress in member_progress.items()
        if progress.health is not None and _is_fresh(progress.reported_at, reference_time)
    }
    if not fresh:
        return None
    if len(fresh) == 1:
        (only,) = fresh.values()
        return MemberStateSummary(
            health=only.health or "unknown",
            health_reason=only.health_reason,
            phase_reason=only.phase_reason,
            phase_message=only.phase_message,
            last_error=only.last_error,
            member_count=1,
        )

    states: dict[str, list[tuple[str, MemberProgress]]] = {}
    for member_id, progress in fresh.items():
        state = "degraded" if progress.health == "degraded" else progress.phase_reason or "unknown"
        states.setdefault(state, []).append((member_id, progress))

    total = len(fresh)
    degraded = states.get("degraded", [])
    if degraded:
        latest_member, latest = max(degraded, key=lambda entry: entry[1].reported_at or "")
        reason = latest.health_reason
        if reason in {"data_validation", "infrastructure"}:
            phase_reason = f"runtime_{reason}_degraded"
        elif reason == "commits_stalled":
            phase_reason = reason
        else:
            phase_reason = "runtime_reported_degraded"
        return MemberStateSummary(
            health="degraded",
            health_reason=reason,
            phase_reason=phase_reason,
            phase_message=_split_message(states, total),
            last_error=latest.last_error,
            member_count=total,
        )

    # A runtime is still loading only while every member is; one working member means it works.
    phase_reason = next(
        (
            state
            for state in ("processing", "waiting_for_source_records", "waiting_for_table")
            if state in states
        ),
        next(iter(states)),
    )
    health = "healthy" if all(progress.health == "healthy" for progress in fresh.values()) else "unknown"
    return MemberStateSummary(
        health=health,
        health_reason=None,
        phase_reason=phase_reason,
        phase_message=_split_message(states, total),
        last_error=None,
        member_count=total,
    )


def _split_message(states: dict[str, list[tuple[str, MemberProgress]]], total: int) -> str:
    if len(states) == 1:
        (state,) = states
        return f"All {total} runtime members are {_STATE_LABELS.get(state, state.replace('_', ' '))}."
    order = list(_STATE_LABELS) + sorted(state for state in states if state not in _STATE_LABELS)
    parts: list[str] = []
    for state in order:
        members = states.get(state)
        if not members:
            continue
        label = _STATE_LABELS.get(state, state.replace("_", " "))
        parts.append(f"{len(members)} of {total} {label}")
    return f"Runtime members: {'; '.join(parts)}."


def _is_fresh(reported_at: str | None, reference_time: str) -> bool:
    reported = _parse_timestamp(reported_at)
    reference = _parse_timestamp(reference_time)
    if reported is None or reference is None:
        return False
    return (reference - reported).total_seconds() <= MEMBER_STATE_FRESHNESS_SECONDS


def _parse_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        return None


def _prune_member_progress(
    member_progress: dict[str, MemberProgress],
    *,
    limit: int,
) -> dict[str, MemberProgress]:
    """Bound the retained member map so replica churn cannot grow status unboundedly."""
    if len(member_progress) <= limit:
        return member_progress
    ordered = sorted(
        member_progress.items(),
        key=lambda item: (item[1].reported_at or "", item[0]),
        reverse=True,
    )
    return dict(ordered[:limit])
