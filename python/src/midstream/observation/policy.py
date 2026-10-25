"""Observation backoff and staleness policy helpers."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from midstream.models import ObservationPolicy

DEFAULT_OBSERVATION_BACKOFF_SECONDS = (15, 45, 120, 300)
DEFAULT_HEARTBEAT_STALE_SECONDS = 120
DEFAULT_RESOURCE_OBSERVATION_STALE_SECONDS = 600
DEFAULT_DESIRED_OBSERVED_MISMATCH_STALE_SECONDS = 600


def observation_backoff_seconds(attempt: int) -> int:
    if attempt < 1:
        raise ValueError("attempt must be >= 1")
    if attempt <= len(DEFAULT_OBSERVATION_BACKOFF_SECONDS):
        return DEFAULT_OBSERVATION_BACKOFF_SECONDS[attempt - 1]
    return DEFAULT_OBSERVATION_BACKOFF_SECONDS[-1]


def build_one_shot_observation_policy(*, attempt: int, requested_at: str) -> ObservationPolicy:
    backoff = observation_backoff_seconds(attempt)
    scheduled_for = _parse_timestamp(requested_at) + timedelta(seconds=backoff)
    return ObservationPolicy(
        attempt=attempt,
        max_attempts=len(DEFAULT_OBSERVATION_BACKOFF_SECONDS),
        scheduled_for=_format_timestamp(scheduled_for),
        mode="one_shot",
    )


def heartbeat_is_stale(last_heartbeat: str | None, *, now: str) -> bool:
    return _is_stale(last_heartbeat, now=now, stale_after_seconds=DEFAULT_HEARTBEAT_STALE_SECONDS)


def resource_observation_is_stale(last_observation: str | None, *, now: str) -> bool:
    return _is_stale(
        last_observation,
        now=now,
        stale_after_seconds=DEFAULT_RESOURCE_OBSERVATION_STALE_SECONDS,
    )


def desired_observed_mismatch_is_stale(
    *,
    desired_phase: str,
    observed_phase: str,
    last_transition_time: str,
    now: str,
) -> bool:
    if desired_phase == observed_phase:
        return False
    return _is_stale(
        last_transition_time,
        now=now,
        stale_after_seconds=DEFAULT_DESIRED_OBSERVED_MISMATCH_STALE_SECONDS,
    )


def _is_stale(timestamp: str | None, *, now: str, stale_after_seconds: int) -> bool:
    if timestamp is None:
        return True
    return (_parse_timestamp(now) - _parse_timestamp(timestamp)).total_seconds() > stale_after_seconds


def _parse_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def _format_timestamp(value: datetime) -> str:
    return value.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")