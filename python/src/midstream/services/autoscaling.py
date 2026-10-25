"""Runtime autoscaling: more members for a runtime that is far behind its topics, its own count after.

The control plane measures every materialization's consumer lag once a minute
(`services/consumer_lag.py`). Each measurement is recorded in the materialization's own shard, and
there, after it, `autoscale_after_lag` decides whether its runtime should run more members than the
materialization's own `replicas`:

- **More members** when the runtime is more than `runtime_autoscale_min_lag_records` behind and, at
  the pace it drains, would take longer than `runtime_autoscale_target_minutes` to catch up - or does
  not drain at all. It gets the members that pace says it needs to make the target (the drain shared
  evenly between members), at least one more and at most `runtime_autoscale_max_replicas`, and never
  more than its topics' partitions keep busy. Only a runtime observed running and healthy gets more:
  members added to one that keeps restarting would only restart too. The pace is measured over the
  last ten minutes, from the last change on: `MIN_POINTS` measurements must show it first.
- **Its own count again** once it has been under the lag floor for `CALM_POINTS` measurements in a
  row, or when autoscaling is turned off - for every materialization in the settings, or for this
  one - at once.
- Never two changes within `runtime_autoscale_cooldown_seconds`: each change moves the runtime's
  partitions between its members, and the new pace needs time to show.

The scaled count is the control plane's own record (`runtime-autoscale:<id>`), not the
materialization's `replicas`, which stays what the person set. Every render of the runtime's spec
reads it (`scaled_materialization`), so the reconcile sweep keeps a scaled runtime scaled. A stop
forgets it, so a resumed runtime starts at its own count. Kubernetes' horizontal autoscaler plays no
part; the vertical one keeps sizing each member's CPU as before.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import UTC, datetime
import logging
import math

from midstream.config import ControlPlaneConfig, get_config
from midstream.models import (
    LAG_POINTS_KEPT,
    EventCause,
    LagPoint,
    Materialization,
    MaterializationStatus,
    MaterializationWorkEvent,
    RuntimeAutoscale,
    TargetRuntime,
)
from midstream.protocol_logging import log_protocol_event
from midstream.runtime.naming import resolve_runtime_app_name
from midstream.scale import assignable_members
from midstream.storage import (
    delete_runtime_autoscale,
    get_materialization,
    get_materialization_status,
    get_runtime_autoscale,
    save_runtime_autoscale,
)
from midstream.storage.table import TableProtocol

LOGGER = logging.getLogger(__name__)

#: Measurements at the present count before members are added: the pace they show.
MIN_POINTS = 3
#: Measurements in a row under the lag floor that make a scaled runtime caught up.
CALM_POINTS = 3
#: How far back the pace is measured.
PACE_WINDOW_SECONDS = 600


@dataclass(frozen=True, slots=True)
class AutoscalePolicy:
    """The deployment's autoscaling settings, as one decision reads them."""

    enabled: bool
    max_replicas: int
    target_seconds: int
    min_lag: int
    cooldown_seconds: int

    @classmethod
    def from_config(cls, config: ControlPlaneConfig) -> "AutoscalePolicy":
        return cls(
            enabled=config.runtime_autoscaling_enabled,
            max_replicas=config.runtime_autoscale_max_replicas,
            target_seconds=config.runtime_autoscale_target_minutes * 60,
            min_lag=config.runtime_autoscale_min_lag_records,
            cooldown_seconds=config.runtime_autoscale_cooldown_seconds,
        )


@dataclass(frozen=True, slots=True)
class AutoscalePlan:
    """The autoscaling record after one measurement, and whether the runtime's count changed."""

    autoscale: RuntimeAutoscale
    changed: bool


def plan_autoscale(
    materialization: Materialization,
    autoscale: RuntimeAutoscale | None,
    *,
    lag: int,
    measured_at: str,
    capacity: int | None,
    healthy: bool,
    policy: AutoscalePolicy,
) -> AutoscalePlan:
    """What one lag measurement makes of a materialization's autoscaling.

    `capacity` is the members its topics' partitions keep busy (None when not known); `healthy`
    whether its runtime is observed running and healthy.
    """
    record = autoscale or RuntimeAutoscale(materialization_id=materialization.materialization_id)
    if record.points and measured_at <= record.points[-1].at:
        # A measurement seen already - delivered again - or an older one: nothing new to decide from.
        return AutoscalePlan(record, changed=False)
    points = (*record.points, LagPoint(at=measured_at, lag=lag))[-LAG_POINTS_KEPT:]
    own = materialization.replicas
    # A count at or under its own - the person raised it since - is no scaling at all.
    current = record.replicas if record.replicas is not None and record.replicas > own else own
    scaled = current > own
    pace = _pace(points, since=record.changed_at, now=measured_at)
    eta = None if pace is None or pace <= 0 else int(lag / pace)
    kept = replace(
        record,
        replicas=current if scaled else None,
        reason=record.reason if scaled else None,
        points=points,
        eta_seconds=eta,
        updated_at=measured_at,
    )

    def back_to_own() -> AutoscalePlan:
        return AutoscalePlan(replace(kept, replicas=None, reason=None, changed_at=measured_at), changed=True)

    if not (policy.enabled and materialization.autoscaling and materialization.desired_phase == "running"):
        return back_to_own() if scaled else AutoscalePlan(kept, changed=False)
    if scaled and len(points) >= CALM_POINTS and all(point.lag <= policy.min_lag for point in points[-CALM_POINTS:]):
        return back_to_own()

    cap = max(own, min(policy.max_replicas, capacity if capacity else policy.max_replicas))
    if lag <= policy.min_lag or current >= cap or not healthy:
        return AutoscalePlan(kept, changed=False)
    if record.changed_at is not None and _seconds(record.changed_at, measured_at) < policy.cooldown_seconds:
        return AutoscalePlan(kept, changed=False)
    if len(_since(points, record.changed_at, measured_at)) < MIN_POINTS:
        return AutoscalePlan(kept, changed=False)
    if eta is not None and eta <= policy.target_seconds:
        return AutoscalePlan(kept, changed=False)
    # The drain shared evenly between members: as many as make the target at this pace, or twice as
    # many while there is no drain to share.
    wanted = current * 2 if eta is None else math.ceil(current * eta / policy.target_seconds)
    replicas = min(cap, max(current + 1, wanted))
    return AutoscalePlan(
        replace(kept, replicas=replicas, reason=_reason(lag, current, eta), changed_at=measured_at),
        changed=True,
    )


def scaled_materialization(materialization: Materialization, autoscale: RuntimeAutoscale | None) -> Materialization:
    """The materialization as its runtime's spec is rendered: at the count autoscaling gave it, if more."""
    if (
        autoscale is None
        or autoscale.replicas is None
        or not materialization.autoscaling
        or autoscale.replicas <= materialization.replicas
    ):
        return materialization
    return replace(materialization, replicas=autoscale.replicas)


def autoscale_after_lag(
    table: TableProtocol,
    work_event: MaterializationWorkEvent,
    *,
    config: ControlPlaneConfig | None = None,
) -> list[MaterializationWorkEvent]:
    """Decide a materialization's runtime count from the lag just recorded; the apply that changes it.

    Runs on the `record_consumer_lag` event, keyed by the materialization, so its record is written in
    the materialization's own shard.
    """
    cfg = get_config() if config is None else config
    consumer_lag = work_event.consumer_lag
    if consumer_lag is None or consumer_lag.total_lag is None or consumer_lag.measured_at is None:
        return []
    materialization = get_materialization(table, work_event.materialization_id)
    if materialization is None:
        return []
    record = get_runtime_autoscale(table, materialization.materialization_id)
    if record is None and (materialization.desired_phase != "running" or not materialization.autoscaling or not cfg.runtime_autoscaling_enabled):
        # Nothing to remember for a runtime autoscaling may not touch.
        return []
    status = get_materialization_status(table, materialization.materialization_id)
    partitions = [topic.partitions for topic in consumer_lag.topics.values() if topic.partitions]
    plan = plan_autoscale(
        materialization,
        record,
        lag=consumer_lag.total_lag,
        measured_at=consumer_lag.measured_at,
        capacity=assignable_members(partitions) if partitions else None,
        healthy=_healthy(status),
        policy=AutoscalePolicy.from_config(cfg),
    )
    save_runtime_autoscale(table, plan.autoscale)
    if not plan.changed:
        return []
    replicas = plan.autoscale.replicas or materialization.replicas
    log_protocol_event(
        LOGGER,
        enabled=cfg.protocol_logging_enabled,
        event="control_plane_runtime_autoscaled",
        materialization_id=materialization.materialization_id,
        replicas=replicas,
        own_replicas=materialization.replicas,
        lag=consumer_lag.total_lag,
        eta_seconds=plan.autoscale.eta_seconds,
        reason=plan.autoscale.reason,
    )
    return [_apply_work(materialization, replicas, at=consumer_lag.measured_at, config=cfg)]


def forget_autoscale(table: TableProtocol, materialization_id: str) -> bool:
    """Forget a materialization's autoscaling; whether its runtime was scaled up."""
    record = get_runtime_autoscale(table, materialization_id)
    if record is None:
        return False
    delete_runtime_autoscale(table, materialization_id)
    return record.replicas is not None


def _apply_work(materialization: Materialization, replicas: int, *, at: str, config: ControlPlaneConfig) -> MaterializationWorkEvent:
    materialization_id = materialization.materialization_id
    return MaterializationWorkEvent(
        work_id=f"work-apply-autoscale-{materialization_id}-{at}",
        materialization_id=materialization_id,
        step="apply_runtime",
        requested_at=at,
        caused_by=EventCause(kind="scheduler", id="runtime-autoscaling"),
        dedupe_key=f"apply_runtime:{materialization_id}:autoscale:{replicas}:{at}",
        target_runtime=TargetRuntime(
            namespace=materialization.namespace,
            kaspr_app_name=resolve_runtime_app_name(materialization, config=config),
        ),
    )


def _healthy(status: MaterializationStatus | None) -> bool:
    return status is not None and status.observed_phase == "running" and status.runtime_health.level == "healthy"


def _since(points: tuple[LagPoint, ...], changed_at: str | None, now: str) -> list[LagPoint]:
    """The measurements after the last change, within the pace window."""
    return [
        point
        for point in points
        if (changed_at is None or point.at > changed_at) and _seconds(point.at, now) <= PACE_WINDOW_SECONDS
    ]


def _pace(points: tuple[LagPoint, ...], *, since: str | None, now: str) -> float | None:
    """Records a second the lag shrank by, from the first measurement after `since` to the last."""
    window = _since(points, since, now)
    if len(window) < 2:
        return None
    seconds = _seconds(window[0].at, window[-1].at)
    if seconds <= 0:
        return None
    return (window[0].lag - window[-1].lag) / seconds


def _reason(lag: int, current: int, eta: int | None) -> str:
    """Why the runtime gets more members, in words for a person."""
    members = "member" if current == 1 else "members"
    if eta is None:
        return f"{lag:,} records behind and not catching up with {current} {members}"
    return f"{lag:,} records behind, about {_duration(eta)} to catch up with {current} {members}"


def _duration(seconds: int) -> str:
    if seconds < 3600:
        return f"{max(1, round(seconds / 60))} min"
    hours, rest = divmod(seconds, 3600)
    if hours >= 48:
        return f"{round(seconds / 86400)} days"
    minutes = round(rest / 60)
    return f"{hours} h {minutes} min" if minutes else f"{hours} h"


def _seconds(start: str, end: str) -> float:
    return (_parse(end) - _parse(start)).total_seconds()


def _parse(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)
