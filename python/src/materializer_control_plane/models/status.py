"""Observed-state models aligned with the status contract."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from materializer_control_plane.models.materialization import LIFECYCLE_PHASES
from materializer_control_plane.models.offsets import table_offsets_from, validate_table_offsets

OBSERVATION_SOURCES = ("runtime_event", "resource_observation", "hybrid")
RUNTIME_HEALTH_LEVELS = ("healthy", "degraded", "unknown", "unreachable")
CONDITION_STATUSES = ("True", "False", "Unknown")


def _validate_choice(name: str, value: str, choices: tuple[str, ...]) -> None:
    if value not in choices:
        allowed = ", ".join(choices)
        raise ValueError(f"{name} must be one of: {allowed}")


@dataclass(frozen=True, slots=True)
class RuntimeHealth:
    level: str
    reason: str | None = None
    message: str | None = None

    def __post_init__(self) -> None:
        _validate_choice("level", self.level, RUNTIME_HEALTH_LEVELS)

    def to_dict(self) -> dict[str, str | None]:
        return {
            "level": self.level,
            "reason": self.reason,
            "message": self.message,
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "RuntimeHealth":
        return cls(
            level=value["level"],
            reason=value.get("reason"),
            message=value.get("message"),
        )


@dataclass(frozen=True, slots=True)
class MemberProgress:
    """Last counter values and state observed from a single runtime member.

    Runtime counters are process-local and reset when a pod restarts. Retaining the
    previous reading per member is what makes monotonic cumulative totals possible. The
    state fields let the materialization's status describe every member, rather than
    whichever one reported last.
    """

    rows_written: int = 0
    bytes_written: int = 0
    files_written: int = 0
    records_skipped: int = 0
    reported_at: str | None = None
    health: str | None = None
    health_reason: str | None = None
    phase_reason: str | None = None
    phase_message: str | None = None
    last_error: str | None = None
    #: Values this member's seals wrote to `_unmapped` (process-local counter, like the others).
    unmapped_values: int = 0
    #: The same, per table, as last reported; what makes each table's total monotonic.
    table_unmapped: dict[str, int] = field(default_factory=dict)
    #: Files this member uploaded that no commit covers yet.
    files_pending_commit: int | None = None
    # Live runtime-settings revision this member last reported running; None when it predates them.
    settings_revision: int | None = None
    # Lineage of those settings: "" when they have none, None when the member predates lineages.
    settings_lineage: str | None = None
    # Last per-category skip counts this member reported, for monotonic skip-reason totals.
    skip_counts: dict[str, int] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "rows_written": self.rows_written,
            "bytes_written": self.bytes_written,
            "files_written": self.files_written,
            "records_skipped": self.records_skipped,
            "unmapped_values": self.unmapped_values,
        }
        optional_values = {
            "reported_at": self.reported_at,
            "health": self.health,
            "health_reason": self.health_reason,
            "phase_reason": self.phase_reason,
            "phase_message": self.phase_message,
            "last_error": self.last_error,
            "files_pending_commit": self.files_pending_commit,
            "settings_revision": self.settings_revision,
            "settings_lineage": self.settings_lineage,
        }
        for key, value in optional_values.items():
            if value is not None:
                payload[key] = value
        if self.skip_counts:
            payload["skip_counts"] = dict(self.skip_counts)
        if self.table_unmapped:
            payload["table_unmapped"] = dict(self.table_unmapped)
        return payload

    @classmethod
    def from_dict(cls, value: dict[str, Any] | None) -> "MemberProgress":
        if not value:
            return cls()
        return cls(
            rows_written=value.get("rows_written") or 0,
            bytes_written=value.get("bytes_written") or 0,
            files_written=value.get("files_written") or 0,
            records_skipped=value.get("records_skipped") or 0,
            reported_at=value.get("reported_at"),
            health=value.get("health"),
            health_reason=value.get("health_reason"),
            phase_reason=value.get("phase_reason"),
            phase_message=value.get("phase_message"),
            last_error=value.get("last_error"),
            unmapped_values=value.get("unmapped_values") or 0,
            files_pending_commit=value.get("files_pending_commit"),
            settings_revision=value.get("settings_revision"),
            settings_lineage=value.get("settings_lineage") if isinstance(value.get("settings_lineage"), str) else None,
            skip_counts=dict(value.get("skip_counts") or {}),
            table_unmapped={str(key): int(item) for key, item in (value.get("table_unmapped") or {}).items()},
        )


@dataclass(frozen=True, slots=True)
class SkipReasonTotal:
    """Records skipped for one reason across all members, cumulative since creation."""

    category: str
    count: int
    last_reason: str
    last_seen_at: str | None = None
    last_partition: int | None = None
    last_offset: int | None = None
    last_schema_version: int | None = None
    last_member_id: str | None = None

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "category": self.category,
            "count": self.count,
            "last_reason": self.last_reason,
        }
        optional_values = {
            "last_seen_at": self.last_seen_at,
            "last_partition": self.last_partition,
            "last_offset": self.last_offset,
            "last_schema_version": self.last_schema_version,
            "last_member_id": self.last_member_id,
        }
        for key, value in optional_values.items():
            if value is not None:
                payload[key] = value
        return payload

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "SkipReasonTotal":
        return cls(
            category=value["category"],
            count=value.get("count") or 0,
            last_reason=value.get("last_reason") or value["category"],
            last_seen_at=value.get("last_seen_at"),
            last_partition=value.get("last_partition"),
            last_offset=value.get("last_offset"),
            last_schema_version=value.get("last_schema_version"),
            last_member_id=value.get("last_member_id"),
        )


@dataclass(frozen=True, slots=True)
class RuntimeProgress:
    rows_written: int | None = None
    bytes_written: int | None = None
    files_written: int | None = None
    records_skipped: int | None = None
    #: The highest offset handled, per table and source partition, as members last reported it.
    latest_offsets: dict[str, dict[str, int]] = field(default_factory=dict)
    cumulative_rows_written: int | None = None
    cumulative_bytes_written: int | None = None
    cumulative_files_written: int | None = None
    cumulative_records_skipped: int | None = None
    cumulative_unmapped_values: int | None = None
    #: When a member last reported values going to `_unmapped`.
    unmapped_last_seen_at: str | None = None
    #: Values written to `_unmapped`, per table, monotonic across member restarts; and when a
    #: member last reported one for the table.
    cumulative_unmapped_by_table: dict[str, int] = field(default_factory=dict)
    unmapped_last_seen_by_table: dict[str, str] = field(default_factory=dict)
    member_progress: dict[str, MemberProgress] = field(default_factory=dict)
    skip_reasons: dict[str, SkipReasonTotal] = field(default_factory=dict)
    #: Each table as the committer member last reported it, by table name (spec §7.12).
    tables: dict[str, dict[str, Any]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for name in (
            "rows_written",
            "bytes_written",
            "files_written",
            "records_skipped",
            "cumulative_rows_written",
            "cumulative_bytes_written",
            "cumulative_files_written",
            "cumulative_records_skipped",
            "cumulative_unmapped_values",
        ):
            value = getattr(self, name)
            if value is not None and value < 0:
                raise ValueError(f"{name} must be >= 0")
        validate_table_offsets("latest_offsets", self.latest_offsets)

    def table(self, name: str) -> dict[str, Any] | None:
        """The committer's last report of the table called `name`."""
        return self.tables.get(name)

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {}
        for name in (
            "rows_written",
            "bytes_written",
            "files_written",
            "records_skipped",
            "cumulative_rows_written",
            "cumulative_bytes_written",
            "cumulative_files_written",
            "cumulative_records_skipped",
            "cumulative_unmapped_values",
        ):
            value = getattr(self, name)
            if value is not None:
                payload[name] = value
        if self.unmapped_last_seen_at is not None:
            payload["unmapped_last_seen_at"] = self.unmapped_last_seen_at
        if self.cumulative_unmapped_by_table:
            payload["cumulative_unmapped_by_table"] = dict(self.cumulative_unmapped_by_table)
        if self.unmapped_last_seen_by_table:
            payload["unmapped_last_seen_by_table"] = dict(self.unmapped_last_seen_by_table)
        if self.tables:
            payload["tables"] = {name: dict(block) for name, block in self.tables.items()}
        if self.latest_offsets:
            payload["latest_offsets"] = {table: dict(offsets) for table, offsets in self.latest_offsets.items()}
        if self.member_progress:
            payload["member_progress"] = {
                member_id: progress.to_dict()
                for member_id, progress in self.member_progress.items()
            }
        if self.skip_reasons:
            payload["skip_reasons"] = [
                reason.to_dict()
                for reason in sorted(
                    self.skip_reasons.values(), key=lambda reason: reason.count, reverse=True
                )
            ]
        return payload

    @classmethod
    def from_dict(cls, value: dict[str, Any] | None) -> "RuntimeProgress":
        if not value:
            return cls()
        return cls(
            rows_written=value.get("rows_written"),
            bytes_written=value.get("bytes_written"),
            files_written=value.get("files_written"),
            records_skipped=value.get("records_skipped"),
            latest_offsets=table_offsets_from(value.get("latest_offsets")),
            cumulative_rows_written=value.get("cumulative_rows_written"),
            cumulative_bytes_written=value.get("cumulative_bytes_written"),
            cumulative_files_written=value.get("cumulative_files_written"),
            cumulative_records_skipped=value.get("cumulative_records_skipped"),
            cumulative_unmapped_values=value.get("cumulative_unmapped_values"),
            unmapped_last_seen_at=value.get("unmapped_last_seen_at"),
            cumulative_unmapped_by_table={
                str(key): int(item) for key, item in (value.get("cumulative_unmapped_by_table") or {}).items()
            },
            unmapped_last_seen_by_table={
                str(key): str(item) for key, item in (value.get("unmapped_last_seen_by_table") or {}).items()
            },
            tables={
                str(name): dict(block)
                for name, block in (value.get("tables") or {}).items()
                if isinstance(block, dict)
            },
            member_progress={
                member_id: MemberProgress.from_dict(entry)
                for member_id, entry in (value.get("member_progress") or {}).items()
            },
            skip_reasons={
                reason.category: reason
                for reason in (
                    SkipReasonTotal.from_dict(item)
                    for item in value.get("skip_reasons") or []
                    if isinstance(item, dict) and item.get("category")
                )
            },
        )


@dataclass(frozen=True, slots=True)
class TopicLag:
    """How far one source topic's table trails the topic, partition by partition."""

    total_lag: int | None = None
    partition_lag: dict[str, int] = field(default_factory=dict)
    end_offsets: dict[str, int] = field(default_factory=dict)
    partitions: int | None = None
    error: str | None = None

    def __post_init__(self) -> None:
        if self.total_lag is not None and self.total_lag < 0:
            raise ValueError("total_lag must be >= 0")
        if self.partitions is not None and self.partitions < 0:
            raise ValueError("partitions must be >= 0")

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {}
        if self.total_lag is not None:
            payload["total_lag"] = self.total_lag
        if self.partition_lag:
            payload["partition_lag"] = dict(self.partition_lag)
        if self.end_offsets:
            payload["end_offsets"] = dict(self.end_offsets)
        if self.partitions is not None:
            payload["partitions"] = self.partitions
        if self.error is not None:
            payload["error"] = self.error
        return payload

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "TopicLag":
        return cls(
            total_lag=value.get("total_lag"),
            partition_lag=dict(value.get("partition_lag", {})),
            end_offsets=dict(value.get("end_offsets", {})),
            partitions=value.get("partitions"),
            error=value.get("error"),
        )


@dataclass(frozen=True, slots=True)
class ConsumerLag:
    """Most recent consumer-group lag measurement for a materialization, per source topic.

    `total_lag` sums every topic's, so a materialization reads as one number in lists.
    """

    total_lag: int | None = None
    topics: dict[str, TopicLag] = field(default_factory=dict)
    measured_at: str | None = None
    error: str | None = None

    def __post_init__(self) -> None:
        if self.total_lag is not None and self.total_lag < 0:
            raise ValueError("total_lag must be >= 0")

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {}
        if self.total_lag is not None:
            payload["total_lag"] = self.total_lag
        if self.topics:
            payload["topics"] = {topic: lag.to_dict() for topic, lag in self.topics.items()}
        if self.measured_at is not None:
            payload["measured_at"] = self.measured_at
        if self.error is not None:
            payload["error"] = self.error
        return payload

    @classmethod
    def from_dict(cls, value: dict[str, Any] | None) -> "ConsumerLag | None":
        if not value:
            return None
        return cls(
            total_lag=value.get("total_lag"),
            topics={
                str(topic): TopicLag.from_dict(lag)
                for topic, lag in (value.get("topics") or {}).items()
                if isinstance(lag, dict)
            },
            measured_at=value.get("measured_at"),
            error=value.get("error"),
        )


@dataclass(frozen=True, slots=True)
class ResourceCondition:
    status: str
    reason: str | None
    message: str | None
    last_transition_time: str | None

    def __post_init__(self) -> None:
        _validate_choice("status", self.status, CONDITION_STATUSES)

    def to_dict(self) -> dict[str, str | None]:
        return {
            "status": self.status,
            "reason": self.reason,
            "message": self.message,
            "last_transition_time": self.last_transition_time,
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any] | None) -> "ResourceCondition | None":
        if value is None:
            return None
        return cls(
            status=value["status"],
            reason=value.get("reason"),
            message=value.get("message"),
            last_transition_time=value.get("last_transition_time"),
        )


@dataclass(frozen=True, slots=True)
class RuntimeMemberState:
    """One runtime member's pod as the control plane last read it: how often its container restarted, and why.

    `restarts` counts since the pod was created; `last_exit_*` describe the container's last
    termination (`OOMKilled`, `Error`, ...) and when it ended; `waiting_reason` is what it waits on
    now, `CrashLoopBackOff` while Kubernetes holds off restarting it again.
    """

    name: str
    restarts: int = 0
    last_exit_reason: str | None = None
    last_exit_code: int | None = None
    last_exited_at: str | None = None
    waiting_reason: str | None = None

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("name is required")
        if self.restarts < 0:
            raise ValueError("restarts must be >= 0")

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {"name": self.name, "restarts": self.restarts}
        for key, value in (
            ("last_exit_reason", self.last_exit_reason),
            ("last_exit_code", self.last_exit_code),
            ("last_exited_at", self.last_exited_at),
            ("waiting_reason", self.waiting_reason),
        ):
            if value is not None:
                payload[key] = value
        return payload

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "RuntimeMemberState":
        return cls(
            name=value["name"],
            restarts=int(value.get("restarts") or 0),
            last_exit_reason=value.get("last_exit_reason"),
            last_exit_code=value.get("last_exit_code"),
            last_exited_at=value.get("last_exited_at"),
            waiting_reason=value.get("waiting_reason"),
        )


@dataclass(frozen=True, slots=True)
class ResourceObservation:
    namespace: str
    kaspr_app_name: str
    resource_exists: bool
    managed_agent_states: dict[str, str]
    metadata_generation: int | None = None
    observed_generation: int | None = None
    ready_condition: ResourceCondition | None = None
    progressing_condition: ResourceCondition | None = None
    rollout_in_progress: bool | None = None
    available_members: str | None = None
    rebalance_required: bool | None = None
    rebalancing_members: str | None = None
    managed_agents_total: int | None = None
    managed_agents_running: int | None = None
    managed_agents_paused: int | None = None
    managed_agents_unknown: int | None = None
    #: Each member's pod and its restarts; None when the pods could not be read.
    members: tuple[RuntimeMemberState, ...] | None = None

    def __post_init__(self) -> None:
        if not self.namespace:
            raise ValueError("namespace is required")
        if not self.kaspr_app_name:
            raise ValueError("kaspr_app_name is required")
        for name in (
            "metadata_generation",
            "observed_generation",
            "managed_agents_total",
            "managed_agents_running",
            "managed_agents_paused",
            "managed_agents_unknown",
        ):
            value = getattr(self, name)
            if value is not None and value < 0:
                raise ValueError(f"{name} must be >= 0")

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "namespace": self.namespace,
            "kaspr_app_name": self.kaspr_app_name,
            "resource_exists": self.resource_exists,
            "managed_agent_states": dict(self.managed_agent_states),
        }
        optional_values = {
            "metadata_generation": self.metadata_generation,
            "observed_generation": self.observed_generation,
            "ready_condition": None if self.ready_condition is None else self.ready_condition.to_dict(),
            "progressing_condition": None
            if self.progressing_condition is None
            else self.progressing_condition.to_dict(),
            "rollout_in_progress": self.rollout_in_progress,
            "available_members": self.available_members,
            "rebalance_required": self.rebalance_required,
            "rebalancing_members": self.rebalancing_members,
            "managed_agents_total": self.managed_agents_total,
            "managed_agents_running": self.managed_agents_running,
            "managed_agents_paused": self.managed_agents_paused,
            "managed_agents_unknown": self.managed_agents_unknown,
            "members": None if self.members is None else [member.to_dict() for member in self.members],
        }
        for key, value in optional_values.items():
            if value is not None:
                payload[key] = value
        return payload

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "ResourceObservation":
        return cls(
            namespace=value["namespace"],
            kaspr_app_name=value["kaspr_app_name"],
            resource_exists=value["resource_exists"],
            managed_agent_states=dict(value.get("managed_agent_states", {})),
            metadata_generation=value.get("metadata_generation"),
            observed_generation=value.get("observed_generation"),
            ready_condition=ResourceCondition.from_dict(value.get("ready_condition")),
            progressing_condition=ResourceCondition.from_dict(value.get("progressing_condition")),
            rollout_in_progress=value.get("rollout_in_progress"),
            available_members=value.get("available_members"),
            rebalance_required=value.get("rebalance_required"),
            rebalancing_members=value.get("rebalancing_members"),
            managed_agents_total=value.get("managed_agents_total"),
            managed_agents_running=value.get("managed_agents_running"),
            managed_agents_paused=value.get("managed_agents_paused"),
            managed_agents_unknown=value.get("managed_agents_unknown"),
            members=None
            if value.get("members") is None
            else tuple(RuntimeMemberState.from_dict(member) for member in value["members"]),
        )


@dataclass(frozen=True, slots=True)
class MaterializationStatus:
    materialization_id: str
    desired_phase: str
    observed_phase: str
    phase_reason: str
    last_transition_time: str
    observation_source: str
    runtime_health: RuntimeHealth
    resource_observation: ResourceObservation
    status_version: int = 1
    phase_message: str | None = None
    last_heartbeat: str | None = None
    last_resource_observation_time: str | None = None
    runtime_progress: RuntimeProgress = field(default_factory=RuntimeProgress)
    consumer_lag: ConsumerLag | None = None
    # Version of the materializer package the runtime last reported running. Null until a status
    # event arrives, and may lag the control plane while a runtime awaits a spec re-apply.
    runtime_version: str | None = None
    last_error: str | None = None

    def __post_init__(self) -> None:
        if not self.materialization_id:
            raise ValueError("materialization_id is required")
        _validate_choice("desired_phase", self.desired_phase, LIFECYCLE_PHASES)
        _validate_choice("observed_phase", self.observed_phase, LIFECYCLE_PHASES)
        if not self.phase_reason:
            raise ValueError("phase_reason is required")
        if not self.last_transition_time:
            raise ValueError("last_transition_time is required")
        _validate_choice("observation_source", self.observation_source, OBSERVATION_SOURCES)
        if self.status_version != 1:
            raise ValueError("status_version must be 1")

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "materialization_id": self.materialization_id,
            "desired_phase": self.desired_phase,
            "observed_phase": self.observed_phase,
            "phase_reason": self.phase_reason,
            "last_transition_time": self.last_transition_time,
            "observation_source": self.observation_source,
            "runtime_health": self.runtime_health.to_dict(),
            "resource_observation": self.resource_observation.to_dict(),
            "status_version": self.status_version,
        }
        optional_values = {
            "phase_message": self.phase_message,
            "last_heartbeat": self.last_heartbeat,
            "last_resource_observation_time": self.last_resource_observation_time,
            "runtime_version": self.runtime_version,
            "last_error": self.last_error,
        }
        for key, value in optional_values.items():
            if value is not None:
                payload[key] = value
        runtime_progress = self.runtime_progress.to_dict()
        if runtime_progress:
            payload["runtime_progress"] = runtime_progress
        if self.consumer_lag is not None:
            consumer_lag = self.consumer_lag.to_dict()
            if consumer_lag:
                payload["consumer_lag"] = consumer_lag
        return payload

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "MaterializationStatus":
        return cls(
            materialization_id=value["materialization_id"],
            desired_phase=value["desired_phase"],
            observed_phase=value["observed_phase"],
            phase_reason=value["phase_reason"],
            phase_message=value.get("phase_message"),
            last_transition_time=value["last_transition_time"],
            last_heartbeat=value.get("last_heartbeat"),
            last_resource_observation_time=value.get("last_resource_observation_time"),
            observation_source=value["observation_source"],
            runtime_health=RuntimeHealth.from_dict(value["runtime_health"]),
            runtime_progress=RuntimeProgress.from_dict(value.get("runtime_progress")),
            consumer_lag=ConsumerLag.from_dict(value.get("consumer_lag")),
            resource_observation=ResourceObservation.from_dict(value["resource_observation"]),
            runtime_version=value.get("runtime_version"),
            last_error=value.get("last_error"),
            status_version=value.get("status_version", 1),
        )