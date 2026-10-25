"""Event models aligned with the external contracts."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from materializer_control_plane.models.materialization import (
    PARTITION_GRANULARITIES,
    RUNTIME_MODES,
    TableSpec,
    validate_table_specs,
)
from materializer_control_plane.models.offsets import table_offsets_from, validate_table_offsets
from materializer_control_plane.models.status import ConsumerLag
from materializer_control_plane.scale import normalize_replicas, normalize_storage_size

# `pause` is no longer requested by any endpoint; it stays decodable for events already on the topic.
REQUEST_ACTIONS = ("create", "pause", "stop", "resume", "delete", "update")
WORK_STEPS = (
    # The directory's answer to a claim of table names (`services/table_claims.py`): a create or an
    # update that adds topics goes on once its names are claimed, and stops when they are refused.
    "tables_claimed",
    "claim_rejected",
    "start_discovery",
    # Discovery of the tables an update adds, while the runtime keeps writing the others.
    "discover_tables",
    "create_table",
    "apply_runtime",
    # Wait for a runtime held at zero, so its consumer group starts over with a changed set of topics.
    "await_runtime_drained",
    "delete_runtime",
    "cleanup_runtime_kafka",
    "drop_table",
    "purge_materialization_records",
    "observe_runtime",
    "sweep_observations",
    "purge_deleted_materializations",
    "refresh_consumer_lag",
    "record_consumer_lag",
    # Tick every maintained table for the maintenance app, each minute.
    "schedule_table_maintenance",
    "repair_state",
)
CAUSE_KINDS = (
    "request",
    "runtime_status",
    "resource_observation",
    "scheduler",
    "repair",
)
OBSERVATION_MODES = ("one_shot", "sweep")
#: What a claim of table names is for: a new materialization's tables, or an update's new tables.
CLAIM_PURPOSES = ("create", "update")
CLAIM_STATUSES = ("applied", "rejected")
RUNTIME_EVENT_PHASES = ("running", "degraded")
RUNTIME_EVENT_HEALTH = ("healthy", "degraded", "unknown")


def _validate_choice(name: str, value: str, choices: tuple[str, ...]) -> None:
    if value not in choices:
        allowed = ", ".join(choices)
        raise ValueError(f"{name} must be one of: {allowed}")


def _validate_non_negative(name: str, value: int | None) -> None:
    if value is not None and value < 0:
        raise ValueError(f"{name} must be >= 0")


def _optional_int(value: Any) -> int | None:
    """An integer field another component reported, or None when it is absent or malformed."""
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value


def _optional_str(value: Any) -> str | None:
    """A string field another component reported, or None when it is absent or malformed."""
    return value if isinstance(value, str) else None


@dataclass(frozen=True, slots=True)
class MaterializationRequestSpec:
    #: One table per source topic, each in its dataset and named when the request was accepted.
    tables: tuple[TableSpec, ...]
    #: The deployment's destination, its namespace, the runtime's generated name and the storage per
    #: replica, as the API resolved them: a request chooses none of them.
    destination_uri: str
    #: The materialization's label; its id when none was given.
    name: str | None = None
    runtime_mode: str = "dedicated"
    runtime_name: str | None = None
    namespace: str | None = None
    resource_profile: str | None = None
    storage_size: str | None = None
    replicas: int | None = None
    skip_tombstones: bool = False
    #: Whether the control plane may scale its runtime while it is far behind. On a create None turns
    #: it on; on an update None leaves it as it is, as an event from before 4.4 says nothing of it.
    autoscaling: bool | None = None
    #: Event-time partitioning of the tables: `hour`, `day`, `month` or `none`; None takes the
    #: default setting.
    partition_granularity: str | None = None
    labels: dict[str, str] = field(default_factory=dict)
    annotations: dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        validate_table_specs(self.tables)
        if not self.destination_uri:
            raise ValueError("destination_uri is required")
        _validate_choice("runtime_mode", self.runtime_mode, RUNTIME_MODES)
        if self.storage_size is not None:
            normalize_storage_size(self.storage_size)
        if self.replicas is not None:
            normalize_replicas(self.replicas)
        if not isinstance(self.skip_tombstones, bool):
            raise ValueError("skip_tombstones must be a boolean")
        if self.autoscaling is not None and not isinstance(self.autoscaling, bool):
            raise ValueError("autoscaling must be a boolean")
        if self.partition_granularity is not None:
            _validate_choice("partition_granularity", self.partition_granularity, PARTITION_GRANULARITIES)

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "tables": [table.to_dict() for table in self.tables],
            "destination_uri": self.destination_uri,
            "runtime_mode": self.runtime_mode,
            "runtime_name": self.runtime_name,
            "namespace": self.namespace,
            "resource_profile": self.resource_profile,
            "storage_size": self.storage_size,
            "replicas": self.replicas,
            "skip_tombstones": self.skip_tombstones,
            "autoscaling": self.autoscaling,
            "partition_granularity": self.partition_granularity,
            "labels": dict(self.labels),
            "annotations": dict(self.annotations),
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "MaterializationRequestSpec":
        return cls(
            name=value.get("name"),
            tables=tuple(TableSpec.from_dict(item) for item in value.get("tables") or ()),
            destination_uri=value["destination_uri"],
            runtime_mode=value.get("runtime_mode", "dedicated"),
            runtime_name=value.get("runtime_name"),
            namespace=value.get("namespace"),
            resource_profile=value.get("resource_profile"),
            storage_size=value.get("storage_size"),
            replicas=value.get("replicas"),
            skip_tombstones=value.get("skip_tombstones", False),
            autoscaling=value.get("autoscaling"),
            partition_granularity=value.get("partition_granularity"),
            labels=dict(value.get("labels", {})),
            annotations=dict(value.get("annotations", {})),
        )


#: What becomes of a table whose topic an update removes, or of a kept table: dropped with its data,
#: or kept, read-only.
TABLE_ACTIONS = ("drop", "keep")


@dataclass(frozen=True, slots=True)
class TopicChangeRequest:
    """An update's change of topics: the topics to read from now on, and what becomes of tables.

    `new_tables` gives each topic the materialization has no table of yet the table it gets: its
    dataset and name, chosen with the request. A topic it reads, or kept a table of, keeps that
    table. `table_actions` names, for each table whose topic is removed, `drop` or `keep`; it may
    also `drop` a table kept earlier (`services/topic_changes.py`).
    """

    source_topics: tuple[str, ...]
    table_actions: dict[str, str] = field(default_factory=dict)
    new_tables: tuple[TableSpec, ...] = ()

    def __post_init__(self) -> None:
        if not self.source_topics:
            raise ValueError("a materialization reads at least one topic")
        for name, action in self.table_actions.items():
            if action not in TABLE_ACTIONS:
                raise ValueError(f"the action for table {name} must be one of: {', '.join(TABLE_ACTIONS)}")
        if not all(isinstance(spec, TableSpec) for spec in self.new_tables):
            raise ValueError("new_tables must be TableSpecs")

    def new_table(self, topic: str) -> TableSpec | None:
        """The table a topic new to the materialization gets."""
        return next((spec for spec in self.new_tables if spec.source_topic == topic), None)

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_topics": list(self.source_topics),
            "table_actions": dict(self.table_actions),
            "new_tables": [spec.to_dict() for spec in self.new_tables],
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any] | None) -> "TopicChangeRequest | None":
        if not value:
            return None
        return cls(
            source_topics=tuple(str(topic) for topic in value.get("source_topics") or ()),
            table_actions={str(name): str(action) for name, action in (value.get("table_actions") or {}).items()},
            new_tables=tuple(TableSpec.from_dict(item) for item in value.get("new_tables") or ()),
        )


@dataclass(frozen=True, slots=True)
class MaterializationRequestEvent:
    request_id: str
    materialization_id: str
    action: str
    submitted_at: str
    submitted_by: str
    spec: MaterializationRequestSpec
    #: The account that sent the request; `submitted_by` is its email. None for older requests.
    submitted_by_id: str | None = None
    #: An `update` that changes the topics; None for one that changes only the replica count.
    topic_change: TopicChangeRequest | None = None
    #: The directory request that claims the new tables' names, for a create and for an update that
    #: adds topics: chosen when the request is accepted, so a client can wait on its outcome.
    claim_request_id: str | None = None
    #: An `update` that relabels the materialization: its new label. None leaves the label alone.
    label: str | None = None

    def __post_init__(self) -> None:
        for name in (
            "request_id",
            "materialization_id",
            "submitted_at",
            "submitted_by",
        ):
            if not getattr(self, name):
                raise ValueError(f"{name} is required")
        _validate_choice("action", self.action, REQUEST_ACTIONS)

    def to_dict(self) -> dict[str, Any]:
        payload = {
            "request_id": self.request_id,
            "materialization_id": self.materialization_id,
            "action": self.action,
            "submitted_at": self.submitted_at,
            "submitted_by": self.submitted_by,
            "spec": self.spec.to_dict(),
        }
        if self.submitted_by_id is not None:
            payload["submitted_by_id"] = self.submitted_by_id
        if self.topic_change is not None:
            payload["topic_change"] = self.topic_change.to_dict()
        if self.claim_request_id is not None:
            payload["claim_request_id"] = self.claim_request_id
        if self.label is not None:
            payload["label"] = self.label
        return payload

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "MaterializationRequestEvent":
        return cls(
            request_id=value["request_id"],
            materialization_id=value["materialization_id"],
            action=value["action"],
            submitted_at=value["submitted_at"],
            submitted_by=value["submitted_by"],
            spec=MaterializationRequestSpec.from_dict(value["spec"]),
            submitted_by_id=value.get("submitted_by_id"),
            topic_change=TopicChangeRequest.from_dict(value.get("topic_change")),
            claim_request_id=value.get("claim_request_id"),
            label=value.get("label"),
        )


@dataclass(frozen=True, slots=True)
class EventCause:
    kind: str
    id: str

    def __post_init__(self) -> None:
        _validate_choice("kind", self.kind, CAUSE_KINDS)
        if not self.id:
            raise ValueError("id is required")

    def to_dict(self) -> dict[str, str]:
        return {"kind": self.kind, "id": self.id}

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "EventCause":
        return cls(kind=value["kind"], id=value["id"])


@dataclass(frozen=True, slots=True)
class TargetRuntime:
    namespace: str | None = None
    kaspr_app_name: str | None = None

    def to_dict(self) -> dict[str, str]:
        payload: dict[str, str] = {}
        if self.namespace is not None:
            payload["namespace"] = self.namespace
        if self.kaspr_app_name is not None:
            payload["kaspr_app_name"] = self.kaspr_app_name
        return payload

    @classmethod
    def from_dict(cls, value: dict[str, Any] | None) -> "TargetRuntime | None":
        if value is None:
            return None
        return cls(
            namespace=value.get("namespace"),
            kaspr_app_name=value.get("kaspr_app_name"),
        )


@dataclass(frozen=True, slots=True)
class ObservationPolicy:
    attempt: int | None = None
    max_attempts: int | None = None
    scheduled_for: str | None = None
    mode: str | None = None

    def __post_init__(self) -> None:
        if self.attempt is not None and self.attempt < 1:
            raise ValueError("attempt must be >= 1")
        if self.max_attempts is not None and self.max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        if self.mode is not None:
            _validate_choice("mode", self.mode, OBSERVATION_MODES)

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {}
        if self.attempt is not None:
            payload["attempt"] = self.attempt
        if self.max_attempts is not None:
            payload["max_attempts"] = self.max_attempts
        if self.scheduled_for is not None:
            payload["scheduled_for"] = self.scheduled_for
        if self.mode is not None:
            payload["mode"] = self.mode
        return payload

    @classmethod
    def from_dict(cls, value: dict[str, Any] | None) -> "ObservationPolicy | None":
        if value is None:
            return None
        return cls(
            attempt=value.get("attempt"),
            max_attempts=value.get("max_attempts"),
            scheduled_for=value.get("scheduled_for"),
            mode=value.get("mode"),
        )


@dataclass(frozen=True, slots=True)
class TableClaimResult:
    """What the directory decided about a materialization's claim of table names.

    `details` says which table a refusal was about (`IdentityRequestOutcome.details`).
    """

    request_id: str
    purpose: str
    status: str
    reason: str | None = None
    message: str | None = None
    details: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.request_id:
            raise ValueError("request_id is required")
        _validate_choice("purpose", self.purpose, CLAIM_PURPOSES)
        _validate_choice("status", self.status, CLAIM_STATUSES)

    @property
    def applied(self) -> bool:
        return self.status == "applied"

    def to_dict(self) -> dict[str, Any]:
        return {
            "request_id": self.request_id,
            "purpose": self.purpose,
            "status": self.status,
            "reason": self.reason,
            "message": self.message,
            "details": dict(self.details),
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any] | None) -> "TableClaimResult | None":
        if not isinstance(value, dict):
            return None
        return cls(
            request_id=str(value.get("request_id") or ""),
            purpose=str(value.get("purpose") or ""),
            status=str(value.get("status") or ""),
            reason=value.get("reason"),
            message=value.get("message"),
            details=dict(value.get("details") or {}),
        )


@dataclass(frozen=True, slots=True)
class MaterializationWorkEvent:
    work_id: str
    materialization_id: str
    step: str
    requested_at: str
    caused_by: EventCause
    dedupe_key: str
    target_runtime: TargetRuntime | None = None
    observation_policy: ObservationPolicy | None = None
    #: The lag a refresh measured, carried to the materialization's own partition to be recorded
    #: there (`record_consumer_lag`); no other step has one.
    consumer_lag: ConsumerLag | None = None
    #: The directory's answer to a claim of table names (`tables_claimed`, `claim_rejected`).
    claim: TableClaimResult | None = None

    def __post_init__(self) -> None:
        for name in ("work_id", "materialization_id", "requested_at", "dedupe_key"):
            if not getattr(self, name):
                raise ValueError(f"{name} is required")
        _validate_choice("step", self.step, WORK_STEPS)
        if self.step == "record_consumer_lag" and self.consumer_lag is None:
            raise ValueError("record_consumer_lag requires consumer_lag")
        if self.step in ("tables_claimed", "claim_rejected") and self.claim is None:
            raise ValueError(f"{self.step} requires claim")

    def to_dict(self) -> dict[str, Any]:
        payload = {
            "work_id": self.work_id,
            "materialization_id": self.materialization_id,
            "step": self.step,
            "requested_at": self.requested_at,
            "caused_by": self.caused_by.to_dict(),
            "dedupe_key": self.dedupe_key,
        }
        if self.target_runtime is not None:
            payload["target_runtime"] = self.target_runtime.to_dict()
        if self.observation_policy is not None:
            payload["observation_policy"] = self.observation_policy.to_dict()
        if self.consumer_lag is not None:
            payload["consumer_lag"] = self.consumer_lag.to_dict()
        if self.claim is not None:
            payload["claim"] = self.claim.to_dict()
        return payload

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "MaterializationWorkEvent":
        return cls(
            work_id=value["work_id"],
            materialization_id=value["materialization_id"],
            step=value["step"],
            requested_at=value["requested_at"],
            caused_by=EventCause.from_dict(value["caused_by"]),
            dedupe_key=value["dedupe_key"],
            target_runtime=TargetRuntime.from_dict(value.get("target_runtime")),
            observation_policy=ObservationPolicy.from_dict(value.get("observation_policy")),
            consumer_lag=ConsumerLag.from_dict(value.get("consumer_lag")),
            claim=TableClaimResult.from_dict(value.get("claim")),
        )


@dataclass(frozen=True, slots=True)
class RuntimeStatusEvent:
    event_id: str
    materialization_id: str
    runtime_app_ref: str
    member_id: str
    agent_name: str
    phase: str
    health: str
    reported_at: str
    #: The Iceberg schema id this member conforms to, per table.
    schema_versions: dict[str, int] = field(default_factory=dict)
    # Version of the materializer package the reporting runtime is running. A runtime keeps its
    # image until its spec is re-applied, so this can legitimately lag the control plane.
    runtime_version: str | None = None
    rows_written: int | None = None
    bytes_written: int | None = None
    files_written: int | None = None
    records_skipped: int | None = None
    #: The highest offset this member has handled, per table and source partition.
    latest_offsets: dict[str, dict[str, int]] = field(default_factory=dict)
    event_time: str | None = None
    phase_reason: str | None = None
    phase_message: str | None = None
    last_error: str | None = None
    health_reason: str | None = None
    # Records this member skipped, grouped by category; counters reset with the process.
    skip_reasons: list[dict[str, Any]] = field(default_factory=list)
    # Revision of the live runtime settings this member runs; 0 when none were ever delivered,
    # None from a runtime that predates live settings.
    settings_revision: int | None = None
    #: Lineage of the live runtime settings this member runs (handbook 17 §17.7): "" when they have
    #: none, None from a runtime that predates lineages.
    settings_lineage: str | None = None
    #: Values this member's seals wrote to `_unmapped` (counter since the process started).
    unmapped_values: int | None = None
    #: Data files this member uploaded that no commit covers yet.
    files_pending_commit: int | None = None
    #: Each table as the committer member sees it, by table name; only the member running the
    #: committer sends them.
    tables: dict[str, dict[str, Any]] | None = None
    #: This member's own counters per table: rows, bytes and files written, values unmapped.
    table_counters: dict[str, dict[str, int]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for name in (
            "event_id",
            "materialization_id",
            "runtime_app_ref",
            "member_id",
            "agent_name",
            "reported_at",
        ):
            if not getattr(self, name):
                raise ValueError(f"{name} is required")
        _validate_choice("phase", self.phase, RUNTIME_EVENT_PHASES)
        _validate_choice("health", self.health, RUNTIME_EVENT_HEALTH)
        for table, schema_id in self.schema_versions.items():
            _validate_non_negative(f"schema_versions[{table}]", schema_id)
        _validate_non_negative("rows_written", self.rows_written)
        _validate_non_negative("bytes_written", self.bytes_written)
        _validate_non_negative("files_written", self.files_written)
        _validate_non_negative("records_skipped", self.records_skipped)
        _validate_non_negative("settings_revision", self.settings_revision)
        _validate_non_negative("unmapped_values", self.unmapped_values)
        _validate_non_negative("files_pending_commit", self.files_pending_commit)
        validate_table_offsets("latest_offsets", self.latest_offsets)

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "event_id": self.event_id,
            "materialization_id": self.materialization_id,
            "runtime_app_ref": self.runtime_app_ref,
            "member_id": self.member_id,
            "agent_name": self.agent_name,
            "phase": self.phase,
            "health": self.health,
            "reported_at": self.reported_at,
        }
        if self.schema_versions:
            payload["schema_versions"] = dict(self.schema_versions)
        optional_values = {
            "rows_written": self.rows_written,
            "runtime_version": self.runtime_version,
            "bytes_written": self.bytes_written,
            "files_written": self.files_written,
            "records_skipped": self.records_skipped,
            "event_time": self.event_time,
            "phase_reason": self.phase_reason,
            "phase_message": self.phase_message,
            "last_error": self.last_error,
            "health_reason": self.health_reason,
            "settings_revision": self.settings_revision,
            "settings_lineage": self.settings_lineage,
            "unmapped_values": self.unmapped_values,
            "files_pending_commit": self.files_pending_commit,
        }
        for key, value in optional_values.items():
            if value is not None:
                payload[key] = value
        if self.latest_offsets:
            payload["latest_offsets"] = {table: dict(offsets) for table, offsets in self.latest_offsets.items()}
        if self.skip_reasons:
            payload["skip_reasons"] = [dict(item) for item in self.skip_reasons]
        if self.tables is not None:
            payload["tables"] = {name: dict(block) for name, block in self.tables.items()}
        if self.table_counters:
            payload["table_counters"] = {name: dict(counters) for name, counters in self.table_counters.items()}
        return payload

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "RuntimeStatusEvent":
        tables = value.get("tables")
        return cls(
            event_id=value["event_id"],
            materialization_id=value["materialization_id"],
            runtime_app_ref=value["runtime_app_ref"],
            member_id=value["member_id"],
            agent_name=value["agent_name"],
            phase=value["phase"],
            health=value["health"],
            schema_versions={
                str(table): schema_id
                for table, schema_id in (value.get("schema_versions") or {}).items()
                if _optional_int(schema_id) is not None
            },
            rows_written=value.get("rows_written"),
            runtime_version=value.get("runtime_version"),
            bytes_written=value.get("bytes_written"),
            files_written=value.get("files_written"),
            records_skipped=value.get("records_skipped"),
            latest_offsets=table_offsets_from(value.get("latest_offsets")),
            event_time=value.get("event_time"),
            phase_reason=value.get("phase_reason"),
            phase_message=value.get("phase_message"),
            reported_at=value["reported_at"],
            last_error=value.get("last_error"),
            health_reason=value.get("health_reason"),
            skip_reasons=[
                dict(item) for item in value.get("skip_reasons") or [] if isinstance(item, dict)
            ],
            settings_revision=_optional_int(value.get("settings_revision")),
            settings_lineage=_optional_str(value.get("settings_lineage")),
            unmapped_values=_optional_int(value.get("unmapped_values")),
            files_pending_commit=_optional_int(value.get("files_pending_commit")),
            tables=(
                {str(name): dict(block) for name, block in tables.items() if isinstance(block, dict)}
                if isinstance(tables, dict)
                else None
            ),
            table_counters={
                str(name): {
                    str(key): count
                    for key, count in counters.items()
                    if _optional_int(count) is not None and count >= 0
                }
                for name, counters in (value.get("table_counters") or {}).items()
                if isinstance(counters, dict)
            },
        )
