"""Desired-state and schema domain models."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from midstream.scale import (
    DEFAULT_REPLICAS,
    DEFAULT_STORAGE_SIZE,
    normalize_replicas,
    normalize_storage_size,
)

LIFECYCLE_PHASES = (
    "requested",
    "discovering",
    "discovered",
    "deploying",
    "running",
    # Recorded by control planes before `stopped` existed; nothing requests it any more.
    "paused",
    # The runtime is scaled to zero replicas; resuming scales it back to `replicas`.
    "stopped",
    "deleting",
    "deleted",
    "failed",
    "degraded",
)
RUNTIME_MODES = ("dedicated", "shared")
PARTITION_GRANULARITIES = ("hour", "day", "month", "none")
DEFAULT_PARTITION_GRANULARITY = "day"


def _validate_non_empty(name: str, value: str | None) -> None:
    if value is None or value == "":
        raise ValueError(f"{name} is required")


def _validate_choice(name: str, value: str, choices: tuple[str, ...]) -> None:
    if value not in choices:
        allowed = ", ".join(choices)
        raise ValueError(f"{name} must be one of: {allowed}")


@dataclass(frozen=True, slots=True)
class TableSpec:
    """One of a materialization's tables: the topic it holds, the dataset it is in, and its name.

    SQL reads it as `<dataset>.<name>`. Both are chosen when the topic is picked - the name
    defaults to one derived from the topic (`table_names.py`) - and never change. `dataset_id` is
    the dataset's identity in the directory; `dataset`, its name, which never changes either.
    """

    name: str
    source_topic: str
    dataset: str
    dataset_id: str

    def __post_init__(self) -> None:
        _validate_non_empty("table name", self.name)
        _validate_non_empty("source_topic", self.source_topic)
        _validate_non_empty("dataset", self.dataset)
        _validate_non_empty("dataset_id", self.dataset_id)

    @property
    def qualified_name(self) -> str:
        """`<dataset>.<table>`: what SQL reads the table by."""
        return f"{self.dataset}.{self.name}"

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "source_topic": self.source_topic,
            "dataset": self.dataset,
            "dataset_id": self.dataset_id,
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "TableSpec":
        return cls(
            name=str(value["name"]),
            source_topic=str(value["source_topic"]),
            dataset=str(value["dataset"]),
            dataset_id=str(value["dataset_id"]),
        )


def validate_table_specs(tables: tuple[TableSpec, ...], retired: tuple[TableSpec, ...] = ()) -> None:
    """A materialization has at least one table, and no two of its tables, kept ones included,
    share a name - whatever their datasets - or a topic."""
    if not tables:
        raise ValueError("a materialization needs at least one source topic")
    names = [table.name.lower() for table in (*tables, *retired)]
    if len(set(names)) != len(names):
        raise ValueError("table names must be unique")
    topics = [table.source_topic for table in (*tables, *retired)]
    if len(set(topics)) != len(topics):
        raise ValueError("source topics must be unique")


def _specs(value: Any) -> tuple[TableSpec, ...]:
    return tuple(TableSpec.from_dict(item) for item in value or ())


@dataclass(frozen=True, slots=True)
class TopicChangePlan:
    """A materialization's tables after a change of topics, and what the change does to each.

    Made by `services/topic_changes.py::plan_topic_change`, against the materialization and its
    table records as they are.
    """

    #: The tables the runtime writes afterwards, in the order of the topics.
    tables: tuple[TableSpec, ...]
    #: The tables kept, read-only, afterwards.
    retired_tables: tuple[TableSpec, ...]
    #: New tables, to claim, discover and create.
    added: tuple[TableSpec, ...]
    #: Kept tables written again.
    resumed: tuple[TableSpec, ...]
    #: Written tables whose topics are removed: kept or dropped.
    removed: tuple[TableSpec, ...]
    #: Written tables kept read-only from now on.
    retired: tuple[TableSpec, ...]
    #: Tables to drop with their data: written ones whose topics are removed, and kept ones.
    dropped: tuple[TableSpec, ...]

    @property
    def changes_runtime(self) -> bool:
        """Whether the runtime reads another set of topics afterwards."""
        return bool(self.added or self.resumed or self.removed)

    @property
    def changes_anything(self) -> bool:
        """Whether the change does anything at all, besides perhaps reordering the topics."""
        return self.changes_runtime or bool(self.dropped)

    def to_dict(self) -> dict[str, Any]:
        return {
            name: [spec.to_dict() for spec in getattr(self, name)]
            for name in ("tables", "retired_tables", "added", "resumed", "removed", "retired", "dropped")
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "TopicChangePlan":
        return cls(
            tables=_specs(value.get("tables")),
            retired_tables=_specs(value.get("retired_tables")),
            added=_specs(value.get("added")),
            resumed=_specs(value.get("resumed")),
            removed=_specs(value.get("removed")),
            retired=_specs(value.get("retired")),
            dropped=_specs(value.get("dropped")),
        )


@dataclass(frozen=True, slots=True)
class PendingTopicChange:
    """A change of topics that adds tables, waiting for the directory to claim their names.

    Request intake plans it and keeps it here; once the names are claimed the workflow applies the
    plan as it stands, and a refusal drops it, leaving the materialization as it was. While one is
    pending, no other change of topics is taken.
    """

    #: The update request that asked for it.
    request_id: str
    #: The directory request claiming the added tables' names (and releasing the dropped ones').
    claim_request_id: str
    submitted_at: str
    plan: TopicChangePlan

    def __post_init__(self) -> None:
        _validate_non_empty("request_id", self.request_id)
        _validate_non_empty("claim_request_id", self.claim_request_id)
        _validate_non_empty("submitted_at", self.submitted_at)

    @property
    def added(self) -> tuple[TableSpec, ...]:
        return self.plan.added

    def to_dict(self) -> dict[str, Any]:
        return {
            "request_id": self.request_id,
            "claim_request_id": self.claim_request_id,
            "submitted_at": self.submitted_at,
            "plan": self.plan.to_dict(),
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any] | None) -> "PendingTopicChange | None":
        if not isinstance(value, dict):
            return None
        return cls(
            request_id=str(value["request_id"]),
            claim_request_id=str(value["claim_request_id"]),
            submitted_at=str(value["submitted_at"]),
            plan=TopicChangePlan.from_dict(value.get("plan") or {}),
        )


@dataclass(frozen=True, slots=True)
class Materialization:
    materialization_id: str
    #: The tables its runtime writes: one per source topic, in the order of the topics. An update
    #: can add and remove topics (`services/topic_changes.py`).
    tables: tuple[TableSpec, ...]
    destination_uri: str
    desired_phase: str
    created_at: str
    updated_at: str
    #: The Kubernetes namespace its runtime runs in: the deployment's, taken at creation.
    namespace: str
    runtime_mode: str = "dedicated"
    #: A label: free text, not unique, of no meaning in SQL or storage. Set to the id when none is
    #: given, and changeable at any time.
    name: str | None = None
    #: Its runtime's name, generated from its id when it was created (`<prefix>-<id>`, bounded).
    resolved_runtime_name: str | None = None
    runtime_group: str | None = None
    runtime_app_ref: str | None = None
    created_by: str | None = None
    #: The account that created it, when an account did; `created_by` is that account's email.
    created_by_id: str | None = None
    resource_profile: str | None = None
    storage_size: str = DEFAULT_STORAGE_SIZE
    replicas: int = DEFAULT_REPLICAS
    #: Acknowledge null-value (tombstone) source records without writing a row for them.
    skip_tombstones: bool = False
    #: Let the control plane give its runtime more members while it is far behind its topics, and
    #: take them back once it has caught up (`services/autoscaling.py`). The deployment can turn
    #: autoscaling off for every materialization.
    autoscaling: bool = True
    #: How the tables are partitioned by event time: `hour`, `day`, `month` or `none`. Fixed at
    #: creation.
    partition_granularity: str = DEFAULT_PARTITION_GRANULARITY
    labels: dict[str, str] = field(default_factory=dict)
    annotations: dict[str, str] = field(default_factory=dict)
    spec_version: int = 1
    #: Tables kept, read-only, after their topics were removed: still in their datasets and still
    #: maintained, no longer written. Adding a kept table's topic again writes it again.
    retired_tables: tuple[TableSpec, ...] = ()
    #: A change of topics waiting for its new tables' names to be claimed.
    pending_topic_change: PendingTopicChange | None = None

    def __post_init__(self) -> None:
        _validate_non_empty("materialization_id", self.materialization_id)
        for tables in (self.tables, self.retired_tables):
            if not isinstance(tables, tuple) or not all(isinstance(item, TableSpec) for item in tables):
                raise ValueError("tables must be a tuple of TableSpec")
        validate_table_specs(self.tables, self.retired_tables)
        _validate_non_empty("destination_uri", self.destination_uri)
        _validate_non_empty("created_at", self.created_at)
        _validate_non_empty("updated_at", self.updated_at)
        _validate_choice("desired_phase", self.desired_phase, LIFECYCLE_PHASES)
        _validate_choice("runtime_mode", self.runtime_mode, RUNTIME_MODES)
        normalize_storage_size(self.storage_size)
        normalize_replicas(self.replicas)
        if not isinstance(self.skip_tombstones, bool):
            raise ValueError("skip_tombstones must be a boolean")
        if not isinstance(self.autoscaling, bool):
            raise ValueError("autoscaling must be a boolean")
        _validate_choice("partition_granularity", self.partition_granularity, PARTITION_GRANULARITIES)
        if self.namespace == "":
            raise ValueError("namespace is required")
        if self.spec_version < 1:
            raise ValueError("spec_version must be >= 1")

    @property
    def label(self) -> str:
        """What people call it: its name, or its id when it has none."""
        return self.name or self.materialization_id

    @property
    def source_topics(self) -> tuple[str, ...]:
        return tuple(table.source_topic for table in self.tables)

    @property
    def table_names(self) -> tuple[str, ...]:
        return tuple(table.name for table in self.tables)

    @property
    def all_tables(self) -> tuple[TableSpec, ...]:
        """Every table in the catalog: the ones its runtime writes, then the kept ones."""
        return (*self.tables, *self.retired_tables)

    @property
    def dataset_ids(self) -> tuple[str, ...]:
        """The datasets its tables are in, written and kept, each once."""
        return tuple(dict.fromkeys(table.dataset_id for table in self.all_tables))

    def table(self, name: str) -> TableSpec | None:
        """The written table called `name`, compared without regard to case as SQL does."""
        wanted = name.lower()
        return next((table for table in self.tables if table.name.lower() == wanted), None)

    def any_table(self, name: str) -> TableSpec | None:
        """The table called `name`, written or kept, compared without regard to case."""
        wanted = name.lower()
        return next((table for table in self.all_tables if table.name.lower() == wanted), None)

    def to_dict(self) -> dict[str, Any]:
        return {
            "materialization_id": self.materialization_id,
            "name": self.name,
            "tables": [table.to_dict() for table in self.tables],
            "source_topics": list(self.source_topics),
            "destination_uri": self.destination_uri,
            "runtime_mode": self.runtime_mode,
            "runtime_group": self.runtime_group,
            "resolved_runtime_name": self.resolved_runtime_name,
            "runtime_app_ref": self.runtime_app_ref,
            "namespace": self.namespace,
            "desired_phase": self.desired_phase,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "created_by": self.created_by,
            "created_by_id": self.created_by_id,
            "resource_profile": self.resource_profile,
            "storage_size": self.storage_size,
            "replicas": self.replicas,
            "skip_tombstones": self.skip_tombstones,
            "autoscaling": self.autoscaling,
            "partition_granularity": self.partition_granularity,
            "labels": dict(self.labels),
            "annotations": dict(self.annotations),
            "spec_version": self.spec_version,
            "retired_tables": [table.to_dict() for table in self.retired_tables],
            "pending_topic_change": None if self.pending_topic_change is None else self.pending_topic_change.to_dict(),
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "Materialization":
        return cls(
            materialization_id=value["materialization_id"],
            name=value.get("name"),
            tables=_specs(value.get("tables")),
            destination_uri=value["destination_uri"],
            runtime_mode=value.get("runtime_mode", "dedicated"),
            runtime_group=value.get("runtime_group"),
            resolved_runtime_name=value.get("resolved_runtime_name"),
            runtime_app_ref=value.get("runtime_app_ref"),
            namespace=value["namespace"],
            desired_phase=value["desired_phase"],
            created_at=value["created_at"],
            updated_at=value["updated_at"],
            created_by=value.get("created_by"),
            created_by_id=value.get("created_by_id"),
            resource_profile=value.get("resource_profile"),
            storage_size=normalize_storage_size(value.get("storage_size")),
            replicas=normalize_replicas(value.get("replicas")),
            skip_tombstones=value.get("skip_tombstones", False),
            # Created before 4.4, when there was no autoscaling to choose: it has it, as every one does.
            autoscaling=value.get("autoscaling", True),
            partition_granularity=value.get("partition_granularity") or DEFAULT_PARTITION_GRANULARITY,
            # A record from before 4.3 may still say `live_tail`; every materialization has one now.
            labels=dict(value.get("labels", {})),
            annotations=dict(value.get("annotations", {})),
            spec_version=value.get("spec_version", 1),
            retired_tables=_specs(value.get("retired_tables")),
            pending_topic_change=PendingTopicChange.from_dict(value.get("pending_topic_change")),
        )


@dataclass(frozen=True, slots=True)
class MaterializationSchema:
    """The control plane's read-only projection of one of a table's Iceberg schemas (spec §9.3).

    The table is the truth; this record is what the API answers from without opening it. It is
    written at discovery (schema 0) and then from each `schema_changed` table event. Each of a
    materialization's tables has schemas of its own; `table` names which.
    """

    materialization_id: str
    table: str
    schema_id: int
    since: str
    fields: list[dict[str, Any]] = field(default_factory=list)
    field_count: int = 0
    fork_map: dict[str, Any] = field(default_factory=dict)
    #: The Iceberg schema itself, as Iceberg serializes it (`?raw=true`).
    iceberg_schema: dict[str, Any] = field(default_factory=dict)
    metadata_location: str | None = None
    #: What changed from the previous schema: `added`, `forked`, `collapsed`, `rejected`.
    changes: list[dict[str, Any]] = field(default_factory=list)
    last_column_id: int | None = None
    #: Logical paths the policy refused at the field limit; writers stop asking for them.
    rejected_paths: list[list[str]] = field(default_factory=list)

    def __post_init__(self) -> None:
        _validate_non_empty("materialization_id", self.materialization_id)
        _validate_non_empty("table", self.table)
        if self.schema_id < 0:
            raise ValueError("schema_id must be >= 0")
        _validate_non_empty("since", self.since)

    def to_dict(self) -> dict[str, Any]:
        return {
            "materialization_id": self.materialization_id,
            "table": self.table,
            "schema_id": self.schema_id,
            "since": self.since,
            "metadata_location": self.metadata_location,
            "field_count": self.field_count,
            "fields": [dict(item) for item in self.fields],
            "fork_map": dict(self.fork_map),
            "iceberg_schema": dict(self.iceberg_schema),
            "changes": [dict(item) for item in self.changes],
            "last_column_id": self.last_column_id,
            "rejected_paths": [list(item) for item in self.rejected_paths],
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "MaterializationSchema":
        return cls(
            materialization_id=value["materialization_id"],
            table=str(value["table"]),
            schema_id=int(value["schema_id"]),
            since=value["since"],
            metadata_location=value.get("metadata_location"),
            field_count=int(value.get("field_count") or 0),
            fields=[dict(item) for item in value.get("fields") or []],
            fork_map=dict(value.get("fork_map") or {}),
            iceberg_schema=dict(value.get("iceberg_schema") or {}),
            changes=[dict(item) for item in value.get("changes") or []],
            last_column_id=value.get("last_column_id"),
            rejected_paths=[list(item) for item in value.get("rejected_paths") or []],
        )
