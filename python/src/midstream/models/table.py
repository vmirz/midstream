"""The control plane's record of each of a materialization's Iceberg tables, and the table events it reads.

A materialization has one table per source topic, each in a dataset of its own choosing. Each `MaterializationTable` is created by the
`create_table` step and kept current by the `table-events` agent from what the runtime's committer
publishes. The table itself (its pointer and metadata in the bucket) is the truth; these records are
what the API and the console's Overview answer from.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import Any

TABLE_EVENT_KINDS = (
    "schema_changed",
    "table_committed",
    "maintenance_started",
    "maintenance_completed",
    "table_error",
    "plaintext_counted",
)
MAINTENANCE_JOBS = ("compaction", "expire_snapshots", "orphan_cleanup", "reprotect")
MAINTENANCE_HISTORY_LIMIT = 50


def _required(name: str, value: Any) -> None:
    if value is None or value == "":
        raise ValueError(f"{name} is required")


@dataclass(frozen=True, slots=True)
class MaintenanceRun:
    """One finished maintenance job, as the maintenance app (before 2.0, the committer) reported it."""

    job: str
    completed_at: str
    outcome: str = "succeeded"
    duration_ms: int | None = None
    snapshot_id: int | None = None
    counts: dict[str, Any] = field(default_factory=dict)
    message: str | None = None
    #: The compacted partition, for a compaction.
    partition: str | None = None
    #: The table event that reported it, so a redelivered event is not recorded twice.
    event_id: str | None = None
    #: When the job started, for a long one (`RunningMaintenance.started_at`).
    started_at: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "job": self.job,
            "completed_at": self.completed_at,
            "outcome": self.outcome,
            "duration_ms": self.duration_ms,
            "snapshot_id": self.snapshot_id,
            "counts": dict(self.counts),
            "message": self.message,
            "partition": self.partition,
            "event_id": self.event_id,
            "started_at": self.started_at,
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "MaintenanceRun":
        return cls(
            job=str(value["job"]),
            completed_at=str(value["completed_at"]),
            outcome=str(value.get("outcome") or "succeeded"),
            duration_ms=value.get("duration_ms"),
            snapshot_id=value.get("snapshot_id"),
            counts=dict(value.get("counts") or {}),
            message=value.get("message"),
            partition=value.get("partition"),
            event_id=value.get("event_id"),
            started_at=value.get("started_at"),
        )


@dataclass(frozen=True, slots=True)
class RunningMaintenance:
    """The maintenance job running on a table, as the maintenance app reported starting it."""

    job: str
    started_at: str
    #: The partition, or partitions, a compaction rewrites.
    partition: str | None = None
    attempt: int = 1
    #: The maintenance app's member that runs it.
    member_id: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "job": self.job,
            "started_at": self.started_at,
            "partition": self.partition,
            "attempt": self.attempt,
            "member_id": self.member_id,
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any] | None) -> "RunningMaintenance | None":
        if not isinstance(value, dict) or not value.get("job") or not value.get("started_at"):
            return None
        return cls(
            job=str(value["job"]),
            started_at=str(value["started_at"]),
            partition=value.get("partition"),
            attempt=int(value.get("attempt") or 1),
            member_id=value.get("member_id"),
        )


@dataclass(frozen=True, slots=True)
class ReprotectRequest:
    """Someone asked for a table's rows already written to be masked as its protection masks them now
    (`reprotect`, handbook 22 §22.15).

    Kept on the table's record and carried by every tick until the maintenance app reports it done,
    so a restart of either loses nothing.
    """

    request_id: str
    requested_at: str
    requested_by: str | None = None
    #: The protection revision the table must have recorded first - an edit's, which takes seconds
    #: to reach it; 0 for none.
    revision: int = 0
    #: Once done, expire the older snapshots and delete the files only they referenced.
    purge: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "request_id": self.request_id,
            "requested_at": self.requested_at,
            "requested_by": self.requested_by,
            "revision": self.revision,
            "purge": self.purge,
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any] | None) -> "ReprotectRequest | None":
        if not isinstance(value, dict) or not value.get("request_id"):
            return None
        return cls(
            request_id=str(value["request_id"]),
            requested_at=str(value.get("requested_at") or ""),
            requested_by=value.get("requested_by"),
            revision=int(value.get("revision") or 0),
            purge=bool(value.get("purge", False)),
        )


@dataclass(frozen=True, slots=True)
class PlaintextReport:
    """What a table holds in plain text that its protection revision masks, as the maintenance app
    last counted it from the table's manifests (`lakehouse/reprotect.py::plan_reprotect`)."""

    revision: int
    counted_at: str
    snapshot_id: int | None = None
    #: Files a reprotect would rewrite, their rows and bytes.
    files: int = 0
    rows: int = 0
    bytes: int = 0
    #: Files it cannot rewrite yet, and why.
    blocked_files: int = 0
    blocked: tuple[str, ...] = ()
    #: Files whose masking under some earlier revision the record no longer says.
    unknown_files: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "revision": self.revision,
            "counted_at": self.counted_at,
            "snapshot_id": self.snapshot_id,
            "files": self.files,
            "rows": self.rows,
            "bytes": self.bytes,
            "blocked_files": self.blocked_files,
            "blocked": list(self.blocked),
            "unknown_files": self.unknown_files,
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any] | None) -> "PlaintextReport | None":
        if not isinstance(value, dict) or value.get("revision") is None:
            return None
        return cls(
            revision=int(value["revision"]),
            counted_at=str(value.get("counted_at") or ""),
            snapshot_id=value.get("snapshot_id"),
            files=int(value.get("files") or 0),
            rows=int(value.get("rows") or 0),
            bytes=int(value.get("bytes") or 0),
            blocked_files=int(value.get("blocked_files") or 0),
            blocked=tuple(str(item) for item in value.get("blocked") or ()),
            unknown_files=int(value.get("unknown_files") or 0),
        )


@dataclass(frozen=True, slots=True)
class TableError:
    reason: str
    message: str
    occurred_at: str

    def to_dict(self) -> dict[str, Any]:
        return {"reason": self.reason, "message": self.message, "occurred_at": self.occurred_at}

    @classmethod
    def from_dict(cls, value: dict[str, Any] | None) -> "TableError | None":
        if not isinstance(value, dict) or not value.get("reason"):
            return None
        return cls(
            reason=str(value["reason"]),
            message=str(value.get("message") or ""),
            occurred_at=str(value.get("occurred_at") or ""),
        )


#: A table's place in its materialization: written by the runtime; kept read-only after its topic
#: was removed; being dropped, after its topic was removed or as a kept table, until it is gone; or
#: dropped with destination cleanup off, which leaves its files where they were. Such a record stays
#: so that its name, and so its location, is never used again: a table created there would count
#: the dropped table's files as orphans and delete them.
TABLE_STATES = ("active", "retired", "dropping", "dropped")


@dataclass(frozen=True, slots=True)
class MaterializationTable:
    materialization_id: str
    #: The table's dataset - its Iceberg namespace - by name: SQL reads it as `<dataset>.<name>`.
    dataset: str
    #: The dataset's id in the directory.
    dataset_id: str
    #: The table's name in its dataset, chosen when its topic was picked.
    name: str
    #: The Kafka topic the table holds.
    source_topic: str
    location: str
    table_uuid: str
    partition_granularity: str
    created_at: str
    updated_at: str
    metadata_location: str | None = None
    current_snapshot_id: int | None = None
    sequence_number: int | None = None
    last_commit_at: str | None = None
    last_commit_duration_ms: int | None = None
    totals: dict[str, int] = field(default_factory=dict)
    offsets: dict[str, int] = field(default_factory=dict)
    valid_through: str | None = None
    snapshots: int | None = None
    schema_id: int | None = None
    maintenance: tuple[MaintenanceRun, ...] = ()
    last_error: TableError | None = None
    #: One of `TABLE_STATES`.
    state: str = "active"
    #: The maintenance job running on it now, if any: set when the maintenance app starts one, and
    #: cleared when it reports the job done - or abandoned, after the app restarted.
    maintenance_job: RunningMaintenance | None = None
    #: A reprotect asked for and not done yet.
    reprotect: ReprotectRequest | None = None
    #: What the table holds in plain text that its protection masks, as last counted.
    plaintext: PlaintextReport | None = None

    def __post_init__(self) -> None:
        for name in ("materialization_id", "dataset", "dataset_id", "name", "source_topic", "location", "table_uuid", "created_at", "updated_at"):
            _required(name, getattr(self, name))
        if self.state not in TABLE_STATES:
            raise ValueError(f"state must be one of: {', '.join(TABLE_STATES)}")

    @property
    def namespace(self) -> str:
        """The table's Iceberg namespace: its dataset."""
        return self.dataset

    @property
    def identifier(self) -> tuple[str, str]:
        return (self.dataset, self.name)

    @property
    def qualified_name(self) -> str:
        """`<dataset>.<table>`: what SQL reads the table by."""
        return f"{self.dataset}.{self.name}"

    @property
    def error_holds(self) -> bool:
        """Whether its open error holds the materialization degraded: so it does while the table is
        written, and while dropping it fails. A kept or dropped table's does not."""
        return self.last_error is not None and self.state in {"active", "dropping"}

    def with_maintenance(self, run: MaintenanceRun, *, updated_at: str) -> "MaterializationTable":
        history = (run, *self.maintenance)[:MAINTENANCE_HISTORY_LIMIT]
        return replace(self, maintenance=history, updated_at=updated_at)

    def to_dict(self) -> dict[str, Any]:
        return {
            "materialization_id": self.materialization_id,
            "dataset": self.dataset,
            "dataset_id": self.dataset_id,
            "name": self.name,
            "source_topic": self.source_topic,
            "location": self.location,
            "table_uuid": self.table_uuid,
            "partition_granularity": self.partition_granularity,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "metadata_location": self.metadata_location,
            "current_snapshot_id": self.current_snapshot_id,
            "sequence_number": self.sequence_number,
            "last_commit_at": self.last_commit_at,
            "last_commit_duration_ms": self.last_commit_duration_ms,
            "totals": dict(self.totals),
            "offsets": dict(self.offsets),
            "valid_through": self.valid_through,
            "snapshots": self.snapshots,
            "schema_id": self.schema_id,
            "maintenance": [item.to_dict() for item in self.maintenance],
            "last_error": None if self.last_error is None else self.last_error.to_dict(),
            "state": self.state,
            "maintenance_job": None if self.maintenance_job is None else self.maintenance_job.to_dict(),
            "reprotect": None if self.reprotect is None else self.reprotect.to_dict(),
            "plaintext": None if self.plaintext is None else self.plaintext.to_dict(),
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "MaterializationTable":
        return cls(
            materialization_id=value["materialization_id"],
            dataset=value["dataset"],
            dataset_id=value["dataset_id"],
            name=value["name"],
            source_topic=value["source_topic"],
            location=value["location"],
            table_uuid=value["table_uuid"],
            partition_granularity=value.get("partition_granularity") or "day",
            created_at=value["created_at"],
            updated_at=value["updated_at"],
            metadata_location=value.get("metadata_location"),
            current_snapshot_id=value.get("current_snapshot_id"),
            sequence_number=value.get("sequence_number"),
            last_commit_at=value.get("last_commit_at"),
            last_commit_duration_ms=value.get("last_commit_duration_ms"),
            totals={str(key): int(item) for key, item in (value.get("totals") or {}).items()},
            offsets={str(key): int(item) for key, item in (value.get("offsets") or {}).items()},
            valid_through=value.get("valid_through"),
            snapshots=value.get("snapshots"),
            schema_id=value.get("schema_id"),
            maintenance=tuple(MaintenanceRun.from_dict(item) for item in value.get("maintenance") or []),
            last_error=TableError.from_dict(value.get("last_error")),
            state=value.get("state") or "active",
            maintenance_job=RunningMaintenance.from_dict(value.get("maintenance_job")),
            reprotect=ReprotectRequest.from_dict(value.get("reprotect")),
            plaintext=PlaintextReport.from_dict(value.get("plaintext")),
        )


@dataclass(frozen=True, slots=True)
class TableEvent:
    """What a runtime's committer, or the maintenance app, did to a table (`materialization-table-events`, spec §9.2)."""

    event_id: str
    kind: str
    materialization_id: str
    occurred_at: str
    #: The table it happened to, by name.
    table: str
    runtime_app_ref: str | None = None
    member_id: str | None = None
    payload: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for name in ("event_id", "materialization_id", "occurred_at", "table"):
            _required(name, getattr(self, name))
        if self.kind not in TABLE_EVENT_KINDS:
            raise ValueError(f"kind must be one of: {', '.join(TABLE_EVENT_KINDS)}")

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "kind": self.kind,
            "materialization_id": self.materialization_id,
            "occurred_at": self.occurred_at,
            "table": self.table,
            "runtime_app_ref": self.runtime_app_ref,
            "member_id": self.member_id,
            "payload": dict(self.payload),
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "TableEvent":
        return cls(
            event_id=str(value["event_id"]),
            kind=str(value["kind"]),
            materialization_id=str(value["materialization_id"]),
            occurred_at=str(value["occurred_at"]),
            table=str(value.get("table") or ""),
            runtime_app_ref=value.get("runtime_app_ref"),
            member_id=value.get("member_id"),
            payload=dict(value.get("payload") or {}),
        )
