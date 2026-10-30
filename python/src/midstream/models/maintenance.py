"""What the control plane tells the maintenance app (`materializer-maintenance-requests`).

The maintenance app keeps every materialization's tables healthy - compaction, snapshot expiry,
orphan cleanup - whatever its runtime is doing, stopped included. It holds no records of its own:

- `maintenance_tick`, every minute for each table that is written or kept, says where the table is,
  the maintenance settings as they are now, when each job last finished (from the table record),
  which job the record says is running, the protection revision the table recorded, and a reprotect
  asked for and not done yet;
- `maintenance_request` asks for a job now (`POST .../maintenance`);
- `maintenance_cancel` stops whatever runs on a table that is being dropped.

Every message about a table has the same key (`maintenance_routing_key`), so one member of the app
handles each table.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from midstream.models.table import MAINTENANCE_JOBS, ReprotectRequest, RunningMaintenance

MAINTENANCE_MESSAGE_KINDS = ("maintenance_tick", "maintenance_request", "maintenance_cancel")
#: The tables the maintenance app maintains: those its runtime writes, and those kept read-only.
MAINTAINED_TABLE_STATES = ("active", "retired")


def maintenance_routing_key(materialization_id: str, table: str) -> str:
    """The key of every message about one table, so they all reach the member that maintains it."""
    return f"maintenance:{materialization_id}:{table}"


@dataclass(frozen=True, slots=True)
class MaintenanceTarget:
    """A table to maintain: whose it is and where it is."""

    materialization_id: str
    namespace: str
    table: str
    location: str
    source_topic: str
    partition_granularity: str = "day"
    #: `active`, written by its runtime; `retired`, kept read-only after its topic was removed.
    state: str = "active"

    def __post_init__(self) -> None:
        for name in ("materialization_id", "namespace", "table", "location", "source_topic"):
            if not getattr(self, name):
                raise ValueError(f"{name} is required")

    @property
    def key(self) -> str:
        return maintenance_routing_key(self.materialization_id, self.table)

    def to_dict(self) -> dict[str, Any]:
        return {
            "materialization_id": self.materialization_id,
            "namespace": self.namespace,
            "table": self.table,
            "location": self.location,
            "source_topic": self.source_topic,
            "partition_granularity": self.partition_granularity,
            "state": self.state,
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "MaintenanceTarget":
        return cls(
            materialization_id=str(value["materialization_id"]),
            namespace=str(value["namespace"]),
            table=str(value["table"]),
            location=str(value["location"]),
            source_topic=str(value["source_topic"]),
            partition_granularity=str(value.get("partition_granularity") or "day"),
            state=str(value.get("state") or "active"),
        )


@dataclass(frozen=True, slots=True)
class MaintenanceMessage:
    kind: str
    target: MaintenanceTarget
    issued_at: str
    #: Tick and request: the runtimes' maintenance settings as they are now (`RuntimeAppConfig` fields).
    settings: dict[str, Any] = field(default_factory=dict)
    #: Tick: when each job last finished on the table, as its record's history says.
    last_runs: dict[str, str] = field(default_factory=dict)
    #: Tick: the job the table's record says is running.
    running: RunningMaintenance | None = None
    #: Tick: a reprotect asked for and not done yet (`ReprotectRequest`).
    reprotect: ReprotectRequest | None = None
    #: Tick: the protection revision the table recorded, as the control plane last heard; None when
    #: it records none.
    protection_revision: int | None = None
    #: Request: the job asked for.
    job: str | None = None
    request_id: str | None = None
    requested_by: str | None = None
    #: Cancel: why.
    reason: str | None = None

    def __post_init__(self) -> None:
        if self.kind not in MAINTENANCE_MESSAGE_KINDS:
            raise ValueError(f"kind must be one of: {', '.join(MAINTENANCE_MESSAGE_KINDS)}")
        if not self.issued_at:
            raise ValueError("issued_at is required")
        if self.kind == "maintenance_request" and self.job not in MAINTENANCE_JOBS:
            raise ValueError(f"job must be one of: {', '.join(MAINTENANCE_JOBS)}")

    @property
    def key(self) -> str:
        return self.target.key

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "kind": self.kind,
            "target": self.target.to_dict(),
            "issued_at": self.issued_at,
        }
        if self.settings:
            payload["settings"] = dict(self.settings)
        if self.last_runs:
            payload["last_runs"] = dict(self.last_runs)
        if self.running is not None:
            payload["running"] = self.running.to_dict()
        if self.reprotect is not None:
            payload["reprotect"] = self.reprotect.to_dict()
        if self.protection_revision is not None:
            payload["protection_revision"] = self.protection_revision
        for name in ("job", "request_id", "requested_by", "reason"):
            if getattr(self, name) is not None:
                payload[name] = getattr(self, name)
        return payload

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "MaintenanceMessage":
        return cls(
            kind=str(value["kind"]),
            target=MaintenanceTarget.from_dict(value["target"]),
            issued_at=str(value.get("issued_at") or ""),
            settings=dict(value.get("settings") or {}),
            last_runs={str(job): str(at) for job, at in (value.get("last_runs") or {}).items()},
            running=RunningMaintenance.from_dict(value.get("running")),
            reprotect=ReprotectRequest.from_dict(value.get("reprotect")),
            protection_revision=None if value.get("protection_revision") is None else int(value["protection_revision"]),
            job=value.get("job"),
            request_id=value.get("request_id"),
            requested_by=value.get("requested_by"),
            reason=value.get("reason"),
        )
