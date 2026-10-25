"""What the control plane tells the tail app (`materializer-tail-requests`).

The tail app serves a table's newest records from Kafka before they are committed - its live tail
(handbook 19). It keeps nothing durable: every minute the control plane names each table, and that
is all the app knows about which tables exist and how to tail them:

- `tail_tick`, for every table its runtime writes of a materialization that is running and has live
  tail on, says where the table is, which topic it holds, the live-tail settings as they are now, and
  hints from the table's record - the committed offsets, the schema id and the metadata location the
  control plane last heard of;
- `tail_cancel` stops the tail of a table that no longer qualifies: being dropped, kept read-only,
  of a materialization being deleted or stopped, or with live tail turned off.

Every message about a table has the same key (`live_tail_routing_key`), so it lands in one partition
of the topic and one member of the tail app - the one Kaspr gives that partition to - owns the table.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

LIVE_TAIL_MESSAGE_KINDS = ("tail_tick", "tail_cancel")
#: Why a table's tail is stopped.
LIVE_TAIL_CANCEL_REASONS = (
    "table_dropping",
    "table_retired",
    "materialization_deleting",
    "materialization_stopped",
    # Only a control plane before 4.3 sends it, for a materialization whose live tail is off.
    "live_tail_off",
)


def live_tail_routing_key(materialization_id: str, table: str) -> str:
    """The key of every message about one table, so they all reach the member that tails it."""
    return f"tail:{materialization_id}:{table}"


@dataclass(frozen=True, slots=True)
class LiveTailTarget:
    """A table to tail: whose it is, where it is, and the topic it holds."""

    materialization_id: str
    #: The table's dataset by name - its Iceberg namespace - and by id, which access is granted on.
    dataset: str
    dataset_id: str
    table: str
    location: str
    source_topic: str
    partition_granularity: str = "day"
    #: Which incarnation of the name: a table dropped and created again has another.
    table_uuid: str | None = None
    #: The materialization's: a tombstone is then no row, for the tail as for the runtime.
    skip_tombstones: bool = False

    def __post_init__(self) -> None:
        for name in ("materialization_id", "dataset", "dataset_id", "table", "location", "source_topic"):
            if not getattr(self, name):
                raise ValueError(f"{name} is required")
        if not isinstance(self.skip_tombstones, bool):
            raise ValueError("skip_tombstones must be a boolean")

    @property
    def key(self) -> str:
        return live_tail_routing_key(self.materialization_id, self.table)

    def to_dict(self) -> dict[str, Any]:
        return {
            "materialization_id": self.materialization_id,
            "dataset": self.dataset,
            "dataset_id": self.dataset_id,
            "table": self.table,
            "location": self.location,
            "source_topic": self.source_topic,
            "partition_granularity": self.partition_granularity,
            "table_uuid": self.table_uuid,
            "skip_tombstones": self.skip_tombstones,
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "LiveTailTarget":
        return cls(
            materialization_id=str(value["materialization_id"]),
            dataset=str(value["dataset"]),
            dataset_id=str(value["dataset_id"]),
            table=str(value["table"]),
            location=str(value["location"]),
            source_topic=str(value["source_topic"]),
            partition_granularity=str(value.get("partition_granularity") or "day"),
            table_uuid=str(value["table_uuid"]) if value.get("table_uuid") else None,
            skip_tombstones=bool(value.get("skip_tombstones", False)),
        )


@dataclass(frozen=True, slots=True)
class LiveTailHints:
    """What the table's record said when the tick was sent: never ahead of the table, maybe behind it."""

    #: Kafka partition (as text) -> the last offset in the table, inclusive.
    offsets: dict[str, int] = field(default_factory=dict)
    schema_id: int | None = None
    metadata_location: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {"offsets": dict(self.offsets), "schema_id": self.schema_id, "metadata_location": self.metadata_location}

    @classmethod
    def from_dict(cls, value: dict[str, Any] | None) -> "LiveTailHints":
        value = value or {}
        schema_id = value.get("schema_id")
        return cls(
            offsets={str(partition): int(offset) for partition, offset in (value.get("offsets") or {}).items()},
            schema_id=None if schema_id is None else int(schema_id),
            metadata_location=value.get("metadata_location"),
        )


@dataclass(frozen=True, slots=True)
class LiveTailMessage:
    kind: str
    target: LiveTailTarget
    issued_at: str
    #: Tick: the live-tail settings as resolved this minute, overrides included.
    settings: dict[str, Any] = field(default_factory=dict)
    #: Tick: what the table's record knows of it.
    hints: LiveTailHints | None = None
    #: Cancel: why (`LIVE_TAIL_CANCEL_REASONS`).
    reason: str | None = None

    def __post_init__(self) -> None:
        if self.kind not in LIVE_TAIL_MESSAGE_KINDS:
            raise ValueError(f"kind must be one of: {', '.join(LIVE_TAIL_MESSAGE_KINDS)}")
        if not self.issued_at:
            raise ValueError("issued_at is required")
        if self.kind == "tail_cancel" and self.reason not in LIVE_TAIL_CANCEL_REASONS:
            raise ValueError(f"reason must be one of: {', '.join(LIVE_TAIL_CANCEL_REASONS)}")

    @property
    def key(self) -> str:
        return self.target.key

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {"kind": self.kind, "target": self.target.to_dict(), "issued_at": self.issued_at}
        if self.settings:
            payload["settings"] = dict(self.settings)
        if self.hints is not None:
            payload["hints"] = self.hints.to_dict()
        if self.reason is not None:
            payload["reason"] = self.reason
        return payload

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "LiveTailMessage":
        return cls(
            kind=str(value["kind"]),
            target=LiveTailTarget.from_dict(value["target"]),
            issued_at=str(value.get("issued_at") or ""),
            settings=dict(value.get("settings") or {}),
            hints=None if value.get("hints") is None else LiveTailHints.from_dict(value.get("hints")),
            reason=value.get("reason"),
        )
