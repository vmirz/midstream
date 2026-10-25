"""Kafka offsets in snapshot summaries: how far each source partition is in the table (spec §7.6).

Every snapshot the committer writes carries, in its summary, the full map of committed offsets:
for each source partition the inclusive last offset such that every record at or below it is in
the table or needs no row (a tombstone skipped, an offset compaction removed). That map is the
runtime's checkpoint: members start their sessions from it, release acknowledgements by it, and the
committer judges every new file against it. It lives in the table, so it can never disagree with
the data, and it only moves forward.

Maintenance snapshots (compaction, expiry) carry the map forward unchanged; a snapshot without one
(an external writer's) is skipped over by walking back to its parent.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
import json
from typing import Any

from pyiceberg.table.metadata import TableMetadata

OFFSETS_PROPERTY = "materializer.offsets"
EVENT_TIMES_PROPERTY = "materializer.event-times"
VALID_THROUGH_PROPERTY = "materializer.valid-through"
COMMIT_ID_PROPERTY = "materializer.commit-id"
COMMITTED_AT_PROPERTY = "materializer.committed-at"
FILES_PROPERTY = "materializer.files"
RECORDS_PROPERTY = "materializer.records"
RUNTIME_PROPERTY = "materializer.runtime"
OPERATION_PROPERTY = "materializer.operation"

#: How far back to walk for a snapshot that carries offsets before giving up.
MAX_ANCESTOR_WALK = 10_000


@dataclass(frozen=True, slots=True)
class CommittedOffsets:
    """The table's committed offsets as of one snapshot."""

    snapshot_id: int | None = None
    offsets: dict[int, int] = field(default_factory=dict)
    #: Per partition, the latest committed record time, epoch milliseconds.
    event_times: dict[int, int] = field(default_factory=dict)
    committed_at: str | None = None

    def get(self, partition: int) -> int:
        """The partition's committed offset; -1 when nothing of it is in the table yet."""
        return self.offsets.get(partition, -1)

    def covers(self, partition: int, offset: int) -> bool:
        return offset <= self.get(partition)

    @property
    def valid_through(self) -> str | None:
        """The instant before which every partition that ever committed is in the table."""
        if not self.event_times:
            return None
        return format_millis(min(self.event_times.values()))

    def advanced(self, offsets: dict[int, int], event_times: dict[int, int]) -> "CommittedOffsets":
        merged = dict(self.offsets)
        for partition, offset in offsets.items():
            merged[partition] = max(offset, merged.get(partition, -1))
        times = dict(self.event_times)
        for partition, moment in event_times.items():
            times[partition] = max(moment, times.get(partition, moment))
        return CommittedOffsets(snapshot_id=None, offsets=merged, event_times=times, committed_at=self.committed_at)

    def summary_properties(self) -> dict[str, str]:
        properties = {
            OFFSETS_PROPERTY: json.dumps({str(key): value for key, value in sorted(self.offsets.items())}, separators=(",", ":")),
            EVENT_TIMES_PROPERTY: json.dumps({str(key): value for key, value in sorted(self.event_times.items())}, separators=(",", ":")),
        }
        if self.valid_through is not None:
            properties[VALID_THROUGH_PROPERTY] = self.valid_through
        return properties

    def to_dict(self) -> dict[str, Any]:
        return {
            "snapshot_id": self.snapshot_id,
            "offsets": {str(key): value for key, value in sorted(self.offsets.items())},
            "event_times": {str(key): value for key, value in sorted(self.event_times.items())},
            "valid_through": self.valid_through,
            "committed_at": self.committed_at,
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any] | None) -> "CommittedOffsets":
        if not isinstance(value, dict):
            return cls()
        return cls(
            snapshot_id=value.get("snapshot_id"),
            offsets={int(key): int(offset) for key, offset in (value.get("offsets") or {}).items()},
            event_times={int(key): int(moment) for key, moment in (value.get("event_times") or {}).items()},
            committed_at=value.get("committed_at"),
        )


def offsets_from_summary(summary: dict[str, str]) -> CommittedOffsets | None:
    """The offsets a snapshot summary carries, or None when it carries none."""
    raw = summary.get(OFFSETS_PROPERTY)
    if raw is None:
        return None
    offsets = {int(key): int(value) for key, value in json.loads(raw).items()}
    raw_times = summary.get(EVENT_TIMES_PROPERTY)
    event_times = {} if raw_times is None else {int(key): int(value) for key, value in json.loads(raw_times).items()}
    return CommittedOffsets(offsets=offsets, event_times=event_times, committed_at=summary.get(COMMITTED_AT_PROPERTY))


def committed_offsets(metadata: TableMetadata) -> CommittedOffsets:
    """The committed offsets as of the table's current snapshot (walking back past any without)."""
    snapshot = metadata.current_snapshot()
    walked = 0
    while snapshot is not None and walked < MAX_ANCESTOR_WALK:
        summary = {} if snapshot.summary is None else dict(snapshot.summary.additional_properties)
        found = offsets_from_summary(summary)
        if found is not None:
            return CommittedOffsets(
                snapshot_id=metadata.current_snapshot_id,
                offsets=found.offsets,
                event_times=found.event_times,
                committed_at=found.committed_at,
            )
        if snapshot.parent_snapshot_id is None:
            break
        snapshot = metadata.snapshot_by_id(snapshot.parent_snapshot_id)
        walked += 1
    return CommittedOffsets(snapshot_id=metadata.current_snapshot_id)


def parse_millis(value: str | None) -> int | None:
    """ISO-8601 (`...Z` or an offset) to epoch milliseconds; None for None or garbage."""
    if not value:
        return None
    text = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        moment = datetime.fromisoformat(text)
    except ValueError:
        return None
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=UTC)
    return int(moment.timestamp() * 1000)


def format_millis(value: int) -> str:
    return datetime.fromtimestamp(value / 1000, tz=UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")
