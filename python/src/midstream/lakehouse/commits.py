"""The committer's decisions and commits: exactly once, by construction (spec §7.6, §7.8).

Members announce sealed, uploaded files; the committer decides which to commit. A batch's files
(one per partition day) are a *seal*, decided as a unit. Each seal says where it continues its
source partition from: `after_offset`, the offset below which its member found the partition
already in the table (its session's starting point, or its own previous seal's end). Every record in
`(after_offset, offset_end]` is in the seal's files or needs no row.

Against the table's committed offset `c` of the seal's partition:

- `offset_end <= c`: **covered** - another seal already put these records in; the files are deleted.
- `offset_start <= c < offset_end`: **overlap** - part is in already; the member re-seals the rest
  (`c+1` onwards) from its staged rows and announces that instead. The files are deleted.
- `after_offset <= c < offset_start`: **accepted** - the seal continues exactly where the table is.
- `after_offset > c`: a seal between them is missing - typically its announcement was lost with a
  committer that restarted. The seal waits; its member re-announces everything still pending, so
  the gap fills. Accepting it would let `c` jump past records that were never committed.

A record can therefore never be committed twice (covered and overlapping files are rejected), nor
skipped (a gap is never jumped). The committed offsets advance only by accepted seals.
"""

from __future__ import annotations

from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
import logging
from typing import Any
import uuid

from pyiceberg.table import Table
from pyiceberg.table.update import (
    AddSchemaUpdate,
    AssertCurrentSchemaId,
    AssertLastAssignedFieldId,
    SetCurrentSchemaUpdate,
    SetPropertiesUpdate,
    UpdateTableMetadata,
)

from midstream.lakehouse.catalog import commit_once
from midstream.lakehouse.files import build_data_file, read_parquet_footer
from midstream.lakehouse.offsets import (
    COMMIT_ID_PROPERTY,
    COMMITTED_AT_PROPERTY,
    FILES_PROPERTY,
    OPERATION_PROPERTY,
    RECORDS_PROPERTY,
    RUNTIME_PROPERTY,
    CommittedOffsets,
    committed_offsets,
    parse_millis,
)
from midstream.lakehouse.pointer import utc_now
from midstream.lakehouse.schema.forkmap import ForkMap
from midstream.lakehouse.schema.policy import Evolution
from midstream.lakehouse.tables import PROPERTY_FORK_MAP, partition_granularity_of

LOGGER = logging.getLogger(__name__)

ACCEPTED = "accepted"
COVERED = "covered"
OVERLAP = "overlap"
ERROR = "error"
DEFERRED = "deferred"


class MissingDataFileError(FileNotFoundError):
    """An announced data file is not in the bucket (deleted by hand, or never fully uploaded)."""

    def __init__(self, file_path: str) -> None:
        super().__init__(file_path)
        self.file_path = file_path


@dataclass(frozen=True, slots=True)
class AnnouncedFile:
    """One uploaded data file, as its member announced it (`data_file_ready`)."""

    file_path: str
    materialization_id: str
    member_id: str
    batch_id: str
    seal_id: str
    file_count: int
    partition: int
    after_offset: int
    offset_start: int
    offset_end: int
    file_size: int
    record_count: int
    partition_value: dict[str, str | None]
    event_time_min: str | None = None
    event_time_max: str | None = None
    schema_id: int | None = None
    session_generation: int | None = None
    #: The member's process: generations count one process's rebalances and start again at a
    #: restart, so they compare only within one epoch. None from a runtime that predates it.
    session_epoch: str | None = None
    announced_at: str | None = None
    attempt: int = 1
    #: The table the file belongs to: a runtime commits to one table per source topic.
    table: str = ""

    def __post_init__(self) -> None:
        if not self.file_path or not self.seal_id or not self.batch_id:
            raise ValueError("file_path, seal_id and batch_id are required")
        if self.file_count < 1:
            raise ValueError("file_count must be >= 1")
        if not self.after_offset < self.offset_start <= self.offset_end:
            raise ValueError(
                f"offsets must satisfy after_offset < offset_start <= offset_end, got "
                f"{self.after_offset}, {self.offset_start}, {self.offset_end}"
            )

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": "data_file_ready",
            "materialization_id": self.materialization_id,
            "table": self.table,
            "member_id": self.member_id,
            "session_generation": self.session_generation,
            "session_epoch": self.session_epoch,
            "batch_id": self.batch_id,
            "seal_id": self.seal_id,
            "file_count": self.file_count,
            "partition": self.partition,
            "after_offset": self.after_offset,
            "offset_start": self.offset_start,
            "offset_end": self.offset_end,
            "file_path": self.file_path,
            "file_size": self.file_size,
            "record_count": self.record_count,
            "partition_value": dict(self.partition_value),
            "event_time_min": self.event_time_min,
            "event_time_max": self.event_time_max,
            "schema_id": self.schema_id,
            "announced_at": self.announced_at,
            "attempt": self.attempt,
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "AnnouncedFile":
        return cls(
            file_path=str(value["file_path"]),
            materialization_id=str(value.get("materialization_id") or ""),
            member_id=str(value.get("member_id") or ""),
            batch_id=str(value["batch_id"]),
            seal_id=str(value["seal_id"]),
            file_count=int(value.get("file_count") or 1),
            partition=int(value["partition"]),
            after_offset=int(value["after_offset"]),
            offset_start=int(value["offset_start"]),
            offset_end=int(value["offset_end"]),
            file_size=int(value.get("file_size") or 0),
            record_count=int(value.get("record_count") or 0),
            partition_value=dict(value.get("partition_value") or {}),
            event_time_min=value.get("event_time_min"),
            event_time_max=value.get("event_time_max"),
            schema_id=value.get("schema_id"),
            session_generation=value.get("session_generation"),
            session_epoch=None if value.get("session_epoch") is None else str(value["session_epoch"]),
            announced_at=value.get("announced_at"),
            attempt=int(value.get("attempt") or 1),
            table=str(value.get("table") or ""),
        )


@dataclass(slots=True)
class Seal:
    """The files one seal of one batch produced, decided together."""

    seal_id: str
    member_id: str
    batch_id: str
    partition: int
    after_offset: int
    offset_start: int
    offset_end: int
    file_count: int
    files: dict[str, AnnouncedFile] = field(default_factory=dict)
    session_generation: int | None = None
    session_epoch: str | None = None

    @property
    def complete(self) -> bool:
        return len(self.files) >= self.file_count

    @property
    def bytes(self) -> int:
        return sum(item.file_size for item in self.files.values())

    @property
    def records(self) -> int:
        return sum(item.record_count for item in self.files.values())

    @property
    def event_time_max_ms(self) -> int | None:
        values = [parse_millis(item.event_time_max) for item in self.files.values()]
        values = [value for value in values if value is not None]
        return max(values) if values else None


@dataclass(frozen=True, slots=True)
class FileDecision:
    """What the committer decided about one announced file, for its member to act on."""

    file_path: str
    decision: str
    partition: int
    seal_id: str
    batch_id: str
    member_id: str
    offset_end: int
    covered_through: int | None = None
    reason: str | None = None
    decided_at: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "decision": self.decision,
            "partition": self.partition,
            "seal_id": self.seal_id,
            "batch_id": self.batch_id,
            "member_id": self.member_id,
            "offset_end": self.offset_end,
            "covered_through": self.covered_through,
            "reason": self.reason,
            "decided_at": self.decided_at,
        }

    @classmethod
    def from_dict(cls, file_path: str, value: dict[str, Any]) -> "FileDecision":
        return cls(
            file_path=file_path,
            decision=str(value["decision"]),
            partition=int(value.get("partition", -1)),
            seal_id=str(value.get("seal_id") or ""),
            batch_id=str(value.get("batch_id") or ""),
            member_id=str(value.get("member_id") or ""),
            offset_end=int(value.get("offset_end", -1)),
            covered_through=value.get("covered_through"),
            reason=value.get("reason"),
            decided_at=value.get("decided_at"),
        )


class PendingFiles:
    """The committer's in-memory set of announced, undecided files, grouped into seals."""

    def __init__(self) -> None:
        self._seals: dict[str, Seal] = {}
        #: Newest session seen per (member, partition), as (epoch, generation): announcements of an
        #: older session of the same process are stale.
        self._sessions: dict[tuple[str, int], tuple[str | None, int | None]] = {}

    def __len__(self) -> int:
        return sum(len(item.files) for item in self._seals.values())

    def seals(self) -> list[Seal]:
        return list(self._seals.values())

    def paths(self) -> set[str]:
        return {path for item in self._seals.values() for path in item.files}

    @property
    def bytes(self) -> int:
        return sum(item.bytes for item in self._seals.values())

    def session_of(self, member_id: str, partition: int) -> tuple[str | None, int | None] | None:
        """The newest (epoch, generation) announced by the member for the partition; None before any."""
        return self._sessions.get((member_id, partition))

    def add(self, announced: AnnouncedFile) -> bool:
        """Remember an announced file; False when it is stale (an older session of its member's process).

        A generation compares only within its epoch: a member that restarts keeps its name and counts
        its rebalances from nothing again. An announcement of another epoch is the member's next
        process, whatever its generation, and replaces the last one's seals of the partition. The
        next process starts only once the last one has stopped, and the commits topic has one
        partition, so it is heard after everything the last one announced.
        """
        member_id, partition = announced.member_id, announced.partition
        epoch, generation = announced.session_epoch, announced.session_generation
        known = self._sessions.get((member_id, partition))
        if known is None or known[0] != epoch:
            self._sessions[(member_id, partition)] = (epoch, generation)
            if known is not None:
                self._drop_superseded(member_id, partition, epoch, generation)
        elif generation is not None:
            newest = known[1]
            if newest is not None and generation < newest:
                return False
            if newest is None or generation > newest:
                self._sessions[(member_id, partition)] = (epoch, generation)
                self._drop_superseded(member_id, partition, epoch, generation)
        seal = self._seals.get(announced.seal_id)
        if seal is None:
            seal = self._seals[announced.seal_id] = Seal(
                seal_id=announced.seal_id,
                member_id=announced.member_id,
                batch_id=announced.batch_id,
                partition=announced.partition,
                after_offset=announced.after_offset,
                offset_start=announced.offset_start,
                offset_end=announced.offset_end,
                file_count=announced.file_count,
                session_generation=generation,
                session_epoch=epoch,
            )
        seal.files[announced.file_path] = announced
        return True

    def remove(self, seal_ids: Iterable[str]) -> None:
        for seal_id in seal_ids:
            self._seals.pop(seal_id, None)

    def _drop_superseded(self, member_id: str, partition: int, epoch: str | None, generation: int | None) -> None:
        """Forget the member's seals of the partition that its session `(epoch, generation)` replaces:
        another process's, and an older session's of this one."""
        stale = [
            seal_id
            for seal_id, seal in self._seals.items()
            if seal.member_id == member_id
            and seal.partition == partition
            and (
                seal.session_epoch != epoch
                or (
                    generation is not None
                    and seal.session_generation is not None
                    and seal.session_generation < generation
                )
            )
        ]
        self.remove(stale)


@dataclass(frozen=True, slots=True)
class AppendPlan:
    accepted: tuple[Seal, ...]
    decisions: tuple[FileDecision, ...]
    offsets: CommittedOffsets
    deferred: tuple[Seal, ...]

    @property
    def files(self) -> list[AnnouncedFile]:
        return [item for seal in self.accepted for item in seal.files.values()]


def plan_append(
    seals: Iterable[Seal],
    committed: CommittedOffsets,
    *,
    max_files: int,
    max_bytes: int,
    decided_at: str | None = None,
) -> AppendPlan:
    """Decide every complete seal against the committed offsets (see the module docstring)."""
    now = decided_at or utc_now()
    by_partition: dict[int, list[Seal]] = {}
    for seal in seals:
        if seal.complete:
            by_partition.setdefault(seal.partition, []).append(seal)
    accepted: list[Seal] = []
    decisions: list[FileDecision] = []
    deferred: list[Seal] = []
    new_offsets: dict[int, int] = {}
    event_times: dict[int, int] = {}
    files_left = max_files
    bytes_left = max_bytes
    budget_spent = False
    for partition, group in sorted(by_partition.items()):
        cursor = committed.get(partition)
        undecided = sorted(group, key=lambda item: (item.offset_start, -item.offset_end))
        progress = True
        while progress and undecided:
            progress = False
            remaining: list[Seal] = []
            for seal in undecided:
                if seal.offset_end <= cursor:
                    decisions.extend(_decide(seal, COVERED, covered_through=cursor, at=now))
                    progress = True
                elif seal.offset_start <= cursor:
                    decisions.extend(_decide(seal, OVERLAP, covered_through=cursor, at=now))
                    progress = True
                elif seal.after_offset <= cursor:
                    over_budget = accepted and (
                        len(seal.files) > files_left or seal.bytes > bytes_left
                    )
                    if over_budget or budget_spent:
                        budget_spent = True
                        remaining.append(seal)
                        continue
                    accepted.append(seal)
                    files_left -= len(seal.files)
                    bytes_left -= seal.bytes
                    decisions.extend(_decide(seal, ACCEPTED, covered_through=None, at=now))
                    cursor = seal.offset_end
                    new_offsets[partition] = cursor
                    moment = seal.event_time_max_ms
                    if moment is not None:
                        event_times[partition] = max(moment, event_times.get(partition, moment))
                    progress = True
                else:
                    remaining.append(seal)
            undecided = remaining
        deferred.extend(undecided)
    return AppendPlan(
        accepted=tuple(accepted),
        decisions=tuple(decisions),
        offsets=committed.advanced(new_offsets, event_times),
        deferred=tuple(deferred),
    )


def _decide(seal: Seal, decision: str, *, covered_through: int | None, at: str) -> list[FileDecision]:
    return [
        FileDecision(
            file_path=path,
            decision=decision,
            partition=seal.partition,
            seal_id=seal.seal_id,
            batch_id=seal.batch_id,
            member_id=seal.member_id,
            offset_end=seal.offset_end,
            covered_through=covered_through,
            decided_at=at,
        )
        for path in seal.files
    ]


@dataclass(frozen=True, slots=True)
class AppendResult:
    snapshot_id: int
    sequence_number: int
    metadata_location: str
    files: int
    records: int
    bytes: int
    offsets: CommittedOffsets
    commit_id: str


def commit_append(
    table: Table,
    plan: AppendPlan,
    *,
    runtime: str,
    client: Any,
    footer_pool: ThreadPoolExecutor | None = None,
    committed_at: str | None = None,
    commit_id: str | None = None,
) -> AppendResult:
    """Commit a plan's accepted files as one merge-append snapshot carrying the new offsets.

    Commits once: a `CommitFailedException` propagates, and the caller refreshes, plans again and
    retries - which re-judges every file against offsets another commit may have moved.
    """
    files = plan.files
    if not files:
        raise ValueError("an append needs at least one file")
    metadata = table.metadata
    granularity = partition_granularity_of(metadata.properties)

    def describe(item: AnnouncedFile):
        try:
            footer, size = read_parquet_footer(client, item.file_path)
        except FileNotFoundError as error:
            raise MissingDataFileError(item.file_path) from error
        return build_data_file(
            metadata,
            file_path=item.file_path,
            file_size=size,
            parquet_metadata=footer,
            granularity=granularity,
            partition_value=item.partition_value,
        )

    if footer_pool is None or len(files) == 1:
        data_files = [describe(item) for item in files]
    else:
        data_files = list(footer_pool.map(describe, files))
    resolved_commit_id = commit_id or uuid.uuid4().hex
    resolved_at = committed_at or utc_now()
    offsets = CommittedOffsets(
        offsets=plan.offsets.offsets,
        event_times=plan.offsets.event_times,
        committed_at=resolved_at,
    )
    records = sum(item.record_count for item in data_files)
    properties = {
        **offsets.summary_properties(),
        COMMIT_ID_PROPERTY: resolved_commit_id,
        COMMITTED_AT_PROPERTY: resolved_at,
        FILES_PROPERTY: str(len(data_files)),
        RECORDS_PROPERTY: str(records),
        RUNTIME_PROPERTY: runtime,
        OPERATION_PROPERTY: "append",
    }
    transaction = table.transaction()
    with transaction.update_snapshot(snapshot_properties=properties).merge_append() as append:
        for data_file in data_files:
            append.append_data_file(data_file)
    commit_once(transaction)
    snapshot = table.current_snapshot()
    return AppendResult(
        snapshot_id=snapshot.snapshot_id,
        sequence_number=snapshot.sequence_number,
        metadata_location=table.metadata_location,
        files=len(data_files),
        records=records,
        bytes=sum(item.file_size_in_bytes for item in data_files),
        offsets=CommittedOffsets(
            snapshot_id=snapshot.snapshot_id,
            offsets=offsets.offsets,
            event_times=offsets.event_times,
            committed_at=resolved_at,
        ),
        commit_id=resolved_commit_id,
    )


class _ReplaceSchema(UpdateTableMetadata["_ReplaceSchema"]):
    """Stage an evolved schema, made current, with its fork map, guarded against a moved schema."""

    def __init__(self, transaction, evolution: Evolution) -> None:
        super().__init__(transaction)
        self._evolution = evolution

    def _commit(self):
        metadata = self._transaction.table_metadata
        updates = (
            AddSchemaUpdate(schema_=self._evolution.schema),
            SetCurrentSchemaUpdate(schema_id=-1),
            SetPropertiesUpdate(updates={PROPERTY_FORK_MAP: self._evolution.fork_map.to_json()}),
        )
        requirements = (
            AssertCurrentSchemaId(current_schema_id=metadata.current_schema_id),
            AssertLastAssignedFieldId(last_assigned_field_id=metadata.last_column_id),
        )
        return updates, requirements


def commit_schema(table: Table, evolution: Evolution) -> None:
    """Commit an evolved schema and its fork map (one metadata commit, no snapshot)."""
    if not evolution.changed:
        raise ValueError("nothing to commit: the evolution changed nothing")
    transaction = table.transaction()
    _ReplaceSchema(transaction, evolution).commit()
    commit_once(transaction)


def fork_map_of(table_metadata) -> ForkMap:
    return ForkMap.from_json(table_metadata.properties.get(PROPERTY_FORK_MAP))


def table_totals(table: Table) -> dict[str, int]:
    """Records, data files and bytes in the current snapshot, from its summary."""
    snapshot = table.current_snapshot()
    if snapshot is None or snapshot.summary is None:
        return {"records": 0, "files": 0, "bytes": 0}
    summary = snapshot.summary.additional_properties
    return {
        "records": int(summary.get("total-records", 0) or 0),
        "files": int(summary.get("total-data-files", 0) or 0),
        "bytes": int(summary.get("total-files-size", 0) or 0),
    }


def current_offsets(table: Table) -> CommittedOffsets:
    return committed_offsets(table.metadata)


__all__ = [
    "ACCEPTED",
    "AnnouncedFile",
    "AppendPlan",
    "AppendResult",
    "COVERED",
    "DEFERRED",
    "ERROR",
    "FileDecision",
    "MissingDataFileError",
    "OVERLAP",
    "PendingFiles",
    "Seal",
    "commit_append",
    "commit_schema",
    "current_offsets",
    "fork_map_of",
    "plan_append",
    "table_totals",
]
