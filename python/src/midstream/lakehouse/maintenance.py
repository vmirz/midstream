"""Table maintenance: compaction, snapshot expiry, orphan cleanup (spec §7.9).

Pure planning and committing; the maintenance app schedules and runs the jobs
(`maintenance_app/`). Every job is write-then-commit, with the commit the only point of
truth (spec §7.9.1): a job that dies before its commit leaves nothing that matters behind, and one
that dies after it is simply done.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
import logging
from typing import Any
import uuid

from pyiceberg.manifest import DataFile, DataFileContent, ManifestContent
from pyiceberg.table import Table
from pyiceberg.table.metadata import TableMetadata
from pyiceberg.table.snapshots import Operation, Summary
from pyiceberg.table.update.snapshot import ExpireSnapshots, _OverwriteFiles

from midstream.lakehouse.catalog import commit_once
from midstream.lakehouse.files import partition_text_of
from midstream.lakehouse.gcs import parse_gcs_uri
from midstream.lakehouse.offsets import (
    COMMIT_ID_PROPERTY,
    COMMITTED_AT_PROPERTY,
    FILES_PROPERTY,
    OPERATION_PROPERTY,
    RECORDS_PROPERTY,
    RUNTIME_PROPERTY,
    committed_offsets,
    offsets_from_summary,
)
from midstream.lakehouse.pointer import POINTER_NAME, VERSION_HINT_NAME, utc_now
from midstream.lakehouse.tables import TIMESTAMP_FIELD_ID, partition_granularity_of

LOGGER = logging.getLogger(__name__)

#: Input files one compaction takes at most: bounds a job's downloads when partitions are tiny.
MAX_PLAN_INPUT_FILES = 2_000
#: How long one partition of each granularity spans, in milliseconds; a month's varies.
PARTITION_SPAN_MS = {"day": 86_400_000, "hour": 3_600_000}
#: An open partition longer than this - a month - is not waited for as a whole: each of its files
#: settles as it ages, or a busy month's small files would wait until the month ended.
LONG_PARTITION_MS = 86_400_000


class _ReplaceFiles(_OverwriteFiles):
    """A `replace` snapshot: files rewritten, rows unchanged.

    PyIceberg 0.12 cannot total a `replace` summary (`update_snapshot_summaries` refuses the
    operation, experiment E6), so the totals are computed as for an overwrite - the same
    arithmetic - and the summary then names the operation it is.
    """

    def _summary(self, snapshot_properties: dict[str, str] = {}):  # noqa: B006 - PyIceberg's signature
        operation, self._operation = self._operation, Operation.OVERWRITE
        try:
            summary = super()._summary(snapshot_properties)
        finally:
            self._operation = operation
        return Summary(operation=operation, **summary.additional_properties)


class CompactionInputGone(RuntimeError):
    """An input of a compaction is no longer in the table: expired, or compacted already."""


class CompactionMismatch(RuntimeError):
    """A compaction's outputs do not hold exactly its inputs' rows."""


@dataclass(frozen=True, slots=True)
class LiveFile:
    """A data file in the current snapshot."""

    path: str
    file_size: int
    record_count: int
    partition_key: str
    partition: tuple[Any, ...]
    added_snapshot_id: int | None
    added_at_ms: int | None
    timestamp_min: int | None
    timestamp_max: int | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "path": self.path,
            "file_size": self.file_size,
            "record_count": self.record_count,
            "partition_key": self.partition_key,
            "partition": list(self.partition),
            "added_snapshot_id": self.added_snapshot_id,
            "added_at_ms": self.added_at_ms,
            "timestamp_min": self.timestamp_min,
            "timestamp_max": self.timestamp_max,
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "LiveFile":
        return cls(
            path=value["path"],
            file_size=int(value["file_size"]),
            record_count=int(value["record_count"]),
            partition_key=str(value["partition_key"]),
            partition=tuple(value.get("partition") or ()),
            added_snapshot_id=value.get("added_snapshot_id"),
            added_at_ms=value.get("added_at_ms"),
            timestamp_min=value.get("timestamp_min"),
            timestamp_max=value.get("timestamp_max"),
        )


@dataclass(frozen=True, slots=True)
class CompactionPlan:
    """Small files to rewrite: one partition's, or several whole partitions' (`plan_compaction`).

    `partition_key` and `partition` are the first partition's; every input carries its own.
    """

    job_id: str
    partition_key: str
    partition: tuple[Any, ...]
    inputs: tuple[LiveFile, ...]
    planned_snapshot_id: int | None

    @property
    def input_bytes(self) -> int:
        return sum(item.file_size for item in self.inputs)

    @property
    def input_records(self) -> int:
        return sum(item.record_count for item in self.inputs)

    @property
    def partition_keys(self) -> tuple[str, ...]:
        """Every partition the plan compacts, in plan order."""
        return tuple(dict.fromkeys(item.partition_key for item in self.inputs)) or (self.partition_key,)

    @property
    def label(self) -> str:
        """The plan's partitions as people read them: `2026-10-18`, or `2024-10-05 … 2024-11-21 (47 partitions)`."""
        keys = self.partition_keys
        if len(keys) == 1:
            return keys[0]
        return f"{keys[0]} … {keys[-1]} ({len(keys)} partitions)"

    def to_dict(self) -> dict[str, Any]:
        return {
            "job_id": self.job_id,
            "partition_key": self.partition_key,
            "partition": list(self.partition),
            "inputs": [item.to_dict() for item in self.inputs],
            "planned_snapshot_id": self.planned_snapshot_id,
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "CompactionPlan":
        return cls(
            job_id=value["job_id"],
            partition_key=value["partition_key"],
            partition=tuple(value.get("partition") or ()),
            inputs=tuple(LiveFile.from_dict(item) for item in value.get("inputs") or []),
            planned_snapshot_id=value.get("planned_snapshot_id"),
        )


def live_data_files(table: Table) -> list[LiveFile]:
    """Every data file of the current snapshot, with when it was added and its time bounds."""
    snapshot = table.current_snapshot()
    if snapshot is None:
        return []
    metadata = table.metadata
    granularity = partition_granularity_of(metadata.properties)
    files: list[LiveFile] = []
    for manifest in snapshot.manifests(table.io):
        if manifest.content != ManifestContent.DATA:
            continue
        for entry in manifest.fetch_manifest_entry(table.io, discard_deleted=True):
            data_file = entry.data_file
            if data_file.content != DataFileContent.DATA:
                continue
            added = metadata.snapshot_by_id(entry.snapshot_id) if entry.snapshot_id is not None else None
            files.append(
                LiveFile(
                    path=data_file.file_path,
                    file_size=int(data_file.file_size_in_bytes),
                    record_count=int(data_file.record_count),
                    partition_key=_partition_key(granularity, data_file.partition),
                    partition=tuple(data_file.partition[index] for index in range(len(data_file.partition))),
                    added_snapshot_id=entry.snapshot_id,
                    added_at_ms=None if added is None else added.timestamp_ms,
                    timestamp_min=_timestamp_bound(data_file.lower_bounds),
                    timestamp_max=_timestamp_bound(data_file.upper_bounds),
                )
            )
    return files


def small_file_count(files: Iterable[LiveFile], *, small_file_bytes: int) -> int:
    return sum(1 for item in files if item.file_size < small_file_bytes)


def plan_compaction(
    table: Table,
    *,
    now: datetime,
    min_input_files: int,
    small_file_bytes: int,
    max_input_bytes: int,
    hot_partition_delay_minutes: int,
    skip: Iterable[str] = (),
    solo: Iterable[str] = (),
    files: list[LiveFile] | None = None,
    max_input_files: int = MAX_PLAN_INPUT_FILES,
) -> CompactionPlan | None:
    """The partitions to compact next, oldest first; None when no partition qualifies.

    A partition qualifies once it holds `min_input_files` files smaller than `small_file_bytes`.
    What live traffic still writes waits for `hot_partition_delay_minutes`, so it is not rewritten
    again and again while it grows: a partition whose time reaches into that last stretch - today,
    or this hour - until no commit has added to it for that long; and, where no time bounds a
    partition soon (an unpartitioned table, the rows without a timestamp, the current month), each
    file until it is that old. A past partition qualifies however recently a backfill or a late
    record added to it.

    A plan takes whole partitions, oldest first, while their small files fit `max_input_bytes`
    and `max_input_files` together, so a backfill's many small partitions compact a gibibyte at a
    time. A partition larger than that is taken alone, its oldest small files up to the bounds. A
    partition in `solo` - one whose compaction failed - is only ever planned alone, so a failure
    is always its own.
    """
    skipped = set(skip)
    alone = set(solo)
    live = live_data_files(table) if files is None else files
    granularity = partition_granularity_of(table.metadata.properties)
    by_partition: dict[str, list[LiveFile]] = {}
    for item in live:
        by_partition.setdefault(item.partition_key, []).append(item)
    now_ms = int(now.timestamp() * 1000)
    hot_ms = hot_partition_delay_minutes * 60_000
    chosen: list[LiveFile] = []
    total = 0
    for key in sorted(by_partition, key=_partition_sort_key):
        if key in skipped:
            continue
        small = sorted(
            (
                item
                for item in _settled_files(by_partition[key], granularity=granularity, now_ms=now_ms, hot_ms=hot_ms)
                if item.file_size < small_file_bytes
            ),
            key=lambda item: (item.timestamp_min or 0, item.path),
        )
        if len(small) < min_input_files:
            continue
        if chosen:
            # Later partitions join whole, and only while they fit; one that must run alone waits.
            size = sum(item.file_size for item in small)
            if key in alone or total + size > max_input_bytes or len(chosen) + len(small) > max_input_files:
                break
            chosen.extend(small)
            total += size
            continue
        taken: list[LiveFile] = []
        for item in small:
            if taken and (total + item.file_size > max_input_bytes or len(taken) >= max_input_files):
                break
            taken.append(item)
            total += item.file_size
        if len(taken) < min_input_files:
            total = 0
            continue
        chosen = taken
        if key in alone or len(taken) < len(small):
            break
    if not chosen:
        return None
    return CompactionPlan(
        job_id=uuid.uuid4().hex,
        partition_key=chosen[0].partition_key,
        partition=chosen[0].partition,
        inputs=tuple(chosen),
        planned_snapshot_id=table.metadata.current_snapshot_id,
    )


def _settled_files(group: list[LiveFile], *, granularity: str, now_ms: int, hot_ms: int) -> list[LiveFile]:
    """The files of one partition that compaction may take now (the rules in `plan_compaction`)."""
    if hot_ms <= 0:
        return group
    window = _partition_window_ms(granularity, group[0].partition)
    if window is not None and window[1] <= now_ms - hot_ms:
        return group
    if window is None or window[1] - window[0] > LONG_PARTITION_MS:
        # Nothing ends this partition soon, so it is never waited for as a whole: each file waits
        # until it is old enough.
        return [item for item in group if item.added_at_ms is None or now_ms - item.added_at_ms >= hot_ms]
    latest_add = max((item.added_at_ms or 0) for item in group)
    return [] if now_ms - latest_add < hot_ms else group


def _partition_window_ms(granularity: str, partition: tuple[Any, ...]) -> tuple[int, int] | None:
    """The span of Kafka timestamps a partition holds, epoch milliseconds; None when none bounds it."""
    if not partition or partition[0] is None:
        return None
    if granularity == "month":
        return _month_start_ms(int(partition[0])), _month_start_ms(int(partition[0]) + 1)
    if granularity not in PARTITION_SPAN_MS:
        return None
    span = PARTITION_SPAN_MS[granularity]
    start = int(partition[0]) * span
    return start, start + span


def _month_start_ms(months: int) -> int:
    """The first instant of the month `months` after January 1970, epoch milliseconds."""
    years, month = divmod(months, 12)
    return int(datetime(1970 + years, month + 1, 1, tzinfo=UTC).timestamp() * 1000)


def commit_compaction(
    table: Table,
    plan: CompactionPlan,
    outputs: list[DataFile],
    *,
    runtime: str,
    output_bounds: list[tuple[int | None, int | None]] | None = None,
) -> int:
    """Replace a plan's inputs by its outputs in one `replace` snapshot; return its id.

    Checks first that every input is still live (not expired, not compacted by another job) and
    that each partition's outputs hold exactly its inputs' rows within their time bounds; raises
    `CompactionInputGone` or `CompactionMismatch` otherwise, committing nothing. The snapshot
    carries the committed offsets forward unchanged.
    """
    live = {item.file_path: item for item in _current_data_files(table)}
    missing = [item.path for item in plan.inputs if item.path not in live]
    if missing:
        raise CompactionInputGone(f"{len(missing)} input(s) are no longer in the table, e.g. {missing[0]}")
    input_records = plan.input_records
    output_records = sum(int(item.record_count) for item in outputs)
    if output_records != input_records:
        raise CompactionMismatch(f"the outputs hold {output_records} rows, the inputs {input_records}")
    _check_partitions(plan, outputs, output_bounds)
    offsets = committed_offsets(table.metadata)
    properties = {
        **offsets.summary_properties(),
        COMMIT_ID_PROPERTY: uuid.uuid4().hex,
        COMMITTED_AT_PROPERTY: utc_now(),
        FILES_PROPERTY: str(len(outputs)),
        RECORDS_PROPERTY: str(output_records),
        RUNTIME_PROPERTY: runtime,
        OPERATION_PROPERTY: "compaction",
        "materializer.compaction-job": plan.job_id,
        "materializer.compacted-files": str(len(plan.inputs)),
    }
    transaction = table.transaction()
    producer = _ReplaceFiles(
        operation=Operation.REPLACE,
        transaction=transaction,
        io=table.io,
        snapshot_properties=properties,
    )
    with producer as replace:
        for item in plan.inputs:
            replace.delete_data_file(live[item.path])
        for data_file in outputs:
            replace.append_data_file(data_file)
    commit_once(transaction)
    return table.metadata.current_snapshot_id


def _check_partitions(
    plan: CompactionPlan,
    outputs: list[DataFile],
    output_bounds: list[tuple[int | None, int | None]] | None,
) -> None:
    """Each partition's outputs hold exactly its inputs' rows, inside its inputs' time bounds."""
    inputs: dict[tuple[Any, ...], list[LiveFile]] = {}
    for item in plan.inputs:
        inputs.setdefault(tuple(item.partition), []).append(item)
    written: dict[tuple[Any, ...], list[int]] = {}
    for index, data_file in enumerate(outputs):
        written.setdefault(_partition_values(data_file.partition), []).append(index)
    stray = set(written) - set(inputs)
    if stray:
        raise CompactionMismatch(f"{sum(len(written[values]) for values in stray)} output(s) are in a partition no input is in")
    bounds = list(output_bounds or [])
    for values, items in inputs.items():
        indexes = written.get(values, [])
        key = items[0].partition_key
        expected = sum(item.record_count for item in items)
        held = sum(int(outputs[index].record_count) for index in indexes)
        if held != expected:
            raise CompactionMismatch(f"partition {key}: the outputs hold {held} rows, the inputs {expected}")
        lows = [item.timestamp_min for item in items if item.timestamp_min is not None]
        highs = [item.timestamp_max for item in items if item.timestamp_max is not None]
        for index in indexes:
            if index >= len(bounds):
                continue
            low, high = bounds[index]
            if low is not None and lows and low < min(lows):
                raise CompactionMismatch(f"partition {key}: an output starts before every input")
            if high is not None and highs and high > max(highs):
                raise CompactionMismatch(f"partition {key}: an output ends after every input")


def _partition_values(partition: Any) -> tuple[Any, ...]:
    try:
        return tuple(partition[index] for index in range(len(partition)))
    except TypeError:
        return ()


def _current_data_files(table: Table) -> list[DataFile]:
    snapshot = table.current_snapshot()
    if snapshot is None:
        return []
    result: list[DataFile] = []
    for manifest in snapshot.manifests(table.io):
        if manifest.content != ManifestContent.DATA:
            continue
        for entry in manifest.fetch_manifest_entry(table.io, discard_deleted=True):
            result.append(entry.data_file)
    return result


def plan_expiry(
    metadata: TableMetadata,
    *,
    now: datetime,
    retention_hours: int,
    min_snapshots_to_keep: int,
    keep_all_minutes: int,
) -> list[int]:
    """Snapshots to expire: past retention, or thinned to one an hour beyond `keep_all_minutes`.

    Never the current snapshot, a branch or tag head, the newest `min_snapshots_to_keep`, or the
    newest snapshot that carries committed offsets (the runtime's checkpoint).
    """
    snapshots = sorted(metadata.snapshots, key=lambda item: item.timestamp_ms, reverse=True)
    if not snapshots:
        return []
    protected = {ref.snapshot_id for ref in metadata.refs.values()}
    if metadata.current_snapshot_id is not None:
        protected.add(metadata.current_snapshot_id)
    protected.update(item.snapshot_id for item in snapshots[:min_snapshots_to_keep])
    for item in snapshots:
        summary = {} if item.summary is None else dict(item.summary.additional_properties)
        if offsets_from_summary(summary) is not None:
            protected.add(item.snapshot_id)
            break
    now_ms = int(now.timestamp() * 1000)
    dense_cutoff = now_ms - keep_all_minutes * 60_000
    retention_cutoff = now_ms - retention_hours * 3_600_000
    kept_hours: set[int] = set()
    expire: list[int] = []
    for item in snapshots:
        if item.snapshot_id in protected:
            kept_hours.add(item.timestamp_ms // 3_600_000)
            continue
        if item.timestamp_ms >= dense_cutoff:
            continue
        if item.timestamp_ms < retention_cutoff:
            expire.append(item.snapshot_id)
            continue
        hour = item.timestamp_ms // 3_600_000
        if hour in kept_hours:
            expire.append(item.snapshot_id)
        else:
            kept_hours.add(hour)
    return expire


def expire_snapshots(table: Table, snapshot_ids: list[int]) -> int:
    """Remove `snapshot_ids` from the table's metadata (one commit); return how many.

    Files go nowhere: what only expired snapshots referenced is deleted by orphan cleanup
    (PyIceberg's expiry deletes no files, experiment E7).
    """
    if not snapshot_ids:
        return 0
    transaction = table.transaction()
    ExpireSnapshots(transaction).by_ids(list(snapshot_ids)).commit()
    commit_once(transaction)
    return len(snapshot_ids)


def referenced_files(table: Table) -> set[str]:
    """Every file any retained snapshot needs: data files, manifests, manifest lists, metadata.

    A manifest's `deleted` entries are not references: they record that a snapshot removed the file
    (a compaction's inputs, say), and any snapshot still reading it lists it as added or existing in
    its own manifests (experiment E7).
    """
    referenced: set[str] = {table.metadata_location}
    for entry in table.metadata.metadata_log:
        referenced.add(entry.metadata_file)
    seen_manifests: set[str] = set()
    for snapshot in table.metadata.snapshots:
        referenced.add(snapshot.manifest_list)
        for manifest in snapshot.manifests(table.io):
            referenced.add(manifest.manifest_path)
            if manifest.manifest_path in seen_manifests:
                continue
            seen_manifests.add(manifest.manifest_path)
            for entry in manifest.fetch_manifest_entry(table.io, discard_deleted=True):
                referenced.add(entry.data_file.file_path)
    return referenced


@dataclass(frozen=True, slots=True)
class OrphanScan:
    candidates: tuple[str, ...] = ()
    listed: int = 0
    truncated: bool = False


def find_orphans(
    client: Any,
    location: str,
    referenced: set[str],
    *,
    now: datetime,
    min_age_hours: int,
    protected: Iterable[str] = (),
    limit: int = 1000,
) -> OrphanScan:
    """Files under the table's `data/` and `metadata/` nothing references, older than the age guard."""
    keep = set(referenced) | set(protected)
    bucket_name, prefix = parse_gcs_uri(location.rstrip("/") + "/")
    cutoff = now - timedelta(hours=min_age_hours)
    candidates: list[str] = []
    listed = 0
    for directory in ("data/", "metadata/"):
        for blob in client.list_blobs(bucket_name, prefix=prefix + directory):
            listed += 1
            name = blob.name
            uri = f"gs://{bucket_name}/{name}"
            base = name.rsplit("/", 1)[-1]
            if uri in keep or base in (POINTER_NAME, VERSION_HINT_NAME) or base.startswith("catalog-pointer"):
                continue
            created = _blob_time(blob)
            if created is None or created > cutoff:
                # Only a file provably older than the guard is deleted.
                continue
            candidates.append(uri)
            if len(candidates) >= limit:
                return OrphanScan(candidates=tuple(candidates), listed=listed, truncated=True)
    return OrphanScan(candidates=tuple(candidates), listed=listed)


def _blob_time(blob: Any) -> datetime | None:
    for name in ("time_created", "updated"):
        value = getattr(blob, name, None)
        if isinstance(value, datetime):
            return value if value.tzinfo is not None else value.replace(tzinfo=UTC)
    return None


def _partition_key(granularity: str, partition: Any) -> str:
    """The partition as people read it - `2026-10-18`, `2026-10-18-10`, `__null__` - or `all`."""
    try:
        values = tuple(partition[index] for index in range(len(partition)))
    except TypeError:
        values = ()
    return partition_text_of(granularity, values) or "all"


def _partition_sort_key(key: str) -> tuple:
    # ISO dates and hours sort as text; rows without a timestamp come after every day.
    return (key.startswith("__"), key)


def _timestamp_bound(bounds: dict[int, bytes] | None) -> int | None:
    """`_kafka_timestamp`'s bound (microseconds since the epoch) from a file's statistics."""
    if not bounds:
        return None
    raw = bounds.get(TIMESTAMP_FIELD_ID)
    if raw is None or len(raw) != 8:
        return None
    return int.from_bytes(raw, "little", signed=True)


__all__ = [
    "CompactionInputGone",
    "CompactionMismatch",
    "CompactionPlan",
    "LiveFile",
    "OrphanScan",
    "commit_compaction",
    "expire_snapshots",
    "find_orphans",
    "live_data_files",
    "plan_compaction",
    "plan_expiry",
    "referenced_files",
    "small_file_count",
]
