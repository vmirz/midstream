"""One table's live tail in memory: a contiguous run of its records per Kafka partition.

Pure bookkeeping, no Kafka and no I/O. Per partition the buffer holds every record Kafka has with an
offset in `[covered_from, next_offset)` - as a row, or known to need none (a skipped tombstone, an
undecodable value; offsets compaction or transactions left empty simply never arrive). Records are
appended in offset order by the one consumer that reads the partition, so the run never has a hole:

- **segments**: a partition's rows are kept a segment at a time. The open segment takes rows as
  they are read; at `SEGMENT_ROWS` rows, `SEGMENT_BYTES` of staged JSON or `SEGMENT_MAX_AGE_SECONDS`
  it closes into one Arrow batch in the table's schema, keyed by the schema id the rows were built
  under, and keeps its staged lines zstd-compressed so it can be built again after a schema change;
- **commits** raise each partition's `committed` offset (never lower it). A segment the table now
  holds is kept for `retention_seconds` after the commit that covered it, then dropped from the old
  end, and `covered_from` moves past it;
- **selection** answers "every record past `after`": for each partition, the segments holding rows
  past it, the open segment's rows, and how far the answer covers. A request whose `after` is older
  than what is still covered is refused for the whole table rather than answered with a hole.

The buffer never evicts from the new end: when a table outgrows its bound the consumer pauses it
(`table_tail.py`), so what is held stays a prefix of the records past the committed offsets.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
import time
from typing import Any

import pyarrow as pa

#: A partition's open segment closes at this many rows, staged bytes or seconds of age.
SEGMENT_ROWS = 2_000
SEGMENT_BYTES = 4 * 1024 * 1024
SEGMENT_MAX_AGE_SECONDS = 5.0
#: Resident bytes of an open segment per staged byte: its rows are Python objects until it closes.
OPEN_ROW_OVERHEAD = 4
#: Level zstd compresses a closed segment's staged lines at: cheap, and 5-10x on JSON.
RAW_COMPRESSION_LEVEL = 1


@dataclass(frozen=True, slots=True)
class SegmentView:
    """A closed segment's rows as one Arrow batch, under one schema."""

    batch: pa.RecordBatch
    schema_id: int
    #: Offsets of the rows whose values a schema change in flight will move into columns
    #: (`ingest.TailRow.settling`): few, so kept exactly, and counted for any part of the segment.
    settling: tuple[int, ...] = ()
    #: The batch's size, read once: pyarrow walks every buffer of every column to answer it, and the
    #: fetch loop weighs every segment of every active table after each fetch.
    nbytes: int = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "nbytes", self.batch.nbytes)


class Segment:
    """A closed run of one partition's rows: an Arrow batch, and its staged lines compressed."""

    __slots__ = (
        "partition",
        "seq",
        "first_offset",
        "last_offset",
        "rows",
        "raw",
        "raw_size",
        "raw_bytes",
        "view",
        "time_min",
        "time_max",
        "committed_at",
    )

    def __init__(
        self,
        *,
        partition: int,
        seq: int,
        first_offset: int,
        last_offset: int,
        rows: int,
        raw: bytes,
        raw_size: int,
        raw_bytes: int,
        view: SegmentView,
        time_min: int | None,
        time_max: int | None,
    ) -> None:
        self.partition = partition
        #: Arrival order across the table's partitions: what merges partitions when a cap cuts.
        self.seq = seq
        self.first_offset = first_offset
        self.last_offset = last_offset
        self.rows = rows
        #: Its staged lines, newline-joined and zstd-compressed, and their size uncompressed.
        self.raw = raw
        self.raw_size = raw_size
        self.raw_bytes = raw_bytes
        self.view = view
        #: Epoch milliseconds of the earliest and latest Kafka timestamp among its rows.
        self.time_min = time_min
        self.time_max = time_max
        #: Monotonic time of the commit that covered it; None while the table does not hold it.
        self.committed_at: float | None = None

    @property
    def nbytes(self) -> int:
        return self.view.nbytes + len(self.raw)

    def lines(self) -> list[bytes]:
        """Its staged lines, decompressed."""
        return decompress_lines(self.raw, self.raw_size)


class OpenSegment:
    """A partition's newest rows, as built, until they close into a segment."""

    __slots__ = ("seq", "lines", "rows", "offsets", "moments", "schema_id", "settling", "raw_bytes", "opened_at")

    def __init__(self, *, seq: int, schema_id: int, opened_at: float) -> None:
        self.seq = seq
        self.lines: list[bytes] = []
        self.rows: list[dict[str, Any]] = []
        self.offsets: list[int] = []
        #: Each row's Kafka timestamp in epoch milliseconds, or None.
        self.moments: list[int | None] = []
        self.schema_id = schema_id
        #: Offsets of its settling rows.
        self.settling: list[int] = []
        self.raw_bytes = 0
        self.opened_at = opened_at

    @property
    def nbytes(self) -> int:
        return self.raw_bytes * OPEN_ROW_OVERHEAD


class PartitionBuffer:
    """One Kafka partition of a table's tail."""

    __slots__ = ("partition", "committed", "covered_from", "next_offset", "segments", "open", "gap_floor")

    def __init__(self, partition: int, *, committed: int, start: int, gap_floor: int | None = None) -> None:
        self.partition = partition
        #: The last offset the table holds, inclusive; -1 for a partition never committed.
        self.committed = committed
        #: Every record Kafka holds with an offset in `[covered_from, next_offset)` is here.
        self.covered_from = start
        self.next_offset = start
        self.segments: deque[Segment] = deque()
        self.open: OpenSegment | None = None
        #: Kafka's first offset when the table's next record was below it - retention removed the
        #: records in between - so an answer from here on has a gap Kafka made, not one of ours.
        self.gap_floor = gap_floor

    @property
    def rows(self) -> int:
        return sum(segment.rows for segment in self.segments) + (len(self.open.rows) if self.open else 0)

    def uncommitted_rows(self) -> int:
        """Rows past the table's committed offset."""
        count = 0
        for segment in reversed(self.segments):
            if segment.last_offset <= self.committed:
                break
            if segment.first_offset > self.committed:
                count += segment.rows
            else:
                count += sum(1 for offset in _segment_offsets(segment) if offset > self.committed)
        if self.open is not None:
            count += sum(1 for offset in self.open.offsets if offset > self.committed)
        return count

    def resident_bytes(self) -> int:
        return sum(segment.nbytes for segment in self.segments) + (self.open.nbytes if self.open else 0)

    def retained_bytes(self) -> int:
        """Bytes of segments the table already holds: what can go first when memory runs short."""
        return sum(segment.nbytes for segment in self.segments if segment.last_offset <= self.committed)


@dataclass(slots=True)
class PartitionSelection:
    """What an answer takes of one partition: everything past `after`, as far as it is held."""

    partition: int
    after: int
    segments: list[Segment] = field(default_factory=list)
    #: A copy of the open segment's rows and lines past `after`, with their offsets.
    open_rows: list[dict[str, Any]] = field(default_factory=list)
    open_lines: list[bytes] = field(default_factory=list)
    open_offsets: list[int] = field(default_factory=list)
    open_moments: list[int | None] = field(default_factory=list)
    open_schema_id: int | None = None
    open_settling: list[int] = field(default_factory=list)
    open_seq: int | None = None
    #: The partition's coverage when selected: every record below `next_offset` past `after` is in it.
    next_offset: int = 0
    #: `after` was below what Kafka still holds: the answer starts at Kafka's beginning.
    gap: bool = False


@dataclass(slots=True)
class Selection:
    partitions: dict[int, PartitionSelection]


@dataclass(frozen=True, slots=True)
class Unavailable:
    """An answer the buffer cannot give for the caller's `after`: per partition, why."""

    partitions: dict[int, str]


class TableBuffer:
    """Every Kafka partition of one table's tail."""

    def __init__(self, *, clock: Any = time.monotonic) -> None:
        self.partitions: dict[int, PartitionBuffer] = {}
        self._seq = 0
        self._clock = clock

    # -- shape --------------------------------------------------------------------------------------

    def start(self, partition: int, *, committed: int, start: int, gap_floor: int | None = None) -> PartitionBuffer:
        """(Re)start a partition at `start`, the next offset to read, holding nothing."""
        buffer = PartitionBuffer(partition, committed=committed, start=start, gap_floor=gap_floor)
        self.partitions[partition] = buffer
        return buffer

    def drop(self) -> None:
        """Let go of everything held."""
        self.partitions.clear()

    # -- ingest -------------------------------------------------------------------------------------

    def append(
        self,
        partition: int,
        *,
        offset: int,
        line: bytes,
        row: dict[str, Any],
        moment: int | None,
        schema_id: int,
        settling: bool,
    ) -> OpenSegment:
        """One record's row, at the partition's next offset or past it (Kafka may skip offsets)."""
        buffer = self.partitions[partition]
        if offset < buffer.next_offset:
            raise ValueError(f"offset {offset} of partition {partition} is below the next offset {buffer.next_offset}")
        segment = buffer.open
        if segment is None:
            segment = buffer.open = OpenSegment(seq=self._next_seq(), schema_id=schema_id, opened_at=self._clock())
        segment.lines.append(line)
        segment.rows.append(row)
        segment.offsets.append(offset)
        segment.moments.append(moment)
        segment.raw_bytes += len(line)
        if settling:
            segment.settling.append(offset)
        buffer.next_offset = offset + 1
        return segment

    def skip(self, partition: int, *, offset: int) -> None:
        """A record that needs no row: the partition is covered past it all the same."""
        buffer = self.partitions[partition]
        if offset < buffer.next_offset:
            raise ValueError(f"offset {offset} of partition {partition} is below the next offset {buffer.next_offset}")
        buffer.next_offset = offset + 1

    def advance(self, partition: int, position: int) -> None:
        """Kafka's position after a fetch: offsets below it it holds no record of are covered too."""
        buffer = self.partitions.get(partition)
        if buffer is not None and position > buffer.next_offset:
            buffer.next_offset = position

    def due_to_close(self, partition: int) -> bool:
        segment = self.partitions[partition].open
        if segment is None:
            return False
        return (
            len(segment.rows) >= SEGMENT_ROWS
            or segment.raw_bytes >= SEGMENT_BYTES
            or self._clock() - segment.opened_at >= SEGMENT_MAX_AGE_SECONDS
        )

    def close(self, partition: int, *, batch: pa.RecordBatch, schema_id: int, settling: tuple[int, ...]) -> Segment | None:
        """Close a partition's open segment into one already built as `batch` (under `schema_id`)."""
        buffer = self.partitions[partition]
        segment = buffer.open
        if segment is None:
            return None
        buffer.open = None
        times = [moment for moment in segment.moments if moment is not None]
        raw, raw_size = compress_lines(segment.lines)
        closed = Segment(
            partition=partition,
            seq=segment.seq,
            first_offset=segment.offsets[0],
            last_offset=segment.offsets[-1],
            rows=len(segment.rows),
            raw=raw,
            raw_size=raw_size,
            raw_bytes=segment.raw_bytes,
            view=SegmentView(batch=batch, schema_id=schema_id, settling=tuple(settling)),
            time_min=min(times) if times else None,
            time_max=max(times) if times else None,
        )
        if closed.last_offset <= buffer.committed:
            closed.committed_at = self._clock()
        buffer.segments.append(closed)
        return closed

    # -- the table ----------------------------------------------------------------------------------

    def commit(self, offsets: dict[int, int]) -> None:
        """The table's new committed offsets: each partition's only ever rises."""
        now = self._clock()
        for partition, offset in offsets.items():
            buffer = self.partitions.get(int(partition))
            if buffer is None or offset <= buffer.committed:
                continue
            buffer.committed = int(offset)
            for segment in buffer.segments:
                if segment.last_offset > buffer.committed:
                    break
                if segment.committed_at is None:
                    segment.committed_at = now

    def trim(self, retention_seconds: float) -> int:
        """Drop segments the table has held for `retention_seconds`, from the old end; return how many."""
        now = self._clock()
        dropped = 0
        for buffer in self.partitions.values():
            while buffer.segments:
                oldest = buffer.segments[0]
                if oldest.committed_at is None or now - oldest.committed_at < retention_seconds:
                    break
                buffer.segments.popleft()
                buffer.covered_from = max(buffer.covered_from, oldest.last_offset + 1)
                dropped += 1
        return dropped

    def drop_retained(self) -> int:
        """Drop every segment the table already holds, whatever its age: memory is short."""
        dropped = 0
        for buffer in self.partitions.values():
            while buffer.segments and buffer.segments[0].last_offset <= buffer.committed:
                oldest = buffer.segments.popleft()
                buffer.covered_from = max(buffer.covered_from, oldest.last_offset + 1)
                dropped += 1
        return dropped

    def uncommitted_rows(self) -> int:
        return sum(buffer.uncommitted_rows() for buffer in self.partitions.values())

    def resident_bytes(self) -> int:
        return sum(buffer.resident_bytes() for buffer in self.partitions.values())

    def retained_bytes(self) -> int:
        return sum(buffer.retained_bytes() for buffer in self.partitions.values())

    def rows(self) -> int:
        return sum(buffer.rows for buffer in self.partitions.values())

    # -- answers ------------------------------------------------------------------------------------

    def select(self, after: dict[int, int]) -> Selection | Unavailable:
        """Everything held past `after` - partition -> the last offset the caller has, -1 for none.

        A partition the caller does not name is taken from its beginning (-1). Refused as a whole
        when any partition no longer covers what follows its `after`: answering the others would
        leave a hole.
        """
        unavailable: dict[int, str] = {}
        selections: dict[int, PartitionSelection] = {}
        for partition, buffer in self.partitions.items():
            since = int(after.get(partition, -1))
            gap = False
            if since + 1 < buffer.covered_from:
                if buffer.gap_floor is not None and buffer.covered_from <= buffer.gap_floor:
                    # Kafka holds nothing below its beginning: neither does the table's runtime.
                    gap = True
                else:
                    unavailable[partition] = "behind_retention"
                    continue
            selection = PartitionSelection(partition=partition, after=since, next_offset=buffer.next_offset, gap=gap)
            for segment in buffer.segments:
                if segment.last_offset > since:
                    selection.segments.append(segment)
            segment = buffer.open
            if segment is not None and segment.offsets and segment.offsets[-1] > since:
                start = _first_index_past(segment.offsets, since)
                selection.open_rows = segment.rows[start:]
                selection.open_lines = segment.lines[start:]
                selection.open_offsets = segment.offsets[start:]
                selection.open_moments = segment.moments[start:]
                selection.open_schema_id = segment.schema_id
                selection.open_settling = [offset for offset in segment.settling if offset > since]
                selection.open_seq = segment.seq
            selections[partition] = selection
        if unavailable:
            return Unavailable(partitions=unavailable)
        return Selection(partitions=selections)

    def _next_seq(self) -> int:
        self._seq += 1
        return self._seq


#: zstd at a low level: cheap, and 5-10x on staged JSON.
_RAW_CODEC = pa.Codec("zstd", compression_level=RAW_COMPRESSION_LEVEL)


def compress_lines(lines: list[bytes]) -> tuple[bytes, int]:
    """Staged lines as one zstd buffer, and the size they decompress to."""
    if not lines:
        return b"", 0
    joined = b"\n".join(lines)
    return _RAW_CODEC.compress(joined, asbytes=True), len(joined)


def decompress_lines(raw: bytes, size: int) -> list[bytes]:
    if not raw:
        return []
    return _RAW_CODEC.decompress(raw, decompressed_size=size, asbytes=True).split(b"\n")


def _first_index_past(offsets: list[int], since: int) -> int:
    low, high = 0, len(offsets)
    while low < high:
        middle = (low + high) // 2
        if offsets[middle] <= since:
            low = middle + 1
        else:
            high = middle
    return low


def _segment_offsets(segment: Segment) -> list[int]:
    return segment.view.batch.column("_kafka_offset").to_pylist()
