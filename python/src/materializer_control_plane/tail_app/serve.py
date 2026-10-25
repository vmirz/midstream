"""An answer from what the buffer selected: one Parquet file in the table's exact physical format.

Runs on the member's serve threads: pyarrow does most of the work and lets go of the GIL. The rows
are the table's current schema's - a segment built under an older schema is built again from its
staged lines with the shared row builder first, as the seal would conform them now - and the file is
written as the seal writes one: the table's Arrow schema with its field ids, zstd, statistics on
(`lakehouse/parquet.py`). Rows of several Kafka partitions share the file, as a compacted file's do.

Caps (`max_rows`, `max_bytes`) cut the newest end only: pieces - closed segments, and each
partition's open rows - are taken in the order they arrived across partitions, so what an answer
holds of each partition is a prefix of what follows its `after`, and `through` says where it ends.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
import io
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from materializer_control_plane.lakehouse.parquet import DEFAULT_COMPRESSION
from materializer_control_plane.tail_app.buffer import PartitionSelection, Segment, SegmentView, Selection
from materializer_control_plane.tail_app.ingest import TailSchema, build, epoch_ms

#: Rows a `format=json` answer holds at most: it is for tests and debugging.
JSON_ROW_LIMIT = 1_000


@dataclass(slots=True)
class Answer:
    """An answer's body and what its headers say about it."""

    body: bytes
    rows: int
    #: Per partition, the last offset the answer covers: every record past `after` up to it is in it.
    through: dict[int, int]
    #: Partitions a cap stopped short of what the buffer holds.
    cut: set[int] = field(default_factory=set)
    schema_id: int = 0
    settling: int = 0
    time_min: int | None = None
    time_max: int | None = None
    content_type: str = "application/octet-stream"


@dataclass(slots=True)
class _Piece:
    partition: int
    seq: int
    batch: pa.RecordBatch
    settling: list[int]
    #: Whether the piece is the last of its partition: taking it whole covers the partition.
    last: bool


def build_answer(
    selection: Selection,
    schema: TailSchema,
    *,
    max_rows: int,
    max_bytes: int,
    fmt: str = "parquet",
) -> Answer:
    """The rows `selection` holds, under `schema`, capped, as one Parquet file (or JSON rows)."""
    if fmt == "json":
        max_rows = min(max_rows, JSON_ROW_LIMIT)
    pieces: list[_Piece] = []
    for partition_selection in selection.partitions.values():
        pieces.extend(_pieces(partition_selection, schema))
    pieces.sort(key=lambda piece: (piece.seq, piece.partition))
    through = {partition: max(item.after, item.next_offset - 1) for partition, item in selection.partitions.items()}
    taken: list[pa.RecordBatch] = []
    settling = 0
    rows = 0
    size = 0
    cut: set[int] = set()
    stopped = False
    for piece in pieces:
        if stopped:
            if piece.partition not in cut:
                cut.add(piece.partition)
                through[piece.partition] = _last_taken(taken, piece.partition, selection.partitions[piece.partition].after)
            continue
        room_rows = max_rows - rows
        per_row = piece.batch.nbytes / max(piece.batch.num_rows, 1)
        room_bytes = int((max_bytes - size) / per_row) if per_row else piece.batch.num_rows
        allowed = min(room_rows, room_bytes)
        if allowed >= piece.batch.num_rows:
            batch = piece.batch
        else:
            # Stop here: this partition's answer ends inside the piece, every other one before it.
            batch = piece.batch.slice(0, max(allowed, 0))
            stopped = True
        if batch.num_rows:
            taken.append(batch)
            offsets = batch.column("_kafka_offset")
            last_offset = offsets[batch.num_rows - 1].as_py()
            settling += sum(1 for offset in piece.settling if offset <= last_offset)
            rows += batch.num_rows
            size += batch.nbytes
        if stopped:
            cut.add(piece.partition)
            through[piece.partition] = _last_taken(taken, piece.partition, selection.partitions[piece.partition].after)
    table = pa.Table.from_batches(taken, schema=schema.arrow_schema) if taken else schema.arrow_schema.empty_table()
    time_min, time_max = _time_bounds(table)
    if fmt == "json":
        body = _json_body(table)
        content_type = "application/json"
    else:
        body = parquet_bytes(table)
        content_type = "application/octet-stream"
    return Answer(
        body=body,
        rows=table.num_rows,
        through=through,
        cut=cut,
        schema_id=schema.schema_id,
        settling=settling,
        time_min=time_min,
        time_max=time_max,
        content_type=content_type,
    )


def parquet_bytes(table: pa.Table) -> bytes:
    """`table` as one Parquet file, as the seal writes one: zstd, statistics on, field ids kept."""
    sink = io.BytesIO()
    pq.write_table(table, sink, compression=DEFAULT_COMPRESSION, write_statistics=True)
    return sink.getvalue()


def rows_batch(rows: list[dict[str, Any]], schema: TailSchema) -> pa.RecordBatch:
    """Built rows as one Arrow batch in the table's schema."""
    table = pa.Table.from_pylist(rows, schema=schema.arrow_schema)
    return table.combine_chunks().to_batches()[0] if table.num_rows else _empty_batch(schema)


def rebuild(lines: list[bytes], schema: TailSchema, *, partition: int) -> tuple[pa.RecordBatch, list[int]]:
    """Staged lines built again under `schema`: their batch, and the offsets of settling rows."""
    rows: list[dict[str, Any]] = []
    settling: list[int] = []
    for line in lines:
        row = build(line, schema, default_partition=partition)
        rows.append(row.built.row)
        if row.settling:
            settling.append(row.built.row["_kafka_offset"])
    return rows_batch(rows, schema), settling


def segment_view(segment: Segment, schema: TailSchema) -> SegmentView:
    """A segment's rows under `schema`: its own batch, or one built again and kept for next time."""
    view = segment.view
    if view.schema_id == schema.schema_id:
        return view
    batch, settling = rebuild(segment.lines(), schema, partition=segment.partition)
    view = SegmentView(batch=batch, schema_id=schema.schema_id, settling=tuple(settling))
    # One attribute, so a thread reading it sees the old view or the new, never half of one.
    segment.view = view
    return view


def _pieces(selection: PartitionSelection, schema: TailSchema) -> list[_Piece]:
    pieces: list[_Piece] = []
    for segment in selection.segments:
        view = segment_view(segment, schema)
        batch = view.batch
        settling = list(view.settling)
        if segment.first_offset <= selection.after:
            mask = pc.greater(batch.column("_kafka_offset"), pa.scalar(selection.after, pa.int64()))
            batch = batch.filter(mask)
            settling = [offset for offset in settling if offset > selection.after]
        if batch.num_rows:
            pieces.append(_Piece(partition=selection.partition, seq=segment.seq, batch=batch, settling=settling, last=False))
    if selection.open_rows:
        if selection.open_schema_id == schema.schema_id:
            batch = rows_batch(selection.open_rows, schema)
            settling = list(selection.open_settling)
        else:
            batch, settling = rebuild(selection.open_lines, schema, partition=selection.partition)
        pieces.append(_Piece(partition=selection.partition, seq=selection.open_seq or 0, batch=batch, settling=settling, last=True))
    if pieces:
        pieces[-1].last = True
    return pieces


def _last_taken(taken: list[pa.RecordBatch], partition: int, after: int) -> int:
    last = after
    for batch in taken:
        partitions = batch.column("_kafka_partition")
        offsets = batch.column("_kafka_offset")
        mask = pc.equal(partitions, pa.scalar(partition, pa.int32()))
        mine = pc.filter(offsets, mask)
        if len(mine):
            last = max(last, pc.max(mine).as_py())
    return last


def _time_bounds(table: pa.Table) -> tuple[int | None, int | None]:
    if table.num_rows == 0:
        return None, None
    column = table.column("_kafka_timestamp")
    if column.null_count == len(column):
        return None, None
    bounds = pc.min_max(column)
    low, high = bounds["min"].as_py(), bounds["max"].as_py()
    return epoch_ms(low), epoch_ms(high)




def _json_body(table: pa.Table) -> bytes:
    import json

    rows = [_jsonable(row) for row in table.to_pylist()]
    return json.dumps({"rows": rows}, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def _jsonable(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    if isinstance(value, dict):
        return {key: _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    return value


def _empty_batch(schema: TailSchema) -> pa.RecordBatch:
    return pa.RecordBatch.from_pylist([], schema=schema.arrow_schema)
