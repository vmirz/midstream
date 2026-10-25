"""Sealing a batch: its staged records, conformed to the table's schema, as Iceberg Parquet files.

Pure file conversion, run in a child process (`seal_worker.py`) so its seconds of Python work never
hold the writer's event loop (spec §7.3):

1. split the staged JSONL lines by partition value - the month (day, or hour) of the Kafka timestamp,
   lines without one on their own - into one spool file per value, in their order; for a re-seal,
   only the lines after an offset. Only the timestamp is read here: no row is built yet;
2. for each partition value in turn, turn each spooled line into a row (`lakehouse/rows.py::build_row`,
   which the live tail shares): each value conformed to the schema (`lakehouse/schema/conform.py`) -
   into the field that takes it, anything no field takes into `_unmapped` - beside the Kafka
   metadata columns, and write them as one zstd Parquet file per partition value, field ids in its
   schema, statistics on.

Rows are written a row group at a time - about `row_group_bytes` of staged JSON each - and one
partition's file at a time, so the child's memory is bounded by one row group whatever the batch's
size or the spread of its timestamps. Written as the rows arrived, with every partition's file open
at once and each flushing on its own, a batch spread over many months - a backfill - held every row
until the end: 523 MiB for 50,000 rows over ten months, 2.1 GiB for 250,000 over two years, where
one month's worth stays under 300 MiB.

The result names each file with its record count, size, event-time bounds and final object URI.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
import json
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from materializer_control_plane.lakehouse.files import data_file_uri, partition_text, partition_values
from materializer_control_plane.lakehouse.lifecycle import schema_from_json
from materializer_control_plane.lakehouse.parquet import DEFAULT_COMPRESSION, arrow_schema
from materializer_control_plane.lakehouse.rows import build_row, parse_timestamp
from materializer_control_plane.lakehouse.schema.conform import Conformer
from materializer_control_plane.lakehouse.schema.forkmap import ForkMap


@dataclass(frozen=True, slots=True)
class SealJob:
    jsonl_path: str
    output_dir: str
    schema: dict[str, Any]
    fork_map: dict[str, Any]
    granularity: str
    location: str
    member_id: str
    partition: int
    offset_start: int
    offset_end: int
    seal_id: str
    row_group_bytes: int = 32 * 1024 * 1024
    #: Rows at or below this offset are left out (a re-seal after an overlap); None keeps all.
    after_offset: int | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "jsonl_path": self.jsonl_path,
            "output_dir": self.output_dir,
            "schema": self.schema,
            "fork_map": self.fork_map,
            "granularity": self.granularity,
            "location": self.location,
            "member_id": self.member_id,
            "partition": self.partition,
            "offset_start": self.offset_start,
            "offset_end": self.offset_end,
            "seal_id": self.seal_id,
            "row_group_bytes": self.row_group_bytes,
            "after_offset": self.after_offset,
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "SealJob":
        return cls(**{key: value[key] for key in cls.__dataclass_fields__ if key in value})


@dataclass(frozen=True, slots=True)
class SealOutput:
    files: list[dict[str, Any]] = field(default_factory=list)
    rows: int = 0
    unmapped_values: int = 0
    schema_id: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {"files": self.files, "rows": self.rows, "unmapped_values": self.unmapped_values, "schema_id": self.schema_id}

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "SealOutput":
        return cls(
            files=list(value.get("files") or []),
            rows=int(value.get("rows") or 0),
            unmapped_values=int(value.get("unmapped_values") or 0),
            schema_id=int(value.get("schema_id") or 0),
        )


#: Spooled lines held in memory before they are appended to their spool files, all partitions together.
SPOOL_BUFFER_BYTES = 8 * 1024 * 1024


class _Spool:
    """The staged lines of one partition value, in their order, on disk until their file is written.

    Lines are appended in memory and written out together once every spool holds
    `SPOOL_BUFFER_BYTES` between them, so a batch spread over hundreds of partition values opens
    one file at a time rather than one handle per value.
    """

    def __init__(self, path: Path, index: int) -> None:
        self.path = path
        self.index = index
        self.pending = bytearray()
        self.lines = 0

    def add(self, line: bytes) -> None:
        self.pending += line
        self.pending += b"\n"
        self.lines += 1

    def flush(self) -> None:
        if not self.pending:
            return
        with open(self.path, "ab") as handle:
            handle.write(self.pending)
        self.pending = bytearray()


class _PartitionFile:
    """One output file, for one partition value, written a row group at a time."""

    def __init__(self, path: Path, schema: pa.Schema, index: int) -> None:
        self.path = path
        self.schema = schema
        self.index = index
        self.writer: pq.ParquetWriter | None = None
        self.rows: list[dict[str, Any]] = []
        self.buffered_bytes = 0
        self.record_count = 0
        self.offset_min: int | None = None
        self.offset_max: int | None = None
        self.moment_min: datetime | None = None
        self.moment_max: datetime | None = None

    def add(self, row: dict[str, Any], *, staged_bytes: int) -> None:
        self.rows.append(row)
        self.buffered_bytes += staged_bytes
        self.record_count += 1
        offset = row["_kafka_offset"]
        self.offset_min = offset if self.offset_min is None else min(self.offset_min, offset)
        self.offset_max = offset if self.offset_max is None else max(self.offset_max, offset)
        moment = row["_kafka_timestamp"]
        if moment is not None:
            self.moment_min = moment if self.moment_min is None else min(self.moment_min, moment)
            self.moment_max = moment if self.moment_max is None else max(self.moment_max, moment)

    def flush(self) -> None:
        """Write the buffered rows as one row group."""
        if not self.rows:
            return
        table = pa.Table.from_pylist(self.rows, schema=self.schema)
        if self.writer is None:
            self.writer = pq.ParquetWriter(str(self.path), self.schema, compression=DEFAULT_COMPRESSION, write_statistics=True)
        self.writer.write_table(table, row_group_size=table.num_rows)
        self.rows = []
        self.buffered_bytes = 0

    def close(self) -> int:
        self.flush()
        if self.writer is not None:
            self.writer.close()
        return self.path.stat().st_size


def run_seal(job: SealJob) -> SealOutput:
    schema = schema_from_json(job.schema)
    conformer = Conformer(schema, ForkMap.from_dict(job.fork_map))
    target_schema = arrow_schema(schema)
    output_dir = Path(job.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Pass one: each line to its partition value's spool, in order. Only the timestamp is read.
    spools: dict[str | None, _Spool] = {}
    pending_bytes = 0
    with open(job.jsonl_path, "rb") as handle:
        for raw in handle:
            line = raw.strip()
            if not line:
                continue
            metadata = json.loads(line).get("m") or {}
            if job.after_offset is not None and int(metadata["o"]) <= job.after_offset:
                continue
            key = partition_text(job.granularity, parse_timestamp(metadata.get("t")))
            spool = spools.get(key)
            if spool is None:
                spool = spools[key] = _Spool(output_dir / f"{job.seal_id}-{len(spools)}.jsonl", len(spools))
            spool.add(line)
            pending_bytes += len(line) + 1
            if pending_bytes >= SPOOL_BUFFER_BYTES:
                for each in spools.values():
                    each.flush()
                pending_bytes = 0
    for each in spools.values():
        each.flush()
    rows = sum(spool.lines for spool in spools.values())
    if rows == 0:
        raise ValueError("cannot seal an empty batch")

    # Pass two: one partition's file at a time, from its spool, a row group at a time.
    files: list[dict[str, Any]] = []
    unmapped_values = 0
    try:
        for text, spool in sorted(spools.items(), key=lambda item: item[0] or ""):
            group = _PartitionFile(output_dir / f"{job.seal_id}-{spool.index}.parquet", target_schema, spool.index)
            with open(spool.path, "rb") as handle:
                for raw in handle:
                    line = raw.strip()
                    if not line:
                        continue
                    built = build_row(json.loads(line), conformer=conformer, default_partition=job.partition)
                    unmapped_values += built.unmapped_values
                    group.add(built.row, staged_bytes=len(line))
                    if group.buffered_bytes >= job.row_group_bytes:
                        group.flush()
            size = group.close()
            spool.path.unlink()
            files.append(
                {
                    "local_path": str(group.path),
                    "uri": data_file_uri(
                        job.location,
                        granularity=job.granularity,
                        partition_value_text=text,
                        kafka_partition=job.partition,
                        offset_start=group.offset_min,
                        offset_end=group.offset_max,
                        member_id=job.member_id,
                        unique=f"{job.seal_id[:16]}-{group.index}",
                    ),
                    "partition_text": text,
                    "partition_value": partition_values(job.granularity, text),
                    "record_count": group.record_count,
                    "file_size": size,
                    "event_time_min": None if group.moment_min is None else _format(group.moment_min),
                    "event_time_max": None if group.moment_max is None else _format(group.moment_max),
                }
            )
    finally:
        # A seal that failed part way leaves no spool behind beside its files.
        for spool in spools.values():
            spool.path.unlink(missing_ok=True)
    return SealOutput(files=files, rows=rows, unmapped_values=unmapped_values, schema_id=schema.schema_id)


def _format(moment: datetime) -> str:
    return moment.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")
