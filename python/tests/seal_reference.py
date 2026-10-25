"""The seal as it was before the shared row builder (4.0.0, 5b45cda), kept verbatim.

`lakehouse/rows.py::build_row` took the seal's row construction out of `run_seal` so that the live
tail builds its rows with the very same code. That move must not change a byte the seal writes:
`test_runtime_seal.py` seals the golden cases (`seal_cases.py`) with both this copy and the current
`run_seal` and compares the files byte for byte. Change this file only together with a deliberate
change to what the seal writes.
"""

from __future__ import annotations

from datetime import UTC, datetime
import json
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from midstream.lakehouse.files import data_file_uri, partition_text, partition_values
from midstream.lakehouse.lifecycle import schema_from_json
from midstream.lakehouse.parquet import DEFAULT_COMPRESSION, arrow_schema
from midstream.lakehouse.schema.conform import Conformer, unmapped_text
from midstream.lakehouse.schema.forkmap import ForkMap
from midstream.runtime_app.writer.seal import SealJob, SealOutput


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


def reference_run_seal(job: SealJob) -> SealOutput:
    schema = schema_from_json(job.schema)
    conformer = Conformer(schema, ForkMap.from_dict(job.fork_map))
    target_schema = arrow_schema(schema)
    output_dir = Path(job.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    groups: dict[str | None, _PartitionFile] = {}
    unmapped_values = 0
    rows = 0
    with open(job.jsonl_path, "rb") as handle:
        for raw in handle:
            line = raw.strip()
            if not line:
                continue
            staged = json.loads(line)
            metadata = staged.get("m") or {}
            offset = int(metadata["o"])
            if job.after_offset is not None and offset <= job.after_offset:
                continue
            payload, unmapped = conformer.conform(staged.get("v"))
            if unmapped:
                unmapped_values += len(unmapped)
            moment = _parse_timestamp(metadata.get("t"))
            row = {
                "_kafka_timestamp": moment,
                "_kafka_partition": int(metadata.get("p", job.partition)),
                "_kafka_offset": offset,
                "_kafka_key": metadata.get("k"),
                "_kafka_headers": [tuple(pair) for pair in metadata.get("h") or []] or None,
                "_materialized_at": _parse_timestamp(metadata.get("a")) or datetime.now(tz=UTC),
                "_unmapped": unmapped_text(unmapped),
                **payload,
            }
            key = partition_text(job.granularity, moment)
            group = groups.get(key)
            if group is None:
                group = groups[key] = _PartitionFile(output_dir / f"{job.seal_id}-{len(groups)}.parquet", target_schema, len(groups))
            group.add(row, staged_bytes=len(line))
            if group.buffered_bytes >= job.row_group_bytes:
                group.flush()
            rows += 1
    if rows == 0:
        raise ValueError("cannot seal an empty batch")
    files: list[dict[str, Any]] = []
    for text, group in sorted(groups.items(), key=lambda item: item[0] or ""):
        size = group.close()
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
    return SealOutput(files=files, rows=rows, unmapped_values=unmapped_values, schema_id=schema.schema_id)


def _parse_timestamp(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    text = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        moment = datetime.fromisoformat(text)
    except ValueError:
        return None
    return moment if moment.tzinfo is not None else moment.replace(tzinfo=UTC)


def _format(moment: datetime) -> str:
    return moment.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")
