"""Compact small files - one partition's, or several partitions' - in a process of its own (spec §7.9).

    python -m materializer_control_plane.maintenance_app.compact_worker <job.json>

Reads the inputs one at a time, a batch of rows at a time - each projected onto the table's current
schema by field id, so older files with fewer fields read correctly - and accumulates them, one
partition at a time, up to a memory budget, which a large input may cross in its middle. Each
accumulation is sorted by `_kafka_timestamp, _kafka_partition, _kafka_offset` and appended to the
partition's open output a row group at a time, zstd, field ids in its schema; an output is closed
and uploaded to its partition's `data/` location, under a name no other file has, once it reaches
the target file size, when its partition ends, or at the end. So the worker holds about the budget
plus one row group however large an input is, and writes files of the target size however small
the budget: each is a run of sorted row groups, the inputs taken oldest first. Prints the outputs,
each with its partition, as JSON.

Nothing here touches the table: the outputs are only files until the maintenance app commits a
`replace` snapshot naming them, so a worker killed at any point leaves the table exactly as it was
(its uploaded outputs are orphans, removed by orphan cleanup). On Linux the worker asks to be killed
when its parent dies, so it never outlives the maintenance member that started it.

The job file is the maintenance app's own, but the worker does not trust it with the filesystem: the work
directory it names must lie inside the compaction work root, the job id must be one the planner
makes, and the worker names every local file itself. A malformed or tampered job is refused before
anything is read or written.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import re
import signal
import sys
from typing import Any
import uuid

#: Niceness added to the worker: the member's own work - ticks, commits, other tables - keeps its core.
COMPACT_PROCESS_NICENESS = 10
#: Arrow bytes accumulated before an output file is written: with inputs read a batch at a time and
#: outputs written a row group at a time, the worker holds about this plus one row group.
DEFAULT_MEMORY_BUDGET_BYTES = 128 * 1024 * 1024
DEFAULT_ROW_GROUP_ROWS = 100_000
#: An output is closed once it is this large (`compaction_target_file_size_bytes`).
DEFAULT_TARGET_FILE_SIZE_BYTES = 256 * 1024 * 1024
#: Rows decoded from an input at a time. A small file can decode to eight times its size, so one
#: decoded whole could hold several times the budget by itself (E13).
READ_BATCH_ROWS = 10_000
#: Every compaction works in a directory of its own under `<staging_root_path>/maintenance`.
WORK_ROOT_NAME = "maintenance"
#: A job id is what `plan_compaction` makes it: `uuid.uuid4().hex`.
JOB_ID_PATTERN = re.compile(r"[0-9a-f]{32}")


def compaction_work_root() -> Path:
    """The directory compactions work under: the maintenance app makes each job's directory in it."""
    from materializer_control_plane.runtime_app.config import get_runtime_app_config

    return Path(get_runtime_app_config().staging_root_path) / WORK_ROOT_NAME


def confined_path(base: str | os.PathLike[str], name: str) -> Path:
    """`name` resolved under `base`; ValueError unless it lies strictly inside it.

    Resolving follows `..` and symlinks, so neither can lead out; an absolute `name` must still
    resolve inside `base`.
    """
    root = os.path.realpath(base)
    candidate = os.path.realpath(os.path.join(root, name))
    if candidate == root or os.path.commonpath([root, candidate]) != root:
        raise ValueError(f"{name!r} is not inside {root}")
    return Path(candidate)


def _die_with_parent() -> None:
    if not sys.platform.startswith("linux"):
        return
    try:
        import ctypes

        libc = ctypes.CDLL("libc.so.6", use_errno=True)
        PR_SET_PDEATHSIG = 1
        libc.prctl(PR_SET_PDEATHSIG, signal.SIGKILL)
    except Exception:  # noqa: BLE001 - best effort; the maintenance app also kills the worker itself
        pass


def run(job: dict[str, Any], *, work_root: str | os.PathLike[str] | None = None) -> dict[str, Any]:
    """Compact the job's inputs into its outputs; return them.

    `work_root` is where the job's work directory must lie: the compaction work root unless a test
    or a tool names another.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq
    from pyiceberg.io.pyarrow import pyarrow_to_schema

    from materializer_control_plane.lakehouse.files import compacted_file_uri, partition_text_of
    from materializer_control_plane.lakehouse.gcs import parse_gcs_uri, shared_storage_client
    from materializer_control_plane.lakehouse.lifecycle import schema_from_json
    from materializer_control_plane.lakehouse.maintenance import CompactionPlan
    from materializer_control_plane.lakehouse.parquet import arrow_schema, project_batch

    plan = CompactionPlan.from_dict(job["plan"])
    if not isinstance(plan.job_id, str) or not JOB_ID_PATTERN.fullmatch(plan.job_id):
        raise ValueError(f"job id {plan.job_id!r} is not one a compaction plan makes")
    named = job.get("work_dir")
    if not isinstance(named, str) or not named:
        raise ValueError("the job names no work directory")
    work_dir = confined_path(compaction_work_root() if work_root is None else work_root, named)
    schema = schema_from_json(job["schema"])
    target = arrow_schema(schema)
    work_dir.mkdir(parents=True, exist_ok=True)
    budget = int(job.get("memory_budget_bytes") or DEFAULT_MEMORY_BUDGET_BYTES)
    row_group_rows = int(job.get("row_group_rows") or DEFAULT_ROW_GROUP_ROWS)
    target_bytes = int(job.get("target_file_size_bytes") or DEFAULT_TARGET_FILE_SIZE_BYTES)
    client = shared_storage_client()
    outputs: list[dict[str, Any]] = []
    chunk: list[Any] = []
    chunk_bytes = 0
    # The output being written: its writer, and what it holds so far.
    output: dict[str, Any] | None = None
    # The partition being accumulated: the plan's inputs come grouped by partition.
    partition: tuple[Any, ...] = plan.partition
    partition_key = plan.partition_key
    sort_keys = [("_kafka_timestamp", "ascending"), ("_kafka_partition", "ascending"), ("_kafka_offset", "ascending")]

    def bound(current: Any, value: Any, pick) -> Any:
        return value if current is None else current if value is None else pick(current, value)

    def flush() -> None:
        """Sort what is accumulated and append it to the open output, a row group at a time."""
        nonlocal chunk, chunk_bytes, output
        if not chunk:
            return
        table = pa.concat_tables(chunk)
        chunk, chunk_bytes = [], 0
        # The order is computed once and the rows taken a row group at a time: sorting the whole
        # accumulation would hold it twice.
        order = pa.compute.sort_indices(table, sort_keys=sort_keys)
        if output is None:
            index = len(outputs)
            local = confined_path(work_dir, f"output-{index}.parquet")
            writer = pq.ParquetWriter(str(local), table.schema, compression="zstd", write_statistics=True)
            output = {"index": index, "local": local, "writer": writer, "rows": 0, "low": None, "high": None, "first": None, "last": None}
        for start in range(0, table.num_rows, row_group_rows):
            output["writer"].write_table(table.take(order.slice(start, row_group_rows)), row_group_size=row_group_rows)
        stamps, offsets = table.column("_kafka_timestamp"), table.column("_kafka_offset")
        output["rows"] += table.num_rows
        output["low"] = bound(output["low"], pa.compute.min(stamps).as_py(), min)
        output["high"] = bound(output["high"], pa.compute.max(stamps).as_py(), max)
        output["first"] = bound(output["first"], pa.compute.min(offsets).as_py(), min)
        output["last"] = bound(output["last"], pa.compute.max(offsets).as_py(), max)
        del table, order, stamps, offsets
        pa.default_memory_pool().release_unused()
        if output["local"].stat().st_size >= target_bytes:
            close()

    def close() -> None:
        """Finish the open output and upload it."""
        nonlocal output
        if output is None:
            return
        current, output = output, None
        current["writer"].close()
        uri = compacted_file_uri(
            job["location"],
            granularity=job["granularity"],
            partition_value_text=partition_text_of(job["granularity"], partition),
            # Unique to this run: a plan run again never collides with an earlier run's outputs.
            unique=f"{plan.job_id[:16]}-{current['index']}-{uuid.uuid4().hex[:8]}",
        )
        bucket_name, name = parse_gcs_uri(uri)
        client.bucket(bucket_name).blob(name).upload_from_filename(str(current["local"]), if_generation_match=0)
        low, high = current["low"], current["high"]
        outputs.append(
            {
                "uri": uri,
                "partition_key": partition_key,
                "partition": list(partition),
                "record_count": current["rows"],
                "file_size": current["local"].stat().st_size,
                "timestamp_min": None if low is None else int(low.timestamp() * 1_000_000),
                "timestamp_max": None if high is None else int(high.timestamp() * 1_000_000),
                "offset_min": current["first"],
                "offset_max": current["last"],
            }
        )
        current["local"].unlink(missing_ok=True)

    for position, item in enumerate(plan.inputs):
        if item.partition_key != partition_key:
            # A file never mixes partitions: the previous one's output is finished first.
            flush()
            close()
            partition, partition_key = item.partition, item.partition_key
        bucket_name, name = parse_gcs_uri(item.path)
        local_input = confined_path(work_dir, f"input-{position}.parquet")
        client.bucket(bucket_name).blob(name).download_to_filename(str(local_input))
        with pq.ParquetFile(str(local_input)) as parquet:
            file_schema = pyarrow_to_schema(parquet.schema_arrow)
            for batch in parquet.iter_batches(batch_size=READ_BATCH_ROWS):
                projected = pa.Table.from_batches([project_batch(schema, file_schema, batch)]).cast(target)
                chunk.append(projected)
                chunk_bytes += projected.nbytes
                if chunk_bytes >= budget:
                    flush()
        local_input.unlink(missing_ok=True)
    flush()
    close()
    return {"job_id": plan.job_id, "outputs": outputs}


def main(argv: list[str] | None = None) -> int:
    args = sys.argv[1:] if argv is None else argv
    if len(args) != 1:
        print("usage: compact_worker <job.json>", file=sys.stderr)
        return 2
    _die_with_parent()
    try:
        os.nice(COMPACT_PROCESS_NICENESS)
    except (AttributeError, OSError):
        pass
    try:
        result = run(json.loads(Path(args[0]).read_text(encoding="utf-8")))
    except Exception as exc:
        print(f"{type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    sys.exit(main())
