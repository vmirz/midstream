"""Rewrite data files under a table's protection revision, in a process of its own (design §15.3).

    python -m midstream.maintenance_app.reprotect_worker <job.json>

The job names the files to rewrite (`ReprotectPlan`) and carries what decides how: the table's
schema, fork map and protection record, and each field's revision of birth. Each input is read a
batch of rows at a time, projected onto the table's current schema as compaction reads it, and its
rows rewritten as its label's program says (`lakehouse/reprotect.py`): string fields masked in
place, number fields' values masked into their family's string field, withheld ones nulled, JSON
text walked as the writer walks a record. The rows go, in their order, to one output of the input's
own, named with the table's revision ahead of the input's label and uploaded beside it. Prints the
outputs, each with the input it replaces, as JSON.

Like compaction, nothing here touches the table: the outputs are only files until the maintenance
app commits a `replace` snapshot naming them, and a worker killed at any point leaves the table as
it was. Hashes are made with `PROTECTION_KEY`, read from the environment as the maintenance app
reads it; a key whose id is not the one the table's hashes were made with is refused, as is a job
whose table moved to another revision since it was planned.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import sys
from typing import Any
import uuid

from midstream.maintenance_app.compact_worker import (
    COMPACT_PROCESS_NICENESS,
    DEFAULT_ROW_GROUP_ROWS,
    JOB_ID_PATTERN,
    READ_BATCH_ROWS,
    _die_with_parent,
    compaction_work_root,
    confined_path,
)


def run(job: dict[str, Any], *, work_root: str | os.PathLike[str] | None = None) -> dict[str, Any]:
    """Rewrite the job's inputs, each into an output of its own; return them, and what was masked."""
    import pyarrow as pa
    import pyarrow.parquet as pq
    from pyiceberg.io.pyarrow import pyarrow_to_schema

    from midstream.lakehouse.files import partition_text_of, reprotected_file_uri
    from midstream.lakehouse.gcs import parse_gcs_uri, shared_storage_client
    from midstream.lakehouse.lifecycle import schema_from_json
    from midstream.lakehouse.parquet import arrow_schema, project_batch
    from midstream.lakehouse.reprotect import ReprotectDecider, ReprotectPlan, label_rewrite
    from midstream.lakehouse.schema.forkmap import ForkMap
    from midstream.lakehouse.schema.index import build_index
    from midstream.protection.catalog import MASK_HASH
    from midstream.protection.mask import MissingProtectionKey
    from midstream.protection.policy import TableProtection
    from midstream.protection.secrets import protection_key_from_environment

    plan = ReprotectPlan.from_dict(job["plan"])
    if not isinstance(plan.job_id, str) or not JOB_ID_PATTERN.fullmatch(plan.job_id):
        raise ValueError(f"job id {plan.job_id!r} is not one a reprotect plan makes")
    named = job.get("work_dir")
    if not isinstance(named, str) or not named:
        raise ValueError("the job names no work directory")
    work_dir = confined_path(compaction_work_root() if work_root is None else work_root, named)
    work_dir.mkdir(parents=True, exist_ok=True)
    schema = schema_from_json(job["schema"])
    index = build_index(schema, ForkMap.from_dict(job.get("fork_map") or {}), last_column_id=job.get("last_column_id"))
    protection = TableProtection.from_dict(job["protection"])
    births = {int(field_id): int(revision) for field_id, revision in (job.get("births") or {}).items()}
    decider = ReprotectDecider(protection, index, births)
    if decider.revision != plan.revision:
        raise ValueError(f"the table's protection is at revision {decider.revision}, the plan was made at {plan.revision}")
    secret = None
    if protection.policy.hashes or any(item.active and item.mask == MASK_HASH for item in protection.fields.values()):
        key = protection_key_from_environment(os.environ)
        if key is None:
            raise MissingProtectionKey("this maintenance app has no protection key: add protection-key to the auth Secret and restart it")
        if plan.key_id is not None and key.key_id != plan.key_id:
            raise ValueError(
                f"protection_key_mismatch: the table's values were hashed with key {plan.key_id}, this maintenance app holds {key.key_id}"
            )
        secret = key.secret
    target = arrow_schema(schema)
    row_group_rows = int(job.get("row_group_rows") or DEFAULT_ROW_GROUP_ROWS)
    client = shared_storage_client()
    rewrites: dict[tuple[int, ...], Any] = {}
    outputs: list[dict[str, Any]] = []
    for position, item in enumerate(plan.inputs):
        label = item.protection_label
        rewrite = rewrites.get(label)
        if rewrite is None:
            rewrite = rewrites[label] = label_rewrite(decider.program(label), decider, secret=secret)
        bucket_name, name = parse_gcs_uri(item.path)
        local_input = confined_path(work_dir, f"input-{position}.parquet")
        local_output = confined_path(work_dir, f"output-{position}.parquet")
        client.bucket(bucket_name).blob(name).download_to_filename(str(local_input))
        rows = 0
        low = high = None
        pending: list[Any] = []
        pending_rows = 0
        with pq.ParquetWriter(str(local_output), target, compression="zstd", write_statistics=True) as writer, pq.ParquetFile(str(local_input)) as parquet:
            file_schema = pyarrow_to_schema(parquet.schema_arrow)
            for batch in parquet.iter_batches(batch_size=READ_BATCH_ROWS):
                projected = pa.Table.from_batches([project_batch(schema, file_schema, batch)]).cast(target)
                rewritten = rewrite.table(projected)
                stamps = rewritten.column("_kafka_timestamp")
                if rewritten.num_rows:
                    first, last = pa.compute.min(stamps).as_py(), pa.compute.max(stamps).as_py()
                    low = first if low is None or (first is not None and first < low) else low
                    high = last if high is None or (last is not None and last > high) else high
                rows += rewritten.num_rows
                pending.append(rewritten)
                pending_rows += rewritten.num_rows
                if pending_rows >= row_group_rows:
                    writer.write_table(pa.concat_tables(pending), row_group_size=row_group_rows)
                    pending, pending_rows = [], 0
            if pending:
                writer.write_table(pa.concat_tables(pending), row_group_size=row_group_rows)
        local_input.unlink(missing_ok=True)
        output_label = (plan.revision, *label)
        uri = reprotected_file_uri(
            job["location"],
            granularity=job["granularity"],
            partition_value_text=partition_text_of(job["granularity"], item.partition),
            unique=f"{plan.job_id[:16]}-{position}-{uuid.uuid4().hex[:8]}",
            protection_label=output_label,
        )
        out_bucket, out_name = parse_gcs_uri(uri)
        client.bucket(out_bucket).blob(out_name).upload_from_filename(str(local_output), if_generation_match=0)
        outputs.append(
            {
                "input": item.path,
                "uri": uri,
                "partition_key": item.partition_key,
                "partition": list(item.partition),
                "record_count": rows,
                "file_size": local_output.stat().st_size,
                "timestamp_min": None if low is None else int(low.timestamp() * 1_000_000),
                "timestamp_max": None if high is None else int(high.timestamp() * 1_000_000),
                "protection_label": list(output_label),
            }
        )
        local_output.unlink(missing_ok=True)
    counts: dict[str, Any] = {"masked": 0, "withheld": 0, "moved": 0, "by_kind": {}}
    for rewrite in rewrites.values():
        done = rewrite.counts()
        for name in ("masked", "withheld", "moved"):
            counts[name] += done[name]
        for kind_mask, count in done["by_kind"].items():
            counts["by_kind"][kind_mask] = counts["by_kind"].get(kind_mask, 0) + count
    return {"job_id": plan.job_id, "outputs": outputs, "counts": counts}


def main(argv: list[str] | None = None) -> int:
    args = sys.argv[1:] if argv is None else argv
    if len(args) != 1:
        print("usage: reprotect_worker <job.json>", file=sys.stderr)
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
