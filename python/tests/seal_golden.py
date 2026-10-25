"""The golden description of what the seal writes for the cases in `seal_cases.py`.

`describe_output` turns a seal's output into plain JSON - each file's rows, row groups, field ids
and partition - without anything that depends on the pyarrow version, so the golden file
(`golden/seal_rows.json`, written before the shared row builder existed) compares the rows the seal
writes today with those it wrote then. Regenerate it only for a deliberate change to the seal:

    python python/tests/seal_golden.py
"""

from __future__ import annotations

from datetime import datetime
import json
from pathlib import Path
import sys
import tempfile
from typing import Any

import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent))

from seal_cases import golden_evolution, golden_jobs  # noqa: E402

from materializer_control_plane.lakehouse.lifecycle import schema_to_json  # noqa: E402
from materializer_control_plane.runtime_app.writer.seal import SealJob, SealOutput, run_seal  # noqa: E402

GOLDEN_PATH = Path(__file__).resolve().parent / "golden" / "seal_rows.json"
LOCATION = "gs://bucket/develop/mat-0123456789ab/orders"
FIELD_ID = b"PARQUET:field_id"


def seal_job(directory: str, lines: list[dict[str, Any]], *, granularity: str, row_group_bytes: int, after_offset: int | None, name: str = "batch") -> SealJob:
    """The seal job of `lines` against the golden schema, its batch written under `directory`."""
    evolution = golden_evolution()
    path = Path(directory) / f"{name}.jsonl"
    path.write_text("\n".join(json.dumps(item, separators=(",", ":"), ensure_ascii=False) for item in lines) + "\n", encoding="utf-8")
    offsets = [item["m"]["o"] for item in lines]
    return SealJob(
        jsonl_path=str(path),
        output_dir=str(Path(directory) / f"{name}-out"),
        schema=schema_to_json(evolution.schema),
        fork_map=evolution.fork_map.to_dict(),
        granularity=granularity,
        location=LOCATION,
        member_id="member-0",
        partition=lines[0]["m"]["p"],
        offset_start=min(offsets),
        offset_end=max(offsets),
        seal_id="0123456789abcdef0123",
        row_group_bytes=row_group_bytes,
        after_offset=after_offset,
    )


def describe_output(output: SealOutput) -> dict[str, Any]:
    """A seal's output as plain JSON: everything but local paths, with each file's rows."""
    files = []
    for item in output.files:
        parquet = pq.ParquetFile(item["local_path"])
        files.append(
            {
                **{key: value for key, value in item.items() if key not in ("local_path", "file_size")},
                "row_groups": [parquet.metadata.row_group(index).num_rows for index in range(parquet.metadata.num_row_groups)],
                "field_ids": {field.name: int(field.metadata[FIELD_ID]) for field in parquet.schema_arrow},
                "rows": [_plain(row) for row in parquet.read().to_pylist()],
            }
        )
    return {"rows": output.rows, "unmapped_values": output.unmapped_values, "schema_id": output.schema_id, "files": files}


def golden_descriptions() -> dict[str, Any]:
    """Every golden case sealed by the current `run_seal`, described."""
    described: dict[str, Any] = {}
    for name, lines, granularity, row_group_bytes, after_offset in golden_jobs():
        with tempfile.TemporaryDirectory() as directory:
            job = seal_job(directory, lines, granularity=granularity, row_group_bytes=row_group_bytes, after_offset=after_offset)
            described[name] = describe_output(run_seal(job))
    return described


def _plain(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: _plain(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_plain(item) for item in value]
    return value


if __name__ == "__main__":
    GOLDEN_PATH.parent.mkdir(parents=True, exist_ok=True)
    GOLDEN_PATH.write_text(json.dumps(golden_descriptions(), indent=1, sort_keys=True, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"wrote {GOLDEN_PATH}")
