"""Write the console's fixture tables, `console/testdata/iceberg/{orders,payments}`, with the control plane's library.

They are the two tables of one materialization, `shop`, reading the topics `orders` and `payments`.

- `orders`: three appends over two days, a schema change that forks `total` (long -> double) and
  `customer.phone` (string -> struct) and adds `coupon`, a fourth append under the new schema, and
  a compaction of the first day.
- `payments`: one append of six payments, one for each of the orders O-0 to O-5, for joins.

Every object of a table is written under `<out>/<table>/` with its path below the table's `gs://`
location kept, which the console's tests read it back from (`web/server/iceberg/testing.ts`).
Each `expected.json` records what PyIceberg itself says the current snapshot holds and the control
plane's projection of each schema (E5).

Each table also gets its live tail: the records past its committed offsets as the tail app serves
them (handbook 19), built by the tail app's own code - its buffer, the seal's row builder, its
answers - and written under `<out>/<table>/tail/` with what each answer's headers say, in
`expected.json`'s `tail`. Orders' comes in three fetches, each past the last one's `through`, the
third with nothing new; payments' holds the payments of two orders in orders' tail, for a join
across two tails.

    python python/tests/console_fixture.py [--out console/testdata/iceberg]

The console's tests compare against values this writes, and every run makes new ids, so commit the
tables and their `expected.json` together.
"""

from __future__ import annotations

import argparse
from datetime import UTC, datetime, timedelta
import io
import json
from pathlib import Path
import shutil
import sys
import tempfile

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "python" / "src"))
sys.path.insert(0, str(REPO / "python" / "tests"))

from lakehouse_support import LOCATION, MATERIALIZATION_LOCATION, NAMESPACE, LakeFixture  # noqa: E402
from pyiceberg.typedef import Record  # noqa: E402

from materializer_control_plane.lakehouse.commits import commit_schema  # noqa: E402
from materializer_control_plane.lakehouse.files import build_data_file, read_parquet_footer  # noqa: E402
from materializer_control_plane.lakehouse.gcs import set_shared_storage_client  # noqa: E402
from materializer_control_plane.lakehouse.lifecycle import schema_to_json  # noqa: E402
from materializer_control_plane.lakehouse.maintenance import commit_compaction, live_data_files, plan_compaction  # noqa: E402
from materializer_control_plane.lakehouse.offsets import committed_offsets  # noqa: E402
from materializer_control_plane.lakehouse.schema.forkmap import ForkMap  # noqa: E402
from materializer_control_plane.lakehouse.schema.infer import infer  # noqa: E402
from materializer_control_plane.lakehouse.schema.policy import evolve  # noqa: E402
from materializer_control_plane.lakehouse.schema.render import render_schema  # noqa: E402
from materializer_control_plane.maintenance_app.compact_worker import run as compact  # noqa: E402
from materializer_control_plane.tail_app.buffer import TableBuffer  # noqa: E402
from materializer_control_plane.tail_app.ingest import TailSchema, build, decode_value, epoch_ms, stage  # noqa: E402
from materializer_control_plane.tail_app.serve import build_answer, rows_batch  # noqa: E402
from tail_support import Record as KafkaRecord  # noqa: E402

import pyarrow.parquet as pq  # noqa: E402

DAY1 = datetime(2026, 10, 17, 10, 0, tzinfo=UTC)
DAY2 = datetime(2026, 10, 18, 10, 0, tzinfo=UTC)
MATERIALIZATION_ID = "mat-a1b2c3d4e5f6"


def value(offset: int, *, v2: bool = False) -> dict:
    """One order record; `v2` is the shape after the schema change."""
    base = {
        "order_id": f"O-{offset}",
        "total": offset * 10,
        "status": "confirmed" if offset % 2 else "pending",
        "customer": {"email": f"c{offset}@example.com", "phone": "+15550100"},
        "items": [{"sku": f"S{offset}", "qty": 1}, {"sku": f"T{offset}", "qty": 2}],
    }
    if v2:
        base["total"] = offset * 10 + 0.5
        base["coupon"] = f"SAVE{offset}"
        base["customer"]["phone"] = {"country": "1", "number": "5550100"}
    return base


PAYMENTS_LOCATION = f"{MATERIALIZATION_LOCATION}/payments"


def payment(offset: int) -> dict:
    """One payment, for the order of the same number."""
    return {
        "payment_id": f"P-{offset}",
        "order_id": f"O-{offset}",
        "amount": offset * 10 + 0.25,
        "method": "card" if offset % 2 else "transfer",
    }


def write_tail(lake: LakeFixture, target: Path, *, source_topic: str, fetches: list[list[tuple[int, int, dict, datetime]]]) -> dict:
    """The table's live tail past its committed offsets, fetch by fetch, as the tail app answers.

    Each fetch's records - (partition, offset, value, Kafka timestamp) - go into the tail app's
    buffer as its ingest puts them there (segments closed every two rows, so an answer holds closed
    segments and open rows), and the answer past the last one's `through` is written as
    `tail/tail-<n>.parquet`, with what its headers would say."""
    table = lake.load()
    fork_map = ForkMap.from_json(table.metadata.properties.get("materializer.fork-map"))
    schema = TailSchema(
        schema_id=table.schema().schema_id,
        iceberg_schema=schema_to_json(table.schema()),
        fork_map=fork_map.to_dict(),
        last_column_id=table.metadata.last_column_id,
    )
    committed = {int(partition): offset for partition, offset in committed_offsets(table.metadata).offsets.items()}
    buffer = TableBuffer()
    partitions = sorted({partition for records in fetches for partition, _, _, _ in records} | set(committed))
    for partition in partitions:
        buffer.start(partition, committed=committed.get(partition, -1), start=committed.get(partition, -1) + 1)
    after = dict(committed)
    ends: dict[int, int] = dict(committed)
    answers = []
    (target / "tail").mkdir(parents=True, exist_ok=True)
    for index, records in enumerate(fetches):
        for partition, offset, payload, moment in records:
            record = KafkaRecord(
                topic=source_topic,
                partition=partition,
                offset=offset,
                timestamp=epoch_ms(moment),
                key=f"K{offset}".encode(),
                value=json.dumps(payload).encode(),
            )
            line = stage(record, decode_value(record.value), materialization_id=MATERIALIZATION_ID, source_topic=source_topic)
            row = build(line, schema, default_partition=partition)
            buffer.append(partition, offset=offset, line=line, row=row.built.row, moment=epoch_ms(row.built.moment), schema_id=schema.schema_id, settling=row.settling)
            ends[partition] = max(ends.get(partition, -1), offset)
            segment = buffer.partitions[partition].open
            if segment is not None and len(segment.rows) >= 2:
                buffer.close(partition, batch=rows_batch(segment.rows, schema), schema_id=segment.schema_id, settling=tuple(segment.settling))
        answer = build_answer(buffer.select(after), schema, max_rows=10**9, max_bytes=1 << 40)
        name = f"tail/tail-{index}.parquet"
        (target / name).write_bytes(answer.body)
        served = sorted(pq.read_table(io.BytesIO(answer.body)).to_pylist(), key=lambda r: (r["_kafka_partition"], r["_kafka_offset"]))
        answers.append(
            {
                "file": name,
                "after": {str(p): o for p, o in sorted(after.items())},
                "through": {str(p): o for p, o in sorted(answer.through.items())},
                "end_offsets": {str(p): o for p, o in sorted(ends.items())},
                "rows": answer.rows,
                "bytes": len(answer.body),
                "schema_id": answer.schema_id,
                "settling": answer.settling,
                "time_min": answer.time_min,
                "time_max": answer.time_max,
                "served": [{key: value for key, value in served_row.items() if value is not None and key != "_materialized_at"} for served_row in served],
            }
        )
        after = {partition: max(after.get(partition, -1), offset) for partition, offset in {**after, **answer.through}.items()}
    return {"schema_id": schema.schema_id, "committed": {str(p): o for p, o in sorted(committed.items())}, "answers": answers}


def finish(target: Path, expected: dict) -> dict:
    (target / "expected.json").write_text(json.dumps(expected, indent=1, default=str) + "\n")
    return expected


def write_payments(out: Path) -> dict:
    """The `payments` table: one append of six payments. Return its `expected.json`."""
    lake = LakeFixture(payment(0), granularity="day", location=PAYMENTS_LOCATION, identifier=(NAMESPACE, "payments"), source_topic="payments")
    set_shared_storage_client(lake.client)
    try:
        offsets = list(range(6))
        lake.commit(
            lake.write_seal(
                partition=0,
                after_offset=-1,
                offsets=offsets,
                values=[payment(o) for o in offsets],
                times=[DAY1 + timedelta(minutes=o, seconds=30) for o in offsets],
            )
        )
        expected = copy_table(lake, out / "payments", PAYMENTS_LOCATION)
        # Payments of two orders that are in orders' tail: a join across the two tails finds them.
        expected["tail"] = write_tail(
            lake,
            out / "payments",
            source_topic="payments",
            fetches=[[(0, 6, payment(12), DAY2 + timedelta(minutes=32)), (0, 7, payment(13), DAY2 + timedelta(minutes=33))]],
        )
        return finish(out / "payments", expected)
    finally:
        lake.close()


def write_fixture(out: Path) -> dict:
    """Build both tables in memory, copy their objects under `out/<table>`, and return orders' `expected.json`."""
    write_payments(out)
    lake = LakeFixture(value(0), granularity="day")
    set_shared_storage_client(lake.client)
    try:
        after = -1
        for offsets, day in (([0, 1, 2, 3], DAY1), ([4, 5, 6], DAY1), ([7, 8, 9], DAY2)):
            seal = lake.write_seal(
                partition=0,
                after_offset=after,
                offsets=offsets,
                values=[value(o) for o in offsets],
                times=[day + timedelta(minutes=o) for o in offsets],
            )
            lake.commit(seal)
            after = offsets[-1]
        table = lake.load()
        evolution = evolve(
            table.schema(),
            ForkMap.from_json(table.metadata.properties["materializer.fork-map"]),
            [((), infer(value(10, v2=True)))],
            last_column_id=table.metadata.last_column_id,
        )
        commit_schema(table, evolution)
        offsets = [10, 11]
        lake.commit(
            lake.write_seal(
                partition=1,
                after_offset=-1,
                offsets=offsets,
                values=[value(o, v2=True) for o in offsets],
                times=[DAY2 + timedelta(minutes=o) for o in offsets],
            )
        )

        table = lake.load()
        plan = plan_compaction(
            table,
            now=datetime.now(tz=UTC) + timedelta(days=1),
            min_input_files=2,
            small_file_bytes=64 << 20,
            max_input_bytes=1 << 30,
            hot_partition_delay_minutes=0,
            # Day two stays as written, so the table holds a compacted file and uncompacted ones.
            skip=("2026-10-18",),
        )
        if plan is None or plan.partition_keys != ("2026-10-17",):
            raise SystemExit(f"expected a compaction of 2026-10-17 alone, planned {plan}")
        # The worker works only inside a work root: this one is the fixture's own.
        work_root = Path(tempfile.mkdtemp())
        try:
            result = compact(
                {"plan": plan.to_dict(), "schema": schema_to_json(table.schema()), "work_dir": "compact-job", "location": LOCATION, "granularity": "day"},
                work_root=work_root,
            )
        finally:
            shutil.rmtree(work_root, ignore_errors=True)
        outputs = []
        for output in result["outputs"]:
            footer, size = read_parquet_footer(lake.client, output["uri"])
            outputs.append(
                build_data_file(
                    table.metadata,
                    file_path=output["uri"],
                    file_size=size,
                    parquet_metadata=footer,
                    granularity="day",
                    partition=Record(*output["partition"]),
                )
            )
        commit_compaction(
            table,
            plan,
            outputs,
            runtime="fixture",
            output_bounds=[(o["timestamp_min"], o["timestamp_max"]) for o in result["outputs"]],
        )

        expected = copy_table(lake, out / "orders", LOCATION)
        # Records past the committed offsets ({0: 9, 1: 11}) in the newest shape, in two fetches.
        moment = lambda offset: DAY2 + timedelta(minutes=20 + offset)  # noqa: E731
        expected["tail"] = write_tail(
            lake,
            out / "orders",
            source_topic="orders",
            fetches=[
                [(0, 10, value(10, v2=True), moment(10)), (0, 11, value(11, v2=True), moment(11)), (1, 12, value(12, v2=True), moment(12)), (0, 12, value(20, v2=True), moment(13))],
                [(1, 13, value(13, v2=True), moment(14)), (0, 13, value(21, v2=True), moment(15))],
                # Nothing new: an empty answer, still a Parquet file, as the tail app sends one.
                [],
            ],
        )
        return finish(out / "orders", expected)
    finally:
        lake.close()


def copy_table(lake: LakeFixture, target: Path, location: str) -> dict:
    """Copy a table's objects under `target`, write its `expected.json` there, and return it."""
    table = lake.load()
    if target.exists():
        shutil.rmtree(target)
    prefix = location.removeprefix("gs://")
    for (bucket, name), stored in lake.store.objects.items():
        path = f"{bucket}/{name}"
        if not path.startswith(f"{prefix}/"):
            continue
        destination = target / path[len(prefix) + 1 :]
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(stored.data)

    files = [
        {
            "path": item.path,
            "record_count": item.record_count,
            "file_size": item.file_size,
            "partition_key": item.partition_key,
            "timestamp_min": item.timestamp_min,
            "timestamp_max": item.timestamp_max,
        }
        for item in sorted(live_data_files(table), key=lambda f: f.path)
    ]
    fork_map = ForkMap.from_json(table.metadata.properties.get("materializer.fork-map"))
    expected = {
        "location": location,
        "metadata_location": table.metadata_location,
        "current_snapshot_id": table.metadata.current_snapshot_id,
        "current_schema_id": table.schema().schema_id,
        "files": files,
        "snapshots": [
            {
                "snapshot_id": s.snapshot_id,
                "parent_snapshot_id": s.parent_snapshot_id,
                "sequence_number": s.sequence_number,
                "operation": s.summary.operation.value,
                "schema_id": s.schema_id,
            }
            for s in table.metadata.snapshots
        ],
        "offsets": {str(k): v for k, v in committed_offsets(table.metadata).offsets.items()},
        "rows": sum(f["record_count"] for f in files),
        "arrow_rows": sorted(table.scan().to_arrow().to_pylist(), key=lambda r: (r["_kafka_partition"], r["_kafka_offset"]))[:2],
        "projection": {
            str(schema.schema_id): render_schema(schema, fork_map, schemas=table.metadata.schemas)
            for schema in table.metadata.schemas
        },
    }
    (target / "expected.json").write_text(json.dumps(expected, indent=1, default=str) + "\n")
    return expected


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", type=Path, default=REPO / "console" / "testdata" / "iceberg")
    expected = write_fixture(parser.parse_args().out)
    print(json.dumps({key: expected[key] for key in ("metadata_location", "current_snapshot_id", "rows", "offsets")}, indent=1, default=str))


if __name__ == "__main__":
    main()
