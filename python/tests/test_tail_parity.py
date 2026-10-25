"""R3: a row the tail serves is the row the seal writes, `_materialized_at` alone apart.

Each record goes two ways. The runtime's: a Faust event as the writer gets it (the value decoded by
Kaspr's `json` serializer, the timestamp in float seconds), staged by the writer's own helpers, sealed
by `run_seal`. The tail's: the raw aiokafka record, decoded, staged and built by `tail_app/ingest.py`,
held by the buffer and served by `serve.py`. The two Parquet files must hold the same rows - every
column, forks, collapses and `_unmapped` included - under the same schema, before and after the
schema evolves, and a schema change in flight is the one window where the tail's row is not yet
the seal's: it is counted as settling, and is the seal's once the change is in.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
import io
import json
from pathlib import Path
import random
import sys
import tempfile
from types import SimpleNamespace
import unittest

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent))

from tail_support import BASE_MS, Record  # noqa: E402

from materializer_control_plane.lakehouse.lifecycle import schema_to_json  # noqa: E402
from materializer_control_plane.lakehouse.schema.forkmap import ForkMap  # noqa: E402
from materializer_control_plane.lakehouse.schema.infer import infer  # noqa: E402
from materializer_control_plane.lakehouse.schema.policy import SchemaLimits, evolve  # noqa: E402
from materializer_control_plane.lakehouse.tables import build_schema  # noqa: E402
from materializer_control_plane.runtime_app.writer.batches import staged_line  # noqa: E402
from materializer_control_plane.runtime_app.writer.context import extract_source_record_metadata, format_timestamp  # noqa: E402
from materializer_control_plane.runtime_app.writer.seal import SealJob, run_seal  # noqa: E402
from materializer_control_plane.tail_app.buffer import TableBuffer  # noqa: E402
from materializer_control_plane.tail_app.ingest import UNDECODABLE, TailSchema, build, decode_value, epoch_ms, stage  # noqa: E402
from materializer_control_plane.tail_app.serve import build_answer, rows_batch  # noqa: E402

READ_AT = "2026-10-18T10:00:01.250Z"
LOCATION = "gs://lake/materializer/mat-1/orders"
KEYS = ("order_id", "amount", "status", "customer", "items", "tags", "flag", "note", "ratio", "meta")


def random_value(rng: random.Random, depth: int = 0) -> object:
    """A JSON value of the kinds topics carry, its keys from a small vocabulary so types collide."""
    choice = rng.random()
    if choice < 0.12:
        return rng.randint(-(2**40), 2**40)
    if choice < 0.2:
        return rng.choice([0.5, 1.0, 2.25, 1e16, -3.75, 1e-7])
    if choice < 0.3:
        return rng.choice(["a", "é漢字", "", "x" * rng.randint(1, 30), "10"])
    if choice < 0.36:
        return rng.random() < 0.5
    if choice < 0.42:
        return None
    if choice < 0.52 and depth < 3:
        return [random_value(rng, depth + 1) for _ in range(rng.randint(0, 3))]
    if choice < 0.6 and depth < 3:
        return [{"sku": rng.choice(["a", "b"]), "qty": rng.randint(1, 5)} for _ in range(rng.randint(0, 3))]
    if depth < 3:
        return {key: random_value(rng, depth + 1) for key in rng.sample(KEYS, rng.randint(1, 4))}
    return rng.randint(0, 10)


def random_record(rng: random.Random, partition: int, offset: int) -> tuple[Record, object]:
    """A record and its value: an object mostly, a bare value or a tombstone now and then."""
    roll = rng.random()
    if roll < 0.04:
        value = None
    elif roll < 0.08:
        value = random_value(rng)
    else:
        value = {key: random_value(rng) for key in rng.sample(KEYS, rng.randint(1, 6))}
    timestamp = -1 if rng.random() < 0.05 else BASE_MS + rng.randint(-86_400_000 * 40, 86_400_000) + rng.randint(0, 999)
    key = None if rng.random() < 0.1 else (bytes([0xFF, 0x00, rng.randint(0, 255)]) if rng.random() < 0.1 else f"K{offset}".encode())
    headers = [("event-type", b"order.created")] if rng.random() < 0.5 else []
    if rng.random() < 0.1:
        headers.append(("bin", b"\xff\xfe"))
    if rng.random() < 0.05:
        headers.append(("none", None))
    record = Record(
        topic="orders",
        partition=partition,
        offset=offset,
        timestamp=timestamp,
        key=key,
        value=None if value is None else json.dumps(value).encode("utf-8"),
        headers=headers,
    )
    return record, value


def writer_line(record: Record) -> bytes:
    """The line the runtime's writer stages for `record`: from the Faust event Kaspr hands it."""
    message = SimpleNamespace(
        topic=record.topic,
        partition=record.partition,
        offset=record.offset,
        # Faust's `_to_message`: Kafka's milliseconds as float seconds.
        timestamp=record.timestamp / 1000.0,
        key=record.key,
        headers=record.headers,
    )
    metadata = extract_source_record_metadata("mat-1", SimpleNamespace(message=message), source_topic="orders", observed_at=READ_AT)
    # Kaspr's `json` value serializer: Faust's json codec, `json.loads` of the value as UTF-8.
    value = None if record.value is None else json.loads(record.value.decode("utf-8"))
    return staged_line(metadata, value)


def tail_schema(evolution, schema_id: int | None = None) -> TailSchema:
    return TailSchema(
        schema_id=evolution.schema.schema_id if schema_id is None else schema_id,
        iceberg_schema=schema_to_json(evolution.schema),
        fork_map=evolution.fork_map.to_dict(),
        last_column_id=evolution.last_column_id,
        rejected_paths=[list(path) for path in getattr(evolution, "rejected_paths", ()) or ()],
    )


def sealed_rows(lines: list[bytes], evolution) -> list[dict]:
    """The rows `run_seal` writes for `lines` under `evolution`'s schema, in offset order."""
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "batch.jsonl"
        path.write_bytes(b"\n".join(lines) + b"\n")
        offsets = [json.loads(line)["m"]["o"] for line in lines]
        output = run_seal(
            SealJob(
                jsonl_path=str(path),
                output_dir=str(Path(directory) / "out"),
                schema=schema_to_json(evolution.schema),
                fork_map=evolution.fork_map.to_dict(),
                granularity="none",
                location=LOCATION,
                member_id="member-0",
                partition=json.loads(lines[0])["m"]["p"],
                offset_start=min(offsets),
                offset_end=max(offsets),
                seal_id="0123456789abcdef0123",
            )
        )
        tables = [pq.read_table(item["local_path"]) for item in output.files]
    return sorted((row for table in tables for row in table.to_pylist()), key=lambda row: (row["_kafka_partition"], row["_kafka_offset"]))


class TailBuffer:
    """Records into a table's tail as the tail app ingests them, segments closed every few rows."""

    def __init__(self, schema: TailSchema, partitions: int) -> None:
        self.schema = schema
        self.buffer = TableBuffer()
        for partition in range(partitions):
            self.buffer.start(partition, committed=-1, start=0)

    def ingest(self, record: Record, *, close_every: int = 7) -> None:
        value = decode_value(record.value)
        line = stage(record, value, materialization_id="mat-1", source_topic="orders", observed_at=READ_AT)
        row = build(line, self.schema, default_partition=record.partition)
        moment = row.built.moment
        self.buffer.append(
            record.partition,
            offset=record.offset,
            line=line,
            row=row.built.row,
            moment=epoch_ms(moment),
            schema_id=self.schema.schema_id,
            settling=row.settling,
        )
        open_segment = self.buffer.partitions[record.partition].open
        if len(open_segment.rows) >= close_every:
            self.buffer.close(record.partition, batch=rows_batch(open_segment.rows, self.schema), schema_id=open_segment.schema_id, settling=tuple(open_segment.settling))

    def served(self, schema: TailSchema | None = None):
        answer = build_answer(self.buffer.select({}), schema or self.schema, max_rows=10**9, max_bytes=1 << 40)
        table = pq.read_table(io.BytesIO(answer.body))
        rows = sorted(table.to_pylist(), key=lambda row: (row["_kafka_partition"], row["_kafka_offset"]))
        return answer, table, rows


def field_ids(schema: pa.Schema) -> list[tuple[str, bytes | None]]:
    return [(item.name, (item.metadata or {}).get(b"PARQUET:field_id")) for item in schema]


def without_read_time(rows: list[dict]) -> list[dict]:
    return [{key: value for key, value in row.items() if key != "_materialized_at"} for row in rows]


class TailParityTests(unittest.TestCase):
    def evolution_from(self, values: list[object], *, limits: SchemaLimits | None = None):
        """Schema 0 as discovery infers it from a sample, then every value's shape folded in."""
        objects = [value for value in values if isinstance(value, dict)]
        return evolve(build_schema(), ForkMap(), [((), infer(value)) for value in objects[:40]], limits=limits, schema_id=0)

    def test_random_records_served_by_the_tail_are_the_rows_the_seal_writes(self) -> None:
        for seed in range(12):
            with self.subTest(seed=seed):
                rng = random.Random(seed)
                records = []
                for partition in range(3):
                    for offset in range(60):
                        records.append(random_record(rng, partition, offset))
                evolution = self.evolution_from([value for _, value in records])
                schema = tail_schema(evolution)
                tail = TailBuffer(schema, partitions=3)
                lines_by_partition: dict[int, list[bytes]] = {}
                for record, _ in records:
                    tail.ingest(record)
                    lines_by_partition.setdefault(record.partition, []).append(writer_line(record))
                answer, table, served = tail.served()
                sealed = [row for partition in sorted(lines_by_partition) for row in sealed_rows(lines_by_partition[partition], evolution)]

                self.assertEqual(without_read_time(sealed), without_read_time(served))
                self.assertEqual(len(records), answer.rows)
                # The same physical schema - names, types, field ids - so the file binds as a data file does.
                self.assertEqual(field_ids(schema.arrow_schema), field_ids(table.schema))
                self.assertEqual(schema.arrow_schema.remove_metadata(), table.schema.remove_metadata())

    def test_the_writers_and_the_tails_staged_lines_agree_on_every_kafka_field(self) -> None:
        rng = random.Random(7)
        for offset in range(500):
            record, _ = random_record(rng, 1, offset)
            mine = json.loads(stage(record, decode_value(record.value), materialization_id="mat-1", source_topic="orders", observed_at=READ_AT))
            theirs = json.loads(writer_line(record))
            self.assertEqual(theirs, mine)

    def test_every_kafka_timestamp_is_staged_as_the_writer_stages_it(self) -> None:
        # The writer gets float seconds from Faust; the tail gets aiokafka's milliseconds. Before
        # March 1973 a millisecond count is small enough to be taken for seconds, so the tail
        # converts as Faust does rather than handing `format_timestamp` the milliseconds.
        rng = random.Random(3)
        for ms in [1, 999, 67_608_161_946, 99_999_999_999, 100_000_000_000, -1, 0] + [rng.randint(1, 4_102_444_800_000) for _ in range(5_000)]:
            record = Record(topic="orders", partition=0, offset=0, timestamp=ms, key=None, value=b"{}")
            mine = json.loads(stage(record, {}, materialization_id="mat-1", source_topic="orders", observed_at=READ_AT))["m"]["t"]
            self.assertEqual(json.loads(writer_line(record))["m"]["t"], mine, ms)
        self.assertNotEqual(format_timestamp(67_608_161_946 / 1000.0), format_timestamp(67_608_161_946), "why the conversion matters")

    def test_an_answers_time_bounds_are_its_rows_timestamps_floored_to_the_millisecond_exactly(self) -> None:
        # The console prunes by these bounds, so one a millisecond low drops the newest row from a
        # range that starts at it. Through a float, 2004-05-12T03:48:10.574Z came out .573.
        rng = random.Random(5)
        cases = [1_084_333_690_574, 1_082_586_889_251, 2_158_750_182_095, BASE_MS] + [rng.randint(10, 4_102_444_800_000) for _ in range(300)]
        evolution = self.evolution_from([{"order_id": "O"}])
        for ms in cases:
            with self.subTest(ms=ms):
                tail = TailBuffer(tail_schema(evolution), partitions=1)
                for offset, stamp in enumerate((ms, ms - 7)):
                    tail.ingest(Record(topic="orders", partition=0, offset=offset, timestamp=stamp, key=None, value=b'{"order_id":"O"}'))
                answer, table, _ = tail.served()
                stamps = table.column("_kafka_timestamp").to_pylist()
                exact = sorted((stamp - datetime(1970, 1, 1, tzinfo=UTC)) // timedelta(milliseconds=1) for stamp in stamps)
                self.assertEqual((exact[0], exact[-1]), (answer.time_min, answer.time_max))
                self.assertEqual((ms - 7, ms), (answer.time_min, answer.time_max))
        self.assertIsNone(epoch_ms(None))
        # A non-positive epoch is Kafka's "no timestamp": such rows give an answer no bounds.
        tail = TailBuffer(tail_schema(evolution), partitions=1)
        for offset, stamp in enumerate((-1, 0)):
            tail.ingest(Record(topic="orders", partition=0, offset=offset, timestamp=stamp, key=None, value=b'{"order_id":"O"}'))
        answer, _, _ = tail.served()
        self.assertEqual((2, None, None), (answer.rows, answer.time_min, answer.time_max))

    def test_a_segment_built_under_an_older_schema_is_served_as_the_seal_writes_it_under_the_new(self) -> None:
        rng = random.Random(11)
        records = [random_record(rng, 0, offset) for offset in range(80)]
        first = self.evolution_from([value for _, value in records[:10]])
        tail = TailBuffer(tail_schema(first), partitions=1)
        for record, _ in records:
            tail.ingest(record)
        objects = [value for _, value in records if isinstance(value, dict)]
        second = evolve(first.schema, first.fork_map, [((), infer(value)) for value in objects], last_column_id=first.last_column_id)
        self.assertNotEqual(first.schema.schema_id, second.schema.schema_id)

        _, _, served = tail.served(tail_schema(second))

        sealed = sealed_rows([writer_line(record) for record, _ in records], second)
        self.assertEqual(without_read_time(sealed), without_read_time(served))

    def test_a_collapse_moves_rows_that_had_nothing_unmapped(self) -> None:
        one_fork = SchemaLimits(max_forks_per_field=1)
        values = [{"order_id": f"O{offset}", "v": offset} for offset in range(5)]
        first = evolve(build_schema(), ForkMap(), [((), infer(values[0]))], schema_id=0)
        tail = TailBuffer(tail_schema(first), partitions=1)
        records = []
        for offset, value in enumerate(values):
            record = Record(topic="orders", partition=0, offset=offset, timestamp=BASE_MS + offset, key=None, value=json.dumps(value).encode())
            records.append(record)
            tail.ingest(record, close_every=2)
        _, _, before = tail.served()
        self.assertTrue(all(row["_unmapped"] is None and row["v"] == index for index, row in enumerate(before)))
        forked = evolve(first.schema, first.fork_map, [((), infer({"v": "s"}))], limits=one_fork, last_column_id=first.last_column_id)
        collapsed = evolve(forked.schema, forked.fork_map, [((), infer({"v": {"a": 1}}))], limits=one_fork, last_column_id=forked.last_column_id)

        _, _, after = tail.served(tail_schema(collapsed))

        self.assertEqual([str(index) for index in range(5)], [row["v__json"] for row in after])
        self.assertEqual(without_read_time(sealed_rows([writer_line(record) for record in records], collapsed)), without_read_time(after))

    def test_a_fork_before_its_schema_change_is_settling_then_the_seals_row(self) -> None:
        first = evolve(build_schema(), ForkMap(), [((), infer({"order_id": "O", "total": 1}))], schema_id=0)
        tail = TailBuffer(tail_schema(first), partitions=1)
        records = []
        for offset, value in enumerate([{"order_id": "A", "total": 1}, {"order_id": "B", "total": "many"}, {"order_id": "C", "total": 3}]):
            record = Record(topic="orders", partition=0, offset=offset, timestamp=BASE_MS, key=None, value=json.dumps(value).encode())
            records.append(record)
            tail.ingest(record)

        answer, _, rows = tail.served()

        self.assertEqual(1, answer.settling)
        self.assertEqual({"total": "many"}, json.loads(rows[1]["_unmapped"]))
        forked = evolve(first.schema, first.fork_map, [((), infer({"total": "many"}))], last_column_id=first.last_column_id)
        answer, _, rows = tail.served(tail_schema(forked))
        self.assertEqual(0, answer.settling)
        self.assertIsNone(rows[1]["_unmapped"])
        self.assertEqual(without_read_time(sealed_rows([writer_line(record) for record in records], forked)), without_read_time(rows))

    def test_a_value_kaspr_cannot_decode_gets_no_row(self) -> None:
        for raw in (b"\xff\xfe not utf-8", b"{not json", b""):
            self.assertIs(UNDECODABLE, decode_value(raw))
        self.assertIsNone(decode_value(None))
        self.assertEqual({"a": 1}, decode_value(b'{"a":1}'))
        self.assertEqual(float("inf"), decode_value(b"Infinity"), "Python's json, as Faust's codec")


if __name__ == "__main__":
    unittest.main()
