"""The seal: staged JSONL conformed to the table's schema and written as Iceberg Parquet (spec §7.3)."""

from __future__ import annotations

import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock

import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent))

from lakehouse_support import LOCATION  # noqa: E402
from seal_cases import golden_jobs  # noqa: E402
from seal_golden import GOLDEN_PATH, describe_output, golden_descriptions, seal_job  # noqa: E402
from seal_reference import reference_run_seal  # noqa: E402

from materializer_control_plane.lakehouse.lifecycle import schema_to_json  # noqa: E402
from materializer_control_plane.lakehouse.schema.forkmap import ForkMap  # noqa: E402
from materializer_control_plane.lakehouse.schema.infer import infer  # noqa: E402
from materializer_control_plane.lakehouse.schema.policy import evolve  # noqa: E402
from materializer_control_plane.lakehouse.tables import build_schema  # noqa: E402
from materializer_control_plane.runtime_app.writer import seal as seal_module  # noqa: E402
from materializer_control_plane.runtime_app.writer.seal import SealJob, run_seal  # noqa: E402

FIELD_ID = b"PARQUET:field_id"


class SealTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        evolution = evolve(build_schema(), ForkMap(), [((), infer({"order_id": "O", "amount": 1}))], schema_id=0)
        self.schema = evolution.schema
        self.fork_map = evolution.fork_map

    def job(self, lines: list[dict], *, after_offset: int | None = None, granularity: str = "day") -> SealJob:
        path = Path(self.tmp.name) / "batch.jsonl"
        path.write_text("\n".join(json.dumps(line) for line in lines) + "\n", encoding="utf-8")
        offsets = [line["m"]["o"] for line in lines]
        return SealJob(
            jsonl_path=str(path),
            output_dir=str(Path(self.tmp.name) / "out"),
            schema=schema_to_json(self.schema),
            fork_map=self.fork_map.to_dict(),
            granularity=granularity,
            location=LOCATION,
            member_id="member-0",
            partition=3,
            offset_start=min(offsets),
            offset_end=max(offsets),
            seal_id="0123456789abcdef0123",
            after_offset=after_offset,
        )

    @staticmethod
    def line(offset: int, value, *, at: str | None = "2026-10-18T10:00:00.000Z") -> dict:
        return {"m": {"p": 3, "o": offset, "t": at, "k": f"K{offset}", "h": [["a", "b"]], "a": "2026-10-18T10:00:01.000Z"}, "v": value}

    def test_rows_split_by_day_into_files_named_for_their_offsets(self) -> None:
        output = run_seal(
            self.job(
                [
                    self.line(10, {"order_id": "A", "amount": 1}),
                    self.line(11, {"order_id": "B", "amount": 2}, at="2026-10-19T00:00:00.000Z"),
                    self.line(12, {"order_id": "C", "amount": 3}, at=None),
                ]
            )
        )
        self.assertEqual(3, output.rows)
        by_partition = {item["partition_text"]: item for item in output.files}
        self.assertEqual({"2026-10-18", "2026-10-19", "__null__"}, set(by_partition))
        self.assertIn("/data/ts_day=2026-10-18/p03-o0000000010-0000000010-member-0-", by_partition["2026-10-18"]["uri"])
        self.assertEqual({"ts_day": "2026-10-18"}, by_partition["2026-10-18"]["partition_value"])
        self.assertEqual({"ts_day": None}, by_partition["__null__"]["partition_value"])
        self.assertEqual(1, by_partition["2026-10-19"]["record_count"])

    def test_files_carry_field_ids_and_the_kafka_metadata(self) -> None:
        output = run_seal(self.job([self.line(10, {"order_id": "A", "amount": 1})]))
        (item,) = output.files
        table = pq.read_table(item["local_path"])
        for column in table.schema:
            self.assertIn(FIELD_ID, column.metadata or {}, column.name)
        (row,) = table.to_pylist()
        self.assertEqual("K10", row["_kafka_key"])
        self.assertEqual(3, row["_kafka_partition"])
        self.assertEqual([("a", "b")], [tuple(pair) for pair in row["_kafka_headers"]])
        self.assertEqual("A", row["order_id"])

    def test_values_no_field_takes_go_to_unmapped(self) -> None:
        output = run_seal(self.job([self.line(10, {"order_id": "A", "amount": "not a number", "extra": {"x": 1}})]))
        self.assertEqual(2, output.unmapped_values)
        (item,) = output.files
        (row,) = pq.read_table(item["local_path"]).to_pylist()
        self.assertEqual({"amount": "not a number", "extra": {"x": 1}}, json.loads(row["_unmapped"]))
        self.assertIsNone(row["amount"])

    def test_a_reseal_leaves_out_what_the_table_holds(self) -> None:
        output = run_seal(
            self.job(
                [self.line(offset, {"order_id": str(offset), "amount": offset}) for offset in range(10, 15)],
                after_offset=12,
            )
        )
        self.assertEqual(2, output.rows)
        (item,) = output.files
        self.assertEqual([13, 14], [row["_kafka_offset"] for row in pq.read_table(item["local_path"]).to_pylist()])

    def test_a_month_table_splits_rows_by_month(self) -> None:
        output = run_seal(
            self.job(
                [
                    self.line(10, {"order_id": "A"}),
                    self.line(11, {"order_id": "B"}, at="2026-10-31T23:59:59.999Z"),
                    self.line(12, {"order_id": "C"}, at="2026-11-01T00:00:00.000Z"),
                ],
                granularity="month",
            )
        )
        by_partition = {item["partition_text"]: item for item in output.files}
        self.assertEqual({"2026-10", "2026-11"}, set(by_partition))
        self.assertIn("/data/ts_month=2026-10/p03-o0000000010-0000000011-", by_partition["2026-10"]["uri"])
        self.assertEqual({"ts_month": "2026-11"}, by_partition["2026-11"]["partition_value"])
        self.assertEqual(2, by_partition["2026-10"]["record_count"])

    def test_an_unpartitioned_table_gets_one_file(self) -> None:
        output = run_seal(
            self.job(
                [self.line(10, {"order_id": "A"}), self.line(11, {"order_id": "B"}, at="2026-10-19T00:00:00.000Z")],
                granularity="none",
            )
        )
        (item,) = output.files
        self.assertIn("/data/p03-", item["uri"])
        self.assertEqual({}, item["partition_value"])


    def test_a_batch_spread_over_many_months_leaves_only_its_files_with_rows_in_order(self) -> None:
        # A backfill's batch: 240 rows over two years of months, every month's file written in turn.
        lines = [
            self.line(100 + i, {"order_id": f"O{i}", "amount": i}, at=f"{2024 + (i % 24) // 12}-{(i % 24) % 12 + 1:02d}-15T10:00:00.000Z")
            for i in range(240)
        ]
        job = self.job(lines, granularity="month")
        output = run_seal(job)
        self.assertEqual(240, output.rows)
        self.assertEqual(24, len(output.files))
        self.assertEqual({".parquet"}, {path.suffix for path in Path(job.output_dir).iterdir()}, "no spool is left behind")
        january = next(item for item in output.files if item["partition_text"] == "2024-01")
        self.assertEqual([100 + i for i in range(0, 240, 24)], pq.read_table(january["local_path"]).column("_kafka_offset").to_pylist())
        self.assertEqual(10, january["record_count"])
        # Files are numbered in the order their months were first seen, as before.
        self.assertTrue(january["local_path"].endswith("-0.parquet"))
        self.assertTrue(next(item for item in output.files if item["partition_text"] == "2024-02")["local_path"].endswith("-1.parquet"))

    def test_spools_written_out_in_pieces_keep_every_row_in_order(self) -> None:
        lines = [self.line(i, {"order_id": f"O{i}", "amount": i}, at=f"2026-10-{18 + i % 3:02d}T10:00:00.000Z") for i in range(90)]
        # A buffer far smaller than one line: every line is appended to its spool on its own.
        with mock.patch.object(seal_module, "SPOOL_BUFFER_BYTES", 1):
            output = run_seal(self.job(lines))
        self.assertEqual(90, output.rows)
        for item in output.files:
            day = int(item["partition_text"][-2:]) - 18
            self.assertEqual(list(range(day, 90, 3)), pq.read_table(item["local_path"]).column("_kafka_offset").to_pylist(), item["partition_text"])

    def test_a_seal_that_fails_part_way_leaves_no_spool_behind(self) -> None:
        lines = [self.line(10, {"order_id": "A", "amount": 1}), self.line(11, {"order_id": "B", "amount": 2}, at="2026-10-19T00:00:00.000Z")]
        job = self.job(lines, granularity="day")
        # A line without an offset spools (only its timestamp is read) and fails when its row is built.
        del lines[1]["m"]["o"]
        Path(job.jsonl_path).write_text("\n".join(json.dumps(line) for line in lines) + "\n", encoding="utf-8")
        with self.assertRaises(KeyError):
            run_seal(job)
        self.assertEqual([], [path.name for path in Path(job.output_dir).iterdir() if path.suffix == ".jsonl"])

class SealGoldenTests(unittest.TestCase):
    """The shared row builder (`lakehouse/rows.py`) moved code, not behaviour: the seal writes what it did."""

    def test_the_seal_writes_the_same_bytes_as_before_the_row_builder(self) -> None:
        for name, lines, granularity, row_group_bytes, after_offset in golden_jobs():
            with self.subTest(case=name), tempfile.TemporaryDirectory() as directory:
                now_dir = Path(directory) / "now"
                then_dir = Path(directory) / "then"
                now_dir.mkdir()
                then_dir.mkdir()
                now = run_seal(seal_job(str(now_dir), lines, granularity=granularity, row_group_bytes=row_group_bytes, after_offset=after_offset))
                then = reference_run_seal(seal_job(str(then_dir), lines, granularity=granularity, row_group_bytes=row_group_bytes, after_offset=after_offset))
                self.assertEqual((then.rows, then.unmapped_values, then.schema_id), (now.rows, now.unmapped_values, now.schema_id))
                self.assertEqual(len(then.files), len(now.files))
                for before, after in zip(then.files, now.files, strict=True):
                    self.assertEqual(
                        {key: value for key, value in before.items() if key != "local_path"},
                        {key: value for key, value in after.items() if key != "local_path"},
                    )
                    self.assertEqual(Path(before["local_path"]).read_bytes(), Path(after["local_path"]).read_bytes(), after["partition_text"])

    def test_the_seal_writes_the_golden_rows(self) -> None:
        golden = json.loads(GOLDEN_PATH.read_text(encoding="utf-8"))
        described = golden_descriptions()
        self.assertEqual(sorted(golden), sorted(described))
        for name in golden:
            with self.subTest(case=name):
                self.assertEqual(golden[name], json.loads(json.dumps(described[name])))

    def test_a_row_without_a_read_time_takes_the_time_it_is_sealed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            line = {"m": {"p": 3, "o": 7, "t": None, "k": None, "h": None}, "v": {"order_id": "A"}}
            output = run_seal(seal_job(directory, [line], granularity="day", row_group_bytes=1024, after_offset=None))
            (row,) = describe_output(output)["files"][0]["rows"]
            self.assertIsNotNone(row["_materialized_at"])
            self.assertIsNone(row["_kafka_timestamp"])


if __name__ == "__main__":
    unittest.main()
