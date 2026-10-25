"""The shared row builder: one staged line into one table row, as the seal writes it and the tail serves it."""

from __future__ import annotations

from datetime import UTC, datetime
import json
from pathlib import Path
import sys
import tempfile
import unittest

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent))

from seal_cases import NO_KEY, golden_evolution, golden_lines, line  # noqa: E402
from seal_golden import seal_job  # noqa: E402

from materializer_control_plane.lakehouse.parquet import arrow_schema  # noqa: E402
from materializer_control_plane.lakehouse.rows import BuiltRow, build_row, parse_timestamp  # noqa: E402
from materializer_control_plane.lakehouse.schema.conform import Conformer  # noqa: E402
from materializer_control_plane.runtime_app.writer.seal import run_seal  # noqa: E402

NOW = datetime(2026, 10, 23, 12, 0, 0, tzinfo=UTC)


class RowBuilderTests(unittest.TestCase):
    def setUp(self) -> None:
        evolution = golden_evolution()
        self.schema = evolution.schema
        self.conformer = Conformer(evolution.schema, evolution.fork_map)

    def build(self, staged: dict, *, partition: int = 3) -> BuiltRow:
        return build_row(staged, conformer=self.conformer, default_partition=partition, now=lambda: NOW)

    def test_metadata_columns_come_first_then_the_payload(self) -> None:
        built = self.build(line(100, {"order_id": "O1", "amount": 1}, headers=[["event-type", "order.created"]]))
        self.assertEqual(
            ["_kafka_timestamp", "_kafka_partition", "_kafka_offset", "_kafka_key", "_kafka_headers", "_materialized_at", "_unmapped", "order_id", "amount"],
            list(built.row),
        )
        self.assertEqual(datetime(2026, 10, 18, 10, 0, tzinfo=UTC), built.row["_kafka_timestamp"])
        self.assertEqual(built.row["_kafka_timestamp"], built.moment)
        self.assertEqual((3, 100, "K100"), (built.row["_kafka_partition"], built.row["_kafka_offset"], built.row["_kafka_key"]))
        self.assertEqual([("event-type", "order.created")], built.row["_kafka_headers"])
        self.assertEqual(datetime(2026, 10, 18, 10, 0, 1, 250000, tzinfo=UTC), built.row["_materialized_at"])
        self.assertIsNone(built.row["_unmapped"])
        self.assertEqual(0, built.unmapped_values)

    def test_scalars_go_to_the_member_of_their_exact_type(self) -> None:
        self.assertEqual(1, self.build(line(1, {"amount": 1})).row["amount"])
        row = self.build(line(1, {"amount": 2.5})).row
        self.assertNotIn("amount", row)
        self.assertEqual(2.5, row["amount__v2"])

    def test_a_collapsed_family_takes_every_value_as_json_text(self) -> None:
        self.assertEqual('{"z":[1,2.0,1e+16]}', self.build(line(1, {"v": {"z": [1, 2.0, 1e16]}})).row["v__json"])
        self.assertEqual("1.0", self.build(line(1, {"v": 1.0})).row["v__json"])
        self.assertEqual('"text"', self.build(line(1, {"v": "text"})).row["v__json"])

    def test_structs_and_lists_of_structs(self) -> None:
        row = self.build(line(1, {"customer": {"email": "e", "tier": 2}, "items": [{"sku": "a", "qty": 1, "tags": ["x"]}, None]})).row
        self.assertEqual({"email": "e", "tier": 2}, row["customer"])
        self.assertEqual([{"sku": "a", "qty": 1, "tags": ["x"]}, None], row["items"])

    def test_unknown_keys_and_unplaceable_values_go_to_unmapped(self) -> None:
        built = self.build(line(1, {"order_id": "O", "amount": "many", "extra": {"x": 1}, "items": [{"sku": "b", "extra": 1}], "empty": [], "gone": None}))
        self.assertEqual({"amount": "many", "extra": {"x": 1}, "items[0].extra": 1}, json.loads(built.row["_unmapped"]))
        self.assertEqual(3, built.unmapped_values)
        self.assertEqual("O", built.row["order_id"])

    def test_an_integer_beyond_64_bits_is_unmapped(self) -> None:
        built = self.build(line(1, {"big": 2**70}))
        self.assertEqual({"big": 2**70}, json.loads(built.row["_unmapped"]))

    def test_a_value_that_is_not_an_object_is_kept_under_dollar(self) -> None:
        self.assertEqual({"$": [1, 2]}, json.loads(self.build(line(1, [1, 2])).row["_unmapped"]))
        self.assertEqual({"$": "s"}, json.loads(self.build(line(1, "s")).row["_unmapped"]))
        self.assertEqual(1, self.build(line(1, "s")).unmapped_values)

    def test_a_tombstone_is_a_row_with_a_null_payload(self) -> None:
        built = self.build(line(1, None, headers=[["reason", "gdpr"]]))
        self.assertEqual(["_kafka_timestamp", "_kafka_partition", "_kafka_offset", "_kafka_key", "_kafka_headers", "_materialized_at", "_unmapped"], list(built.row))
        self.assertIsNone(built.row["_unmapped"])
        self.assertEqual(0, built.unmapped_values)

    def test_keys_and_headers_are_copied_as_staged(self) -> None:
        built = self.build(line(1, {"order_id": "O"}, key="b64:AAEC", headers=[["bin", "b64:/w=="], ["no-value", None]]))
        self.assertEqual("b64:AAEC", built.row["_kafka_key"])
        self.assertEqual([("bin", "b64:/w=="), ("no-value", None)], built.row["_kafka_headers"])
        self.assertIsNone(self.build(line(1, {}, key=NO_KEY)).row["_kafka_key"])
        self.assertIsNone(self.build(line(1, {}, headers=[])).row["_kafka_headers"])

    def test_a_record_without_a_timestamp_has_no_moment(self) -> None:
        built = self.build(line(1, {"order_id": "O"}, at=None))
        self.assertIsNone(built.row["_kafka_timestamp"])
        self.assertIsNone(built.moment)

    def test_a_line_without_its_partition_or_read_time_takes_the_defaults(self) -> None:
        built = self.build({"m": {"o": 5, "t": "2026-10-18T10:00:00.000Z"}, "v": {"order_id": "O"}}, partition=7)
        self.assertEqual(7, built.row["_kafka_partition"])
        self.assertEqual(NOW, built.row["_materialized_at"])

    def test_timestamps_parse_with_or_without_zone(self) -> None:
        self.assertEqual(datetime(2026, 1, 2, 3, 4, 5, 6000, tzinfo=UTC), parse_timestamp("2026-01-02T03:04:05.006Z"))
        self.assertEqual(datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC), parse_timestamp("2026-01-02T03:04:05"))
        for value in (None, "", "not a time", 1700000000000):
            self.assertIsNone(parse_timestamp(value))

    def test_every_row_fits_the_tables_arrow_schema(self) -> None:
        rows = [self.build(staged).row for staged in golden_lines()]
        table = pa.Table.from_pylist(rows, schema=arrow_schema(self.schema))
        self.assertEqual(len(rows), table.num_rows)

    def test_the_seal_writes_exactly_the_rows_build_row_builds(self) -> None:
        lines = golden_lines()
        with tempfile.TemporaryDirectory() as directory:
            output = run_seal(seal_job(directory, lines, granularity="none", row_group_bytes=1 << 20, after_offset=None))
            (item,) = output.files
            sealed = pq.read_table(item["local_path"])
        built = pa.Table.from_pylist([self.build(staged).row for staged in lines], schema=arrow_schema(self.schema))
        self.assertEqual(built.to_pylist(), sealed.to_pylist())
        self.assertEqual(sum(self.build(staged).unmapped_values for staged in lines), output.unmapped_values)


if __name__ == "__main__":
    unittest.main()
