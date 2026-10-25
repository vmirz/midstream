"""The GCS-pointer catalog: pointer CAS, table creation with fixed ids, commits, drops (spec §5)."""

from __future__ import annotations

import json
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from lakehouse_support import IDENTIFIER, LOCATION, MATERIALIZATION_ID, LakeFixture  # noqa: E402

from pyiceberg.exceptions import CommitFailedException, NoSuchTableError, TableAlreadyExistsError  # noqa: E402

from materializer_control_plane.lakehouse.catalog import (  # noqa: E402
    ForeignTableError,
    GcsPointerCatalog,
    StaticTableDirectory,
    TableEntry,
    commit_once,
)
from materializer_control_plane.lakehouse.commits import PendingFiles, commit_append, commit_schema, plan_append  # noqa: E402
from materializer_control_plane.lakehouse.offsets import committed_offsets  # noqa: E402
from materializer_control_plane.lakehouse.pointer import (  # noqa: E402
    PointerConflictError,
    TablePointer,
    find_latest_metadata,
    pointer_uri,
    read_pointer,
    version_hint_uri,
    write_pointer,
)
from materializer_control_plane.lakehouse.schema.forkmap import ForkMap  # noqa: E402
from materializer_control_plane.lakehouse.schema.infer import infer  # noqa: E402
from materializer_control_plane.lakehouse.schema.policy import evolve  # noqa: E402
from materializer_control_plane.lakehouse.tables import new_table_metadata  # noqa: E402


class PointerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.lake = LakeFixture({"order_id": "A"})

    def tearDown(self) -> None:
        self.lake.close()

    def test_created_table_has_a_pointer_and_a_version_hint(self) -> None:
        pointer, generation = read_pointer(self.lake.client, LOCATION)
        self.assertEqual(pointer.materialization_id, MATERIALIZATION_ID)
        self.assertEqual(pointer.sequence, 0)
        self.assertEqual(pointer.identifier, IDENTIFIER)
        self.assertGreater(generation, 0)
        hint = self.lake.store.get(version_hint_uri(LOCATION)).decode()
        self.assertTrue(pointer.metadata_location.endswith(f"{hint}.metadata.json"))

    def test_a_stale_generation_cannot_move_the_pointer(self) -> None:
        pointer, generation = read_pointer(self.lake.client, LOCATION)
        write_pointer(self.lake.client, LOCATION, pointer.advanced("gs://lake/x.metadata.json", updated_by="a"), generation=generation)
        with self.assertRaises(PointerConflictError):
            write_pointer(self.lake.client, LOCATION, pointer.advanced("gs://lake/y.metadata.json", updated_by="b"), generation=generation)

    def test_missing_pointer_reads_as_none(self) -> None:
        self.assertEqual(read_pointer(self.lake.client, "gs://lake/nowhere"), (None, 0))

    def test_pointer_round_trips(self) -> None:
        pointer, _ = read_pointer(self.lake.client, LOCATION)
        self.assertEqual(TablePointer.from_dict(pointer.to_dict()), pointer)

    def test_repair_finds_the_newest_metadata_of_the_table(self) -> None:
        table = self.lake.load()
        evolution = evolve(table.schema(), ForkMap(), [((), infer({"extra": 1}))], last_column_id=table.metadata.last_column_id)
        commit_schema(table, evolution)
        newest = self.lake.load().metadata_location
        self.lake.store.objects.pop(tuple(pointer_uri(LOCATION).removeprefix("gs://").split("/", 1)))
        self.assertEqual(find_latest_metadata(self.lake.client, LOCATION, table_uuid=str(table.metadata.table_uuid)), newest)
        self.assertIsNone(find_latest_metadata(self.lake.client, LOCATION, table_uuid="00000000-0000-0000-0000-000000000000"))


class CatalogTests(unittest.TestCase):
    def setUp(self) -> None:
        self.lake = LakeFixture({"order_id": "A", "customer": {"email": "e"}})

    def tearDown(self) -> None:
        self.lake.close()

    def test_creation_keeps_field_ids_and_table_properties(self) -> None:
        table = self.lake.load()
        self.assertEqual(table.metadata.format_version, 2)
        self.assertEqual([field.field_id for field in table.schema().fields[:7]], [1, 2, 3, 4, 5, 6, 7])
        headers = table.schema().find_field("_kafka_headers").field_type
        self.assertEqual((headers.key_id, headers.value_id), (8, 9))
        self.assertEqual(table.schema().find_field("order_id").field_id, 10)
        self.assertEqual(table.spec().fields[0].name, "ts_day")
        self.assertEqual(table.spec().fields[0].field_id, 1000)
        self.assertEqual(table.sort_order().fields[0].source_id, 1)
        properties = table.metadata.properties
        self.assertEqual(properties["write.parquet.compression-codec"], "zstd")
        self.assertEqual(properties["commit.manifest-merge.enabled"], "true")
        self.assertEqual(properties["materializer.materialization-id"], MATERIALIZATION_ID)
        self.assertNotIn("format-version", properties)

    def test_creation_is_idempotent_for_one_materialization(self) -> None:
        table = self.lake.load()
        again = self.lake.catalog.create_table_from_metadata(
            IDENTIFIER, location=LOCATION, materialization_id=MATERIALIZATION_ID, metadata=table.metadata
        )
        self.assertEqual(again.metadata_location, table.metadata_location)

    def test_a_location_held_by_another_materialization_is_refused(self) -> None:
        table = self.lake.load()
        with self.assertRaises(TableAlreadyExistsError):
            self.lake.catalog.create_table_from_metadata(
                IDENTIFIER, location=LOCATION, materialization_id="mat-ffffffffffff", metadata=table.metadata
            )
        foreign = GcsPointerCatalog(
            directory=StaticTableDirectory([TableEntry(IDENTIFIER, LOCATION, "mat-ffffffffffff")]),
            client=self.lake.client,
        )
        with self.assertRaises(ForeignTableError):
            foreign.load_table(IDENTIFIER)

    def test_unknown_identifiers_and_listing(self) -> None:
        with self.assertRaises(NoSuchTableError):
            self.lake.catalog.load_table(("shop", "nope"))
        self.assertEqual(self.lake.catalog.list_tables("shop"), [IDENTIFIER])
        self.assertEqual(self.lake.catalog.list_namespaces(), [("shop",)])
        self.assertTrue(self.lake.catalog.table_exists(("SHOP", "Orders")))

    def test_schema_commit_moves_the_pointer_by_one(self) -> None:
        table = self.lake.load()
        before, _ = read_pointer(self.lake.client, LOCATION)
        evolution = evolve(table.schema(), ForkMap(), [((), infer({"status": "x"}))], last_column_id=table.metadata.last_column_id)
        commit_schema(table, evolution)
        after, _ = read_pointer(self.lake.client, LOCATION)
        self.assertEqual(after.sequence, before.sequence + 1)
        self.assertEqual(after.previous_metadata_location, before.metadata_location)
        reloaded = self.lake.load()
        self.assertEqual(reloaded.schema().schema_id, 1)
        self.assertIsNotNone(reloaded.schema().find_field("status"))
        self.assertEqual(reloaded.metadata.properties["materializer.fork-map"], evolution.fork_map.to_json())

    def test_a_lost_race_raises_and_removes_its_metadata_file(self) -> None:
        first = self.lake.load()
        second = self.lake.load()
        commit_schema(first, evolve(first.schema(), ForkMap(), [((), infer({"a": 1}))], last_column_id=first.metadata.last_column_id))
        metadata_files = [name for name in self.lake.store.names(LOCATION) if name.endswith(".metadata.json")]
        with self.assertRaises(CommitFailedException):
            commit_schema(second, evolve(second.schema(), ForkMap(), [((), infer({"b": 1}))], last_column_id=second.metadata.last_column_id))
        self.assertEqual([name for name in self.lake.store.names(LOCATION) if name.endswith(".metadata.json")], metadata_files)

    def test_commit_once_never_retries_an_append(self) -> None:
        """Two committers with the same files: the loser fails instead of adding them again."""
        stale = self.lake.load()
        announced = self.lake.write_seal(partition=0, after_offset=-1, offsets=[0, 1, 2])
        pending = PendingFiles()
        for item in announced:
            pending.add(item)
        plan = plan_append(pending.seals(), committed_offsets(stale.metadata), max_files=100, max_bytes=1 << 40)
        commit_append(self.lake.load(), plan, runtime="rt", client=self.lake.client)
        with self.assertRaises(CommitFailedException):
            commit_append(stale, plan, runtime="rt", client=self.lake.client)
        table = self.lake.load()
        self.assertEqual(len(table.metadata.snapshots), 1)
        self.assertEqual(table.current_snapshot().summary.additional_properties["total-records"], "3")

    def test_commit_once_requires_a_fresh_transaction(self) -> None:
        table = self.lake.load()
        transaction = table.transaction()
        self.assertIs(commit_once(transaction), table)

    def test_drop_removes_only_the_pointer(self) -> None:
        self.lake.catalog.drop_table(IDENTIFIER)
        self.assertFalse(self.lake.catalog.table_exists(IDENTIFIER))
        self.assertTrue(any(name.endswith(".metadata.json") for name in self.lake.store.names(LOCATION)))
        with self.assertRaises(NoSuchTableError):
            self.lake.catalog.drop_table(IDENTIFIER)

    def test_metadata_file_is_plain_iceberg_json(self) -> None:
        table = self.lake.load()
        raw = json.loads(self.lake.store.get(table.metadata_location))
        self.assertEqual(raw["format-version"], 2)
        self.assertEqual(raw["current-schema-id"], 0)
        self.assertEqual(raw["location"], LOCATION)

    def test_create_table_through_pyiceberg_is_refused(self) -> None:
        with self.assertRaises(NotImplementedError):
            self.lake.catalog.create_table(("materializer", "x"), self.lake.load().schema())

    def test_new_metadata_without_payload(self) -> None:
        table = self.lake.load()
        metadata = new_table_metadata(location=LOCATION, schema=table.schema(), granularity="none", properties={})
        self.assertEqual(metadata.partition_specs[0].fields, ())
        self.assertEqual(metadata.last_partition_id, 999)


if __name__ == "__main__":
    unittest.main()
