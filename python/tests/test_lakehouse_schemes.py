"""A table's whole life - created, committed to, compacted, expired, cleaned of orphans, dropped - on
every scheme a location may have.

Nothing above the object store knows which backend a table lives on: each call finds its store by
the scheme of the table's URI (`store_for`), so a `MemoryStore` installed for a scheme (`set_store`)
stands in for that backend and the production code runs unchanged over `gs://`, `s3://`, `abfss://`
and `file://` locations. The local file store runs the same life on a temporary directory.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from lakehouse_support import BASE_TIME, LakeFixture  # noqa: E402
from pyiceberg.typedef import Record  # noqa: E402

from midstream.lakehouse.commits import PendingFiles, commit_append, plan_append  # noqa: E402
from midstream.lakehouse.files import build_data_file, read_parquet_footer  # noqa: E402
from midstream.lakehouse.lifecycle import create_materializer_table, schema_to_json, table_catalog  # noqa: E402
from midstream.lakehouse.maintenance import (  # noqa: E402
    commit_compaction,
    expire_snapshots,
    find_orphans,
    plan_compaction,
    plan_expiry,
    referenced_files,
)
from midstream.lakehouse.offsets import committed_offsets  # noqa: E402
from midstream.lakehouse.pointer import pointer_uri, read_pointer, version_hint_uri  # noqa: E402
from midstream.lakehouse.schema.forkmap import ForkMap  # noqa: E402
from midstream.lakehouse.schema.infer import infer  # noqa: E402
from midstream.lakehouse.schema.policy import evolve  # noqa: E402
from midstream.lakehouse.store import FileStore, MemoryStore, ObjectStore, scheme_of, set_store, store_for  # noqa: E402
from midstream.lakehouse.tables import build_schema, table_identity, table_properties  # noqa: E402
from midstream.maintenance_app.compact_worker import run as run_compaction  # noqa: E402
from midstream.storage.objects import drop_table_objects  # noqa: E402

MATERIALIZATION_ID = "mat-5c8ed527453d"
STORAGE_PREFIX = "midstream"
#: A destination per scheme, and the container the drop reports for it.
DESTINATIONS = (
    ("gs://my-lake", "my-lake"),
    ("s3://my-lake/prod", "my-lake"),
    ("abfss://lake@acme.dfs.core.windows.net", "lake@acme"),
    ("file:///srv/lake", ""),
)
#: Seals of two rows each, all on one day a month back: small files of a settled partition.
SEALS = 3
DAY = BASE_TIME - timedelta(days=30)


class TableLifeOnEverySchemeTests(unittest.TestCase):
    def test_a_table_lives_and_is_dropped_the_same_on_every_scheme(self) -> None:
        for destination, container in DESTINATIONS:
            with self.subTest(destination=destination):
                self.live(destination, MemoryStore(), container=container)

    def test_a_local_table_lives_and_is_dropped_the_same_in_the_file_store(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            self.live(Path(root).resolve().as_uri(), FileStore(), container="")

    def live(self, destination: str, store: ObjectStore, *, container: str) -> None:
        scheme = scheme_of(destination)
        set_store(scheme, store)
        try:
            self._live(destination, store, container=container)
        finally:
            set_store(scheme, None)

    def _live(self, destination: str, store: ObjectStore, *, container: str) -> None:
        identity = table_identity(
            destination_uri=destination,
            storage_prefix=STORAGE_PREFIX,
            dataset="shop",
            materialization_id=MATERIALIZATION_ID,
            table="orders",
        )
        location = identity.location
        self.assertEqual(f"{destination}/{STORAGE_PREFIX}/{MATERIALIZATION_ID}/orders", location)
        materialization = location.rsplit("/", 1)[0]
        # What the drop must leave alone: the materialization's other tables - one whose name starts
        # with this one's - and the destination's other objects.
        neighbours = (
            f"{materialization}/payments/metadata/catalog-pointer.json",
            f"{materialization}/orders_archive/data/kept.parquet",
            f"{destination}/elsewhere/kept.json",
        )
        for uri in neighbours:
            store.put(uri, b"{}")

        # Created through the location's own store: no store is handed to anything below.
        record = {"order_id": "O-0", "amount": 1}
        evolution = evolve(build_schema(), ForkMap(), [((), infer(record))], schema_id=0)
        created = create_materializer_table(
            identity,
            schema=evolution.schema,
            fork_map=evolution.fork_map,
            granularity="day",
            properties=table_properties(
                materialization_id=MATERIALIZATION_ID,
                source_topic="orders",
                partition_granularity="day",
                created_by="tester@example.com",
                created_at="2026-10-30T00:00:00Z",
                package_version="1.0.0",
                snapshot_retention_hours=1,
                min_snapshots_to_keep=1,
                fork_map_json=evolution.fork_map.to_json(),
            ),
        )
        self.assertEqual(location, created.location)
        self.assertTrue(created.metadata_location.startswith(f"{location}/metadata/00000-"), created.metadata_location)
        pointer, version = read_pointer(store, location)
        self.assertEqual((created.metadata_location, version), (pointer.metadata_location, store.head(pointer_uri(location)).version))
        self.assertEqual(created.metadata_location.rsplit("/", 1)[1].removesuffix(".metadata.json"), store.get(version_hint_uri(location))[0].decode())

        # The fixture writes the seals' files as a runtime member would, into the table just created.
        lake = LakeFixture(record, location=location, identifier=identity.identifier, store=store, materialization_id=MATERIALIZATION_ID)
        try:
            self.assertEqual(created.metadata_location, lake.load().metadata_location)
            catalog = table_catalog(identity, writer="committer")
            self._commit_seals(lake, catalog, identity.identifier)
            expected = [(0, offset, f"O-{offset}") for offset in range(2 * SEALS)]
            self.assertEqual(expected, self._rows(catalog, identity.identifier))
            self.assertEqual(SEALS, int(catalog.load_pointer(identity.identifier)[0].sequence))

            inputs = self._compact(catalog, identity.identifier, location)
            self.assertEqual(expected, self._rows(catalog, identity.identifier))

            table = catalog.load_table(identity.identifier)
            doomed = plan_expiry(table.metadata, now=datetime.now(tz=UTC) + timedelta(days=30), retention_hours=1, min_snapshots_to_keep=1, keep_all_minutes=0)
            self.assertEqual(SEALS, len(doomed))
            self.assertEqual(SEALS, expire_snapshots(table, doomed))

            # Orphans - the compacted inputs, the expired snapshots' manifests - go as the maintenance
            # app deletes them, one by one through the location's store; the table reads the same.
            table = catalog.load_table(identity.identifier)
            scan = find_orphans(store_for(location), location, referenced_files(table), now=datetime.now(tz=UTC) + timedelta(days=3), min_age_hours=24)
            self.assertLessEqual(inputs, set(scan.candidates))
            self.assertTrue(all(uri.startswith(f"{location}/") for uri in scan.candidates), scan.candidates)
            for uri in scan.candidates:
                self.assertTrue(store_for(uri).delete(uri), uri)
            self.assertEqual(expected, self._rows(catalog, identity.identifier))
            self.assertEqual([], [uri for uri in inputs if store.head(uri) is not None])
        finally:
            lake.close()

        # The drop: never outside a table of the materialization, and then the whole table, alone.
        with self.assertRaisesRegex(ValueError, "refusing to delete outside the table"):
            drop_table_objects(location=materialization, materialization_id=MATERIALIZATION_ID)
        result = drop_table_objects(location=location, materialization_id=MATERIALIZATION_ID)
        self.assertTrue(result.complete)
        self.assertEqual(container, result.bucket)
        self.assertGreater(result.deleted_count, 0)
        self.assertEqual([], [item.uri for item in store.list(f"{location}/")])
        self.assertEqual([], [uri for uri in neighbours if store.head(uri) is None])
        self.assertFalse(catalog.table_exists(identity.identifier))

    def _commit_seals(self, lake: LakeFixture, catalog, identifier: tuple[str, str]) -> None:
        """Commit `SEALS` seals as the committer does, each one append through the catalog."""
        after = -1
        for index in range(SEALS):
            offsets = [2 * index, 2 * index + 1]
            announced = lake.write_seal(partition=0, after_offset=after, offsets=offsets, times=[DAY, DAY])
            pending = PendingFiles()
            for item in announced:
                pending.add(item)
            table = catalog.load_table(identifier)
            plan = plan_append(pending.seals(), committed_offsets(table.metadata), max_files=10_000, max_bytes=1 << 50)
            commit_append(table, plan, runtime="test-runtime")
            after = offsets[-1]

    def _compact(self, catalog, identifier: tuple[str, str], location: str) -> set[str]:
        """Compact the seals' day as the maintenance app does; return the files it replaced."""
        table = catalog.load_table(identifier)
        compaction = plan_compaction(
            table,
            now=datetime.now(tz=UTC) + timedelta(days=1),
            min_input_files=2,
            small_file_bytes=64 << 20,
            max_input_bytes=1 << 30,
            hot_partition_delay_minutes=0,
        )
        self.assertIsNotNone(compaction)
        self.assertEqual(SEALS, len(compaction.inputs))
        with tempfile.TemporaryDirectory() as work_root:
            result = run_compaction(
                {"plan": compaction.to_dict(), "schema": schema_to_json(table.schema()), "work_dir": "compact-job", "location": location, "granularity": "day"},
                work_root=work_root,
            )
        outputs = []
        for output in result["outputs"]:
            self.assertTrue(output["uri"].startswith(f"{location}/data/"), output["uri"])
            footer, size = read_parquet_footer(store_for(output["uri"]), output["uri"])
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
        commit_compaction(table, compaction, outputs, runtime="test", output_bounds=[(o["timestamp_min"], o["timestamp_max"]) for o in result["outputs"]])
        return {item.path for item in compaction.inputs}

    def _rows(self, catalog, identifier: tuple[str, str]) -> list[tuple[int, int, str]]:
        rows = catalog.load_table(identifier).scan().to_arrow().to_pylist()
        return sorted((row["_kafka_partition"], row["_kafka_offset"], row["order_id"]) for row in rows)


if __name__ == "__main__":
    unittest.main()
