from __future__ import annotations

import os
from collections.abc import Iterable
import unittest

from midstream.config import ControlPlaneConfig
from midstream.models import (
    Materialization,
    MaterializationSchema,
    MaterializationStatus,
    MaterializationTable,
    ResourceObservation,
    RuntimeBinding,
    RuntimeHealth,
    TableSpec,
)
from midstream.models.table import MaintenanceRun, TableError
from midstream.storage import (
    MATERIALIZATION_KEY_PREFIX,
    MATERIALIZATION_OP_KEY_PREFIX,
    MATERIALIZATION_SCHEMA_KEY_PREFIX,
    MATERIALIZATION_SCHEMA_VERSION_KEY_PREFIX,
    MATERIALIZATION_STATUS_KEY_PREFIX,
    RUNTIME_BINDING_KEY_PREFIX,
    decode_envelope,
    delete_materialization,
    delete_materialization_schemas,
    delete_materialization_table,
    delete_materialization_status,
    delete_operation_record,
    delete_runtime_binding,
    delete_schema_versions,
    get_materialization,
    get_materialization_schema,
    get_materialization_table,
    get_materialization_status,
    get_operation_record,
    get_runtime_binding,
    get_schema_version,
    list_materialization_tables,
    list_materializations,
    list_schema_versions,
    materialization_key,
    materialization_operation_key,
    materialization_prefix,
    materialization_schema_key,
    materialization_schema_version_key,
    materialization_status_key,
    materialization_table_key,
    resolve_materialization_schema,
    runtime_binding_key,
    save_materialization,
    save_materialization_schema,
    save_materialization_status,
    save_materialization_table,
    save_operation_record,
    save_runtime_binding,
)
from midstream import config as control_plane_config


class FakeTable:
    def __init__(self) -> None:
        self.values: dict[str, dict] = {}
        self.prefix_scan_calls: list[str] = []

    def get(self, key: str) -> dict | None:
        return self.values.get(key)

    def __setitem__(self, key: str, value: dict) -> None:
        self.values[key] = value

    def __delitem__(self, key: str) -> None:
        self.values.pop(key, None)

    def prefix_scan(self, prefix: str) -> Iterable[tuple[str, dict]]:
        self.prefix_scan_calls.append(prefix)
        for key in sorted(self.values):
            if key.startswith(prefix):
                yield key, self.values[key]


def build_materialization(materialization_id: str, updated_at: str) -> Materialization:
    return Materialization(
        materialization_id=materialization_id,
        name=f"name-{materialization_id}",
        tables=(TableSpec(name="orders", source_topic=f"topic-{materialization_id}", dataset="shop", dataset_id="ds-000000000001"),),
        destination_uri=f"abfs://lakehouse/{materialization_id}",
        desired_phase="running",
        created_at="2026-07-12T00:00:00Z",
        updated_at=updated_at,
        runtime_mode="dedicated",
        namespace="midstream",
    )


def build_status(materialization_id: str, observed_phase: str, phase_reason: str) -> MaterializationStatus:
    return MaterializationStatus(
        materialization_id=materialization_id,
        desired_phase="running",
        observed_phase=observed_phase,
        phase_reason=phase_reason,
        last_transition_time="2026-07-12T00:05:00Z",
        observation_source="hybrid",
        runtime_health=RuntimeHealth(level="healthy"),
        resource_observation=ResourceObservation(
            namespace="midstream",
            kaspr_app_name=f"kaspr-materializer-{materialization_id}",
            resource_exists=True,
            managed_agent_states={"writer": "running"},
        ),
    )


def build_schema(materialization_id: str, schema_id: int, *, table: str = "orders") -> MaterializationSchema:
    return MaterializationSchema(
        materialization_id=materialization_id,
        table=table,
        schema_id=schema_id,
        since="2026-07-12T00:02:00Z",
        field_count=1,
        fields=[{"field_id": 10 + schema_id, "path": [f"f{schema_id}"], "name": f"f{schema_id}", "type": "string"}],
        iceberg_schema={"type": "struct", "schema-id": schema_id, "fields": []},
    )


def build_table_record(materialization_id: str, *, name: str = "orders", updated_at: str = "2026-07-12T00:02:00Z") -> MaterializationTable:
    return MaterializationTable(
        materialization_id=materialization_id,
        dataset="shop",
        dataset_id="ds-000000000001",
        name=name,
        source_topic=name,
        location=f"gs://lake/materializer/{materialization_id}/{name}",
        table_uuid="6b1c0000-0000-0000-0000-000000000000",
        partition_granularity="day",
        created_at="2026-07-12T00:02:00Z",
        updated_at=updated_at,
    )


class StorageKeyTests(unittest.TestCase):
    def test_key_builders_match_contract_prefixes(self) -> None:
        self.assertEqual(materialization_prefix(), MATERIALIZATION_KEY_PREFIX)
        self.assertEqual(materialization_key("mat-123"), "materialization:mat-123")
        self.assertEqual(
            materialization_status_key("mat-123"),
            "materialization-status:mat-123",
        )
        self.assertEqual(
            materialization_schema_key("mat-123", "orders"),
            "materialization-schema:mat-123:orders",
        )
        self.assertEqual(
            materialization_schema_version_key("mat-123", "orders", 2),
            "materialization-schema-version:mat-123:orders:0000000002",
        )
        self.assertEqual(materialization_table_key("mat-123", "orders"), "materialization-table:mat-123:orders")
        self.assertEqual(
            materialization_operation_key("mat-123", "op-1"),
            "materialization-op:mat-123:op-1",
        )
        self.assertEqual(runtime_binding_key("mat-123"), "runtime-binding:mat-123")
        self.assertEqual(MATERIALIZATION_STATUS_KEY_PREFIX, "materialization-status:")
        self.assertEqual(MATERIALIZATION_SCHEMA_KEY_PREFIX, "materialization-schema:")
        self.assertEqual(MATERIALIZATION_OP_KEY_PREFIX, "materialization-op:")
        self.assertEqual(RUNTIME_BINDING_KEY_PREFIX, "runtime-binding:")


class StoragePersistenceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.table = FakeTable()

    def test_materialization_save_and_get_round_trip(self) -> None:
        materialization = build_materialization("mat-123", "2026-07-12T00:01:00Z")

        stored = save_materialization(self.table, materialization, version=3)

        self.assertEqual(decode_envelope(stored).version, 3)
        self.assertEqual(get_materialization(self.table, "mat-123"), materialization)

    def test_materialization_status_save_and_get_round_trip(self) -> None:
        status = build_status("mat-123", "running", "runtime_ready")

        save_materialization_status(self.table, status)

        self.assertEqual(get_materialization_status(self.table, "mat-123"), status)

    def test_runtime_binding_save_and_get_round_trip(self) -> None:
        binding = RuntimeBinding(
            materialization_id="mat-123",
            runtime_mode="dedicated",
            kaspr_app_name="kaspr-materializer-orders",
            namespace="midstream",
            managed_agent_names=["writer", "schema-proposer"],
            template_version="v1",
            last_applied_spec_hash="abc123",
            last_apply_operation_id="op-1",
            source_topics=["orders", "payments"],
            destination_uri="abfs://lakehouse/orders",
            created_at="2026-07-12T00:00:00Z",
            updated_at="2026-07-12T00:01:00Z",
        )

        save_runtime_binding(self.table, binding)

        self.assertEqual(get_runtime_binding(self.table, "mat-123"), binding)

    def test_schema_projection_save_and_get_round_trip(self) -> None:
        schema = build_schema("mat-123", 0)

        save_materialization_schema(self.table, schema)

        self.assertEqual(get_materialization_schema(self.table, "mat-123", "orders"), schema)

    def test_saving_a_schema_archives_each_one(self) -> None:
        for schema_id in (0, 1, 2):
            save_materialization_schema(self.table, build_schema("mat-123", schema_id))

        archived = list_schema_versions(self.table, "mat-123", "orders")

        self.assertEqual([item.schema_id for item in archived], [2, 1, 0])
        self.assertEqual(get_schema_version(self.table, "mat-123", "orders", 1).schema_id, 1)
        self.assertEqual(get_materialization_schema(self.table, "mat-123", "orders").schema_id, 2)

    def test_an_older_schema_never_replaces_the_current_one(self) -> None:
        save_materialization_schema(self.table, build_schema("mat-123", 2))
        save_materialization_schema(self.table, build_schema("mat-123", 1))

        self.assertEqual(get_materialization_schema(self.table, "mat-123", "orders").schema_id, 2)
        self.assertEqual([item.schema_id for item in list_schema_versions(self.table, "mat-123", "orders")], [2, 1])

    def test_archived_versions_are_isolated_per_materialization(self) -> None:
        save_materialization_schema(self.table, build_schema("mat-a", 0))
        save_materialization_schema(self.table, build_schema("mat-b", 0))
        save_materialization_schema(self.table, build_schema("mat-b", 1))

        self.assertEqual(len(list_schema_versions(self.table, "mat-a", "orders")), 1)
        self.assertEqual(len(list_schema_versions(self.table, "mat-b", "orders")), 2)
        self.assertIsNone(get_schema_version(self.table, "mat-a", "orders", 1))

    def test_archive_retains_only_the_configured_history_limit(self) -> None:
        config = ControlPlaneConfig(schema_history_limit=3)

        for schema_id in range(6):
            save_materialization_schema(self.table, build_schema("mat-123", schema_id), config=config)

        archived = list_schema_versions(self.table, "mat-123", "orders")

        self.assertEqual([item.schema_id for item in archived], [5, 4, 3])
        self.assertIsNone(get_schema_version(self.table, "mat-123", "orders", 2))

    def test_schema_version_keys_sort_numerically(self) -> None:
        for schema_id in (2, 11):
            save_materialization_schema(self.table, build_schema("mat-123", schema_id))

        keys = [key for key in sorted(self.table.values) if key.startswith(MATERIALIZATION_SCHEMA_VERSION_KEY_PREFIX)]

        self.assertEqual(keys[0], materialization_schema_version_key("mat-123", "orders", 2))
        self.assertEqual(keys[1], materialization_schema_version_key("mat-123", "orders", 11))

    def test_archiving_can_be_disabled_for_a_single_write(self) -> None:
        save_materialization_schema(self.table, build_schema("mat-123", 0), archive=False)

        self.assertEqual(list_schema_versions(self.table, "mat-123", "orders"), [])
        self.assertIsNotNone(get_materialization_schema(self.table, "mat-123", "orders"))

    def test_delete_schema_versions_removes_the_whole_archive(self) -> None:
        save_materialization_schema(self.table, build_schema("mat-123", 0))
        save_materialization_schema(self.table, build_schema("mat-123", 1))

        removed = delete_schema_versions(self.table, "mat-123")

        self.assertEqual(removed, 2)
        self.assertEqual(list_schema_versions(self.table, "mat-123", "orders"), [])

    def test_each_table_keeps_its_own_schemas(self) -> None:
        save_materialization_schema(self.table, build_schema("mat-123", 0, table="orders"))
        save_materialization_schema(self.table, build_schema("mat-123", 1, table="orders"))
        save_materialization_schema(self.table, build_schema("mat-123", 0, table="orders_v2"))

        self.assertEqual(1, get_materialization_schema(self.table, "mat-123", "orders").schema_id)
        self.assertEqual(0, get_materialization_schema(self.table, "mat-123", "orders_v2").schema_id)
        self.assertEqual([1, 0], [item.schema_id for item in list_schema_versions(self.table, "mat-123", "orders")])
        self.assertEqual([0], [item.schema_id for item in list_schema_versions(self.table, "mat-123", "orders_v2")])
        self.assertIsNone(get_schema_version(self.table, "mat-123", "orders_v2", 1))
        self.assertIsNone(get_materialization_schema(self.table, "mat-123", "payments"))

        self.assertEqual(3, delete_schema_versions(self.table, "mat-123"))
        self.assertEqual(2, delete_materialization_schemas(self.table, "mat-123"))
        self.assertIsNone(get_materialization_schema(self.table, "mat-123", "orders"))

    def test_a_schema_written_before_1_0_is_not_read(self) -> None:
        self.table[materialization_schema_key("mat-123", "orders")] = {
            "entity_type": "materialization-schema",
            "entity_id": "mat-123",
            "version": 1,
            "updated_at": "2026-07-12T00:02:00Z",
            "payload": {"materialization_id": "mat-123", "schema_version": 3, "schema": {"fields": []}},
        }

        self.assertIsNone(get_materialization_schema(self.table, "mat-123", "orders"))

    def test_schema_lookup_ignores_archived_versions(self) -> None:
        save_materialization_schema(self.table, build_schema("mat-123", 0))

        listed = list_materializations(self.table)

        self.assertEqual(listed["returned"], 0)

    def test_table_record_save_and_get_round_trip(self) -> None:
        record = build_table_record("mat-123").with_maintenance(
            MaintenanceRun(job="compaction", completed_at="2026-07-12T01:00:00Z", counts={"inputs": 8}),
            updated_at="2026-07-12T01:00:00Z",
        )
        record = MaterializationTable.from_dict(
            {**record.to_dict(), "last_error": TableError("commit_failed", "GCS is down", "2026-07-12T01:00:00Z").to_dict()}
        )

        save_materialization_table(self.table, record)

        self.assertEqual(get_materialization_table(self.table, "mat-123", "orders"), record)
        self.assertIn(materialization_table_key("mat-123", "orders"), self.table.values)

    def test_a_materialization_lists_each_of_its_tables_once(self) -> None:
        save_materialization_table(self.table, build_table_record("mat-123", name="payments"))
        save_materialization_table(self.table, build_table_record("mat-123", name="orders"))
        save_materialization_table(self.table, build_table_record("mat-1234", name="orders"))
        newer = build_table_record("mat-123", name="orders", updated_at="2026-07-12T00:09:00Z")
        save_materialization_table(self.table, newer)

        records = list_materialization_tables(self.table, "mat-123")

        self.assertEqual(["orders", "payments"], sorted(record.name for record in records))
        self.assertEqual(newer, next(record for record in records if record.name == "orders"))
        self.assertIsNone(get_materialization_table(self.table, "mat-123", "customers"))

    def test_maintenance_history_is_bounded(self) -> None:
        record = build_table_record("mat-123")
        for index in range(60):
            record = record.with_maintenance(
                MaintenanceRun(job="expire_snapshots", completed_at=f"2026-07-12T01:{index:02d}:00Z"),
                updated_at="2026-07-12T02:00:00Z",
            )

        self.assertEqual(50, len(record.maintenance))
        self.assertEqual("2026-07-12T01:59:00Z", record.maintenance[0].completed_at)

    def test_operation_record_save_and_get_round_trip(self) -> None:
        save_operation_record(
            self.table,
            materialization_id="mat-123",
            operation_id="op-1",
            updated_at="2026-07-12T00:03:00Z",
            payload={
                "operation_id": "op-1",
                "status": "completed",
                "dedupe_key": "apply:mat-123:op-1",
            },
            version=2,
        )

        stored = get_operation_record(
            self.table,
            materialization_id="mat-123",
            operation_id="op-1",
        )

        self.assertIsNotNone(stored)
        self.assertEqual(stored["entity_type"], "materialization-op")
        self.assertEqual(stored["version"], 2)
        self.assertEqual(stored["payload"]["status"], "completed")

    def test_delete_helpers_remove_records(self) -> None:
        save_materialization(self.table, build_materialization("mat-123", "2026-07-12T00:01:00Z"))
        save_materialization_status(self.table, build_status("mat-123", "running", "runtime_ready"))
        save_runtime_binding(
            self.table,
            RuntimeBinding(
                materialization_id="mat-123",
                runtime_mode="dedicated",
                kaspr_app_name="kaspr-materializer-orders",
                namespace="midstream",
                managed_agent_names=["writer"],
                template_version="v1",
                last_applied_spec_hash="abc123",
                last_apply_operation_id="op-1",
                updated_at="2026-07-12T00:01:00Z",
            ),
        )
        save_materialization_schema(self.table, build_schema("mat-123", 0))
        save_materialization_table(self.table, build_table_record("mat-123"))
        save_operation_record(
            self.table,
            materialization_id="mat-123",
            operation_id="op-1",
            updated_at="2026-07-12T00:03:00Z",
            payload={"operation_id": "op-1"},
        )

        delete_materialization(self.table, "mat-123")
        delete_materialization_status(self.table, "mat-123")
        delete_runtime_binding(self.table, "mat-123")
        delete_materialization_schemas(self.table, "mat-123")
        delete_materialization_table(self.table, "mat-123", "orders")
        delete_operation_record(self.table, materialization_id="mat-123", operation_id="op-1")

        self.assertIsNone(get_materialization(self.table, "mat-123"))
        self.assertIsNone(get_materialization_status(self.table, "mat-123"))
        self.assertIsNone(get_runtime_binding(self.table, "mat-123"))
        self.assertIsNone(get_materialization_schema(self.table, "mat-123", "orders"))
        self.assertIsNone(get_materialization_table(self.table, "mat-123", "orders"))
        self.assertIsNone(
            get_operation_record(
                self.table,
                materialization_id="mat-123",
                operation_id="op-1",
            )
        )


class SchemaRegressionRepairTests(unittest.TestCase):
    def setUp(self) -> None:
        self.table = FakeTable()

    def test_resolve_restores_newest_archived_schema_when_current_regressed(self) -> None:
        save_materialization_schema(self.table, build_schema("mat-123", 1))
        stale = dict(self.table.values[materialization_schema_key("mat-123", "orders")])
        save_materialization_schema(self.table, build_schema("mat-123", 2))
        # A lagging changelog partition replays the schema 1 record over the current one.
        self.table[materialization_schema_key("mat-123", "orders")] = stale
        self.assertEqual(get_materialization_schema(self.table, "mat-123", "orders").schema_id, 1)

        resolved = resolve_materialization_schema(self.table, "mat-123", "orders")

        self.assertEqual(resolved.schema_id, 2)
        self.assertEqual(get_materialization_schema(self.table, "mat-123", "orders").schema_id, 2)

    def test_resolve_leaves_current_schema_untouched_when_it_is_newest(self) -> None:
        save_materialization_schema(self.table, build_schema("mat-123", 3))

        resolved = resolve_materialization_schema(self.table, "mat-123", "orders")

        self.assertEqual(resolved.schema_id, 3)

    def test_resolve_returns_none_when_nothing_is_stored(self) -> None:
        self.assertIsNone(resolve_materialization_schema(self.table, "mat-missing", "orders"))


class MaterializationListingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.table = FakeTable()

    def test_listing_scans_only_materialization_prefix(self) -> None:
        save_materialization(self.table, build_materialization("mat-1", "2026-07-12T00:01:00Z"))
        save_materialization_status(self.table, build_status("mat-1", "running", "runtime_ready"))
        self.table["some-other-prefix:key"] = {"entity_type": "ignored", "entity_id": "x", "version": 1, "updated_at": "2026-07-12T00:00:00Z", "payload": {}}

        result = list_materializations(self.table, limit="30")

        self.assertEqual(self.table.prefix_scan_calls, ["materialization:"])
        self.assertEqual(result["returned"], 1)
        self.assertEqual(result["items"][0]["materialization_id"], "mat-1")

    def test_listing_respects_bounded_limit(self) -> None:
        for index in range(12):
            materialization_id = f"mat-{index:02d}"
            save_materialization(
                self.table,
                build_materialization(materialization_id, f"2026-07-12T00:{index:02d}:00Z"),
            )
            save_materialization_status(
                self.table,
                build_status(materialization_id, "running", "runtime_ready"),
            )

        result = list_materializations(self.table, limit="10")

        self.assertEqual(result["limit"], "10")
        self.assertEqual(result["returned"], 10)
        self.assertEqual(len(result["items"]), 10)

    def test_listing_uses_status_when_available(self) -> None:
        save_materialization(self.table, build_materialization("mat-123", "2026-07-12T00:01:00Z"))
        save_materialization_status(self.table, build_status("mat-123", "degraded", "resource_not_ready"))

        result = list_materializations(self.table, limit="all")

        self.assertEqual(result["items"][0]["desired_phase"], "running")
        self.assertEqual(result["items"][0]["observed_phase"], "degraded")
        self.assertEqual(result["items"][0]["phase_reason"], "resource_not_ready")

    def test_listing_falls_back_when_status_is_missing(self) -> None:
        save_materialization(self.table, build_materialization("mat-123", "2026-07-12T00:01:00Z"))

        result = list_materializations(self.table, limit="all")

        self.assertEqual(result["items"][0]["observed_phase"], "running")
        self.assertEqual(result["items"][0]["phase_reason"], "running")
        self.assertEqual(result["items"][0]["updated_at"], "2026-07-12T00:01:00Z")



def setUpModule() -> None:
    # A deployment always names the namespace its runtimes run in (handbook 02 §2.3); these tests
    # reach the configuration without choosing one, so the environment names it for them.
    os.environ.setdefault("MATERIALIZER_CONTROL_PLANE_DEFAULT_NAMESPACE", "midstream")
    control_plane_config.get_config.cache_clear()


if __name__ == "__main__":
    unittest.main()

class OrphanShardTable(FakeTable):
    """A table whose prefix scan also yields a stale copy of an existing key.

    Reproduces what a partitioned table does when a key has an orphaned copy in a shard it no
    longer routes to: the keyed read returns the current value, the scan returns both.
    """

    def __init__(self) -> None:
        super().__init__()
        self.orphans: list[tuple[str, dict]] = []

    def prefix_scan(self, prefix: str) -> Iterable[tuple[str, dict]]:
        yield from super().prefix_scan(prefix)
        for key, value in self.orphans:
            if key.startswith(prefix):
                yield key, value


class UnreadableRecordTests(unittest.TestCase):
    """A record an earlier, incompatible release left behind must not fail what scans past it.

    Deleting every 1.0 materialization before the upgrade still left one behind on the lab: a copy
    in another shard of the state table, in the 1.0 shape (one `source_topic`), which 2.0 cannot
    read. The fleet list, the sweep, the lag refresh, the name check and the catalog all scan it.
    """

    def setUp(self) -> None:
        self.table = FakeTable()
        save_materialization(self.table, build_materialization("mat-2", "2026-07-12T00:01:00Z"))
        self._save_legacy("mat-1")

    def _save_legacy(self, materialization_id: str) -> None:
        self.table[materialization_key(materialization_id)] = {
            "entity_type": "materialization",
            "entity_id": materialization_id,
            "version": 1,
            "updated_at": "2026-07-12T00:00:00Z",
            "payload": {
                "materialization_id": materialization_id,
                "name": "orders",
                "source_topic": "orders",
                "destination_uri": "gs://lake/orders",
                "desired_phase": "deleted",
                "created_at": "2026-07-11T00:00:00Z",
                "updated_at": "2026-07-12T00:00:00Z",
            },
        }
        self.table[f"materialization-table:{materialization_id}:orders"] = {
            "entity_type": "materialization-table",
            "entity_id": f"{materialization_id}:orders",
            "version": 1,
            "updated_at": "2026-07-12T00:00:00Z",
            "payload": {"materialization_id": materialization_id, "location": "gs://lake/orders"},
        }

    def test_the_fleet_list_skips_it(self) -> None:
        result = list_materializations(self.table, limit="all")

        self.assertEqual([item["materialization_id"] for item in result["items"]], ["mat-2"])

    def test_reading_it_finds_nothing(self) -> None:
        self.assertIsNone(get_materialization(self.table, "mat-1"))
        self.assertIsNone(get_materialization_table(self.table, "mat-1", "orders"))
        self.assertEqual(list_materialization_tables(self.table, "mat-1"), [])

    def test_a_record_whose_tables_have_no_dataset_is_skipped_too(self) -> None:
        # The 2.0 shape: tables named in the materialization's namespace, none in a dataset. 3.0
        # starts clean, and must not fail on one left behind.
        self.table[materialization_key("mat-20")] = {
            "entity_type": "materialization",
            "entity_id": "mat-20",
            "version": 1,
            "updated_at": "2026-07-12T00:00:00Z",
            "payload": {
                "materialization_id": "mat-20",
                "name": "shop",
                "tables": [{"name": "orders", "source_topic": "orders"}],
                "destination_uri": "gs://lake/orders",
                "desired_phase": "running",
                "created_at": "2026-07-11T00:00:00Z",
                "updated_at": "2026-07-12T00:00:00Z",
            },
        }

        self.assertIsNone(get_materialization(self.table, "mat-20"))
        self.assertEqual(
            [item["materialization_id"] for item in list_materializations(self.table, limit="all")["items"]],
            ["mat-2"],
        )

    def test_it_is_reported_once(self) -> None:
        self._save_legacy("mat-reported-once")

        with self.assertLogs("midstream.storage.envelopes", level="WARNING") as logged:
            list_materializations(self.table, limit="all")
            list_materializations(self.table, limit="all")

        reported = [message for message in logged.output if "mat-reported-once" in message]
        self.assertEqual(len(reported), 1, logged.output)


class MaterializationListShardOrphanTests(unittest.TestCase):
    def test_list_prefers_the_keyed_record_and_never_repeats_an_id(self) -> None:
        """Listing must agree with the detail view, and must not list an id twice.

        A stale copy in another shard made deleted materializations reappear as `running` in the
        fleet list after a control-plane restart, while opening one showed it as deleted.
        """
        from dataclasses import replace

        from midstream.storage.envelopes import encode_payload_envelope
        from midstream.storage.keys import materialization_key

        table = OrphanShardTable()
        current = build_materialization("mat-1", "2026-07-12T03:27:00Z")
        current = replace(current, desired_phase="deleted")
        save_materialization(table, current)

        stale = replace(current, desired_phase="running")
        table.orphans.append(
            (
                materialization_key("mat-1"),
                encode_payload_envelope(
                    entity_type="materialization",
                    entity_id="mat-1",
                    payload=stale.to_dict(),
                    updated_at=stale.updated_at,
                ),
            )
        )

        result = list_materializations(table, limit="all")
        ids = [item["materialization_id"] for item in result["items"]]

        self.assertEqual(ids, ["mat-1"], "the same materialization must not be listed twice")
        self.assertEqual(result["items"][0]["desired_phase"], "deleted")

    def test_newest_copy_wins_regardless_of_which_shard_answers(self) -> None:
        """The deleted state must survive, even when the stale copy is scanned first.

        A keyed read returns whichever shard answers, and that answer changed across a redeploy —
        which is how a deleted materialization reappeared as Running. Ordering by the envelope's
        `updated_at` is deterministic: the latest write is the current state.
        """
        from dataclasses import replace

        from midstream.storage.envelopes import encode_payload_envelope
        from midstream.storage.keys import materialization_key

        table = OrphanShardTable()
        # The stale copy is the one saved under the key, so it is scanned first.
        stale = replace(
            build_materialization("mat-1", "2026-07-12T01:01:16Z"), desired_phase="running"
        )
        save_materialization(table, stale)

        deleted = replace(
            build_materialization("mat-1", "2026-07-12T03:27:00Z"), desired_phase="deleted"
        )
        table.orphans.append(
            (
                materialization_key("mat-1"),
                encode_payload_envelope(
                    entity_type="materialization",
                    entity_id="mat-1",
                    payload=deleted.to_dict(),
                    updated_at=deleted.updated_at,
                ),
            )
        )

        result = list_materializations(table, limit="all")

        self.assertEqual([item["materialization_id"] for item in result["items"]], ["mat-1"])
        self.assertEqual(result["items"][0]["desired_phase"], "deleted")
