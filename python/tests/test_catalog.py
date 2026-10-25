"""The Iceberg REST catalog facade: what external engines see of the datasets' tables (spec §5.6)."""

from __future__ import annotations

import asyncio
from collections.abc import Iterable
from dataclasses import replace
import json
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from lakehouse_support import LOCATION, MATERIALIZATION_ID, MATERIALIZATION_LOCATION, NAMESPACE, LakeFixture  # noqa: E402
from table_records import table_spec  # noqa: E402

from materializer_control_plane.config import ControlPlaneConfig  # noqa: E402
from materializer_control_plane.identity.principals import Grants, Principal  # noqa: E402
from materializer_control_plane.lakehouse.pointer import read_pointer  # noqa: E402
from materializer_control_plane.lakehouse.rest import ENDPOINTS, CatalogError  # noqa: E402
from materializer_control_plane.models import Dataset, Materialization, MaterializationTable, TableClaim  # noqa: E402
from materializer_control_plane.storage import get_materialization_table, save_materialization, save_materialization_table  # noqa: E402
from materializer_control_plane.storage.identity import (  # noqa: E402
    delete_table_claim,
    save_dataset,
    save_dataset_name,
    save_table_claim,
)
from materializer_control_plane.web.catalog import (  # noqa: E402
    catalog_commit_view,
    catalog_config_view,
    catalog_errors,
    catalog_load_table_view,
    catalog_metrics_view,
    catalog_namespace_view,
    catalog_namespaces_view,
    catalog_not_supported_view,
    catalog_tables_view,
    reset_catalog_cache,
)
from materializer_control_plane.web.errors import forbidden  # noqa: E402

CONFIG = ControlPlaneConfig(storage_prefix="materializer", catalog_metadata_cache_seconds=60)
PAYMENTS_LOCATION = f"{MATERIALIZATION_LOCATION}/payments"
#: Another materialization's table, in another dataset.
OTHER_ID = "mat-0f0f0f0f0f0f"
INVOICES_LOCATION = "gs://lake/materializer/mat-0f0f0f0f0f0f/invoices"
SHOP, FINANCE, SECRET = "ds-000000000001", "ds-000000000002", "ds-000000000003"
NOW = "2026-10-18T00:00:00Z"


class FakeTable:
    def __init__(self) -> None:
        self.values: dict[str, dict] = {}

    def get(self, key: str) -> dict | None:
        return self.values.get(key)

    def __setitem__(self, key: str, value: dict) -> None:
        self.values[key] = value

    def __delitem__(self, key: str) -> None:
        self.values.pop(key, None)

    def prefix_scan(self, prefix: str) -> Iterable[tuple[str, dict]]:
        for key in sorted(self.values):
            if key.startswith(prefix):
                yield key, self.values[key]


def run(coroutine):
    return asyncio.run(coroutine)


def reader_of(*dataset_ids: str, write: bool = False) -> Principal:
    """Someone who reads these datasets through the catalog - and commits to them, with `write`."""
    permissions = frozenset({"catalog.read", "dataset.view_schema", *(("catalog.write",) if write else ())})
    return Principal(
        kind="user",
        user_id="usr-000000000001",
        email="jane@example.com",
        grants=Grants(group_ids=(), role_ids=(), permissions=frozenset(), datasets={dataset_id: permissions for dataset_id in dataset_ids}),
    )


def save_record(state: FakeTable, lake: LakeFixture, *, materialization_id: str, dataset: str, dataset_id: str, source_topic: str) -> None:
    table = lake.load()
    save_materialization_table(
        state,
        MaterializationTable(
            materialization_id=materialization_id,
            dataset=dataset,
            dataset_id=dataset_id,
            name=lake.identifier[1],
            source_topic=source_topic,
            location=lake.location,
            table_uuid=str(table.metadata.table_uuid),
            partition_granularity="day",
            created_at=NOW,
            updated_at=NOW,
            metadata_location=table.metadata_location,
        ),
    )


def claim(directory: FakeTable, dataset_id: str, table: str, materialization_id: str) -> None:
    save_table_claim(
        directory,
        TableClaim(
            dataset_id=dataset_id,
            table=table,
            materialization_id=materialization_id,
            source_topic=table,
            claimed_at=NOW,
            claimed_by="jane@example.com",
            request_id="idr-1",
        ),
    )


class CatalogTests(unittest.TestCase):
    """Dataset `shop` holds `orders` and `payments` of one materialization; `finance` holds
    `invoices` of another; `secret` holds nothing, and only some may read it."""

    def setUp(self) -> None:
        reset_catalog_cache()
        self.addCleanup(reset_catalog_cache)
        self.lake = LakeFixture({"order_id": "O-1", "amount": 5})
        self.addCleanup(self.lake.close)
        self.payments = LakeFixture(
            {"payment_id": "P-1"},
            location=PAYMENTS_LOCATION,
            identifier=(NAMESPACE, "payments"),
            source_topic="payments",
            store=self.lake.store,
        )
        self.addCleanup(self.payments.close)
        self.invoices = LakeFixture(
            {"invoice_id": "I-1"},
            location=INVOICES_LOCATION,
            identifier=("finance", "invoices"),
            source_topic="invoices",
            store=self.lake.store,
            materialization_id=OTHER_ID,
        )
        self.addCleanup(self.invoices.close)
        self.state = FakeTable()
        self.directory = FakeTable()
        for dataset_id, name, description in ((SHOP, "shop", "Orders and payments"), (FINANCE, "finance", ""), (SECRET, "secret", "")):
            save_dataset(self.directory, Dataset(dataset_id=dataset_id, name=name, description=description, created_at=NOW, created_by="a", updated_at=NOW, updated_by="a"))
            save_dataset_name(self.directory, name, dataset_id, updated_at=NOW)
        self.materialization = Materialization(
            materialization_id=MATERIALIZATION_ID,
            namespace="midstream",
            name="Shop",
            tables=(table_spec("orders", "orders"), table_spec("payments", "payments")),
            destination_uri="gs://lake",
            desired_phase="running",
            created_at=NOW,
            updated_at=NOW,
        )
        save_materialization(self.state, self.materialization)
        save_record(self.state, self.lake, materialization_id=MATERIALIZATION_ID, dataset="shop", dataset_id=SHOP, source_topic="orders")
        save_record(self.state, self.payments, materialization_id=MATERIALIZATION_ID, dataset="shop", dataset_id=SHOP, source_topic="payments")
        save_record(self.state, self.invoices, materialization_id=OTHER_ID, dataset="finance", dataset_id=FINANCE, source_topic="invoices")
        claim(self.directory, SHOP, "orders", MATERIALIZATION_ID)
        claim(self.directory, SHOP, "payments", MATERIALIZATION_ID)
        claim(self.directory, FINANCE, "invoices", OTHER_ID)
        self.reader = reader_of(SHOP, FINANCE)

    def views(self, principal: Principal | None = None, **extra):
        return {"directory_table": self.directory, "principal": principal or self.reader, "config": CONFIG, **extra}

    def load(self, name: str = "orders", *, namespace: str = NAMESPACE, principal: Principal | None = None, **kwargs):
        config = kwargs.pop("config", CONFIG)
        return run(
            catalog_load_table_view(
                namespace,
                name,
                self.state,
                directory_table=self.directory,
                principal=principal or self.reader,
                config=config,
                client=self.lake.client,
                **kwargs,
            )
        )

    def commit(self, body, *, principal: Principal | None = None, name: str = "orders"):
        return run(
            catalog_commit_view(
                NAMESPACE,
                name,
                body,
                self.state,
                committed_by="jane@example.com",
                directory_table=self.directory,
                principal=principal or reader_of(SHOP, write=True),
                config=CONFIG,
                client=self.lake.client,
            )
        )

    def test_the_configuration_lists_what_the_catalog_answers(self) -> None:
        config = catalog_config_view(config=CONFIG)
        self.assertEqual({}, config["overrides"])
        self.assertEqual(list(ENDPOINTS), config["endpoints"])
        self.assertIn("GET /v1/{prefix}/namespaces/{namespace}/tables/{table}", config["endpoints"])

    def test_a_catalog_turned_off_answers_503(self) -> None:
        with self.assertRaises(CatalogError) as caught:
            catalog_config_view(config=replace(CONFIG, catalog_api_enabled=False))
        self.assertEqual(503, caught.exception.status_code)
        self.assertEqual("ServiceUnavailableException", caught.exception.to_response()["error"]["type"])

    def test_every_dataset_the_caller_reads_is_a_namespace(self) -> None:
        self.assertEqual(
            {"namespaces": [["finance"], ["shop"]], "next-page-token": None},
            catalog_namespaces_view(self.state, **self.views()),
        )
        self.assertEqual([], catalog_namespaces_view(self.state, parent="shop", **self.views())["namespaces"])
        # Namespaces are matched as engines match names, without regard to case.
        self.assertEqual(
            {"namespace": ["shop"], "properties": {"description": "Orders and payments"}},
            catalog_namespace_view("SHOP", self.state, **self.views()),
        )
        self.assertEqual({"namespace": ["finance"], "properties": {}}, catalog_namespace_view("finance", self.state, **self.views()))
        # A dataset they cannot read is not listed, and answers as though it did not exist.
        for unknown in ("secret", "materializer", "Shop_", "other"):
            with self.subTest(namespace=unknown), self.assertRaises(CatalogError) as caught:
                catalog_namespace_view(unknown, self.state, **self.views())
            self.assertEqual((404, "NoSuchNamespaceException"), (caught.exception.status_code, caught.exception.error_type))
        self.assertEqual(
            {"namespaces": [["shop"]], "next-page-token": None},
            catalog_namespaces_view(self.state, **self.views(reader_of(SHOP))),
        )

    def test_a_dataset_lists_its_tables_whatever_materialization_writes_them(self) -> None:
        claim(self.directory, FINANCE, "orders_summary", MATERIALIZATION_ID)
        listed = catalog_tables_view("shop", self.state, **self.views())
        self.assertEqual(
            [{"namespace": ["shop"], "name": "orders"}, {"namespace": ["shop"], "name": "payments"}],
            listed["identifiers"],
        )
        # A name claimed for a table not created yet lists nothing.
        self.assertEqual(["invoices"], [item["name"] for item in catalog_tables_view("finance", self.state, **self.views())["identifiers"]])

    def test_one_engine_reads_tables_of_two_datasets_and_two_materializations(self) -> None:
        orders = self.load("orders", namespace="shop")
        invoices = self.load("invoices", namespace="finance")

        self.assertEqual(LOCATION, orders["metadata"]["location"])
        self.assertEqual(INVOICES_LOCATION, invoices["metadata"]["location"])
        with self.assertRaises(CatalogError) as caught:
            self.load("invoices", namespace="finance", principal=reader_of(SHOP))
        self.assertEqual((404, "NoSuchNamespaceException"), (caught.exception.status_code, caught.exception.error_type))

    def test_each_table_loads_from_its_own_location(self) -> None:
        orders, payments = self.load("orders"), self.load("payments")

        self.assertEqual(LOCATION, orders["metadata"]["location"])
        self.assertEqual(PAYMENTS_LOCATION, payments["metadata"]["location"])
        self.assertNotEqual(orders["metadata"]["table-uuid"], payments["metadata"]["table-uuid"])
        with self.assertRaises(CatalogError) as caught:
            self.load("orders", namespace="warehouse")
        self.assertEqual((404, "NoSuchNamespaceException"), (caught.exception.status_code, caught.exception.error_type))

    def test_loading_a_table_returns_its_metadata_location_and_metadata(self) -> None:
        result = self.load()

        pointer, _ = read_pointer(self.lake.client, LOCATION)
        self.assertEqual(pointer.metadata_location, result["metadata-location"])
        self.assertEqual(2, result["metadata"]["format-version"])
        self.assertEqual(LOCATION, result["metadata"]["location"])
        self.assertEqual({}, result["config"])
        json.dumps(result)
        # Names are matched as engines match them, without regard to case.
        self.assertEqual(result, self.load("ORDERS"))

    def test_a_loaded_table_is_answered_from_memory_until_its_cache_expires(self) -> None:
        self.load()
        calls = len(self.lake.store.calls)
        self.load()
        self.assertEqual(calls, len(self.lake.store.calls))

        # Past the cache, the pointer is read again; an unchanged table is not downloaded again.
        reset = replace(CONFIG, catalog_metadata_cache_seconds=1)
        from materializer_control_plane.web import catalog as catalog_module

        for cached in catalog_module._CACHE.values():
            cached.fetched_at -= 10
        before = len(self.lake.store.calls)
        self.load(config=reset)
        downloads = [call for call in self.lake.store.calls[before:] if call[0] == "download"]
        self.assertEqual(1, len(downloads))

    def test_a_name_taken_by_another_table_never_answers_with_the_first_ones_metadata(self) -> None:
        """A freed name can later be another table's: the cache knows each by its UUID."""
        first = self.load("orders")
        # `orders` is dropped from shop, and invoices' materialization takes the name for its own table.
        delete_table_claim(self.directory, SHOP, "orders")
        claim(self.directory, SHOP, "orders", OTHER_ID)
        invoices = get_materialization_table(self.state, OTHER_ID, "invoices")
        save_materialization_table(self.state, replace(invoices, name="orders", dataset="shop", dataset_id=SHOP))

        second = self.load("orders")

        self.assertNotEqual(first["metadata"]["table-uuid"], second["metadata"]["table-uuid"])
        self.assertEqual(INVOICES_LOCATION, second["metadata"]["location"])

    def test_a_kept_table_stays_in_its_dataset_and_a_dropped_one_leaves_it(self) -> None:
        save_materialization(
            self.state,
            replace(self.materialization, tables=self.materialization.tables[:1], retired_tables=self.materialization.tables[1:]),
        )
        payments = get_materialization_table(self.state, MATERIALIZATION_ID, "payments")
        save_materialization_table(self.state, replace(payments, state="retired"))

        listed = catalog_tables_view("shop", self.state, **self.views())["identifiers"]
        self.assertEqual(["orders", "payments"], [item["name"] for item in listed])
        self.assertEqual(PAYMENTS_LOCATION, self.load("payments")["metadata"]["location"])

        for state in ("dropping", "dropped"):
            with self.subTest(state=state):
                save_materialization_table(self.state, replace(payments, state=state))
                listed = catalog_tables_view("shop", self.state, **self.views())["identifiers"]
                self.assertEqual(["orders"], [item["name"] for item in listed])
                reset_catalog_cache()
                with self.assertRaises(CatalogError) as caught:
                    self.load("payments")
                self.assertEqual(404, caught.exception.status_code)

    def test_a_released_name_leaves_the_catalog(self) -> None:
        delete_table_claim(self.directory, SHOP, "payments")

        self.assertEqual(["orders"], [item["name"] for item in catalog_tables_view("shop", self.state, **self.views())["identifiers"]])
        with self.assertRaises(CatalogError) as caught:
            self.load("payments")
        self.assertEqual((404, "NoSuchTableException"), (caught.exception.status_code, caught.exception.error_type))

    def test_an_unknown_table_is_404(self) -> None:
        with self.assertRaises(CatalogError) as caught:
            self.load("customers")
        self.assertEqual((404, "NoSuchTableException"), (caught.exception.status_code, caught.exception.error_type))

    def test_an_engine_commits_through_the_pointer_and_is_recorded(self) -> None:
        loaded = self.load()
        body = {
            "requirements": [{"type": "assert-table-uuid", "uuid": loaded["metadata"]["table-uuid"]}],
            "updates": [{"action": "set-properties", "updates": {"owner": "analytics"}}],
        }

        response = self.commit(body)

        self.assertEqual("analytics", response["metadata"]["properties"]["owner"])
        pointer, _ = read_pointer(self.lake.client, LOCATION)
        self.assertEqual(response["metadata-location"], pointer.metadata_location)
        self.assertEqual("catalog:jane@example.com", pointer.updated_by)
        self.assertEqual("analytics", self.lake.load().metadata.properties["owner"])
        # The cache does not hide the commit.
        self.assertEqual(response["metadata-location"], self.load()["metadata-location"])

    def test_committing_needs_catalog_write_on_the_tables_dataset(self) -> None:
        body = {"requirements": [], "updates": [{"action": "set-properties", "updates": {"owner": "analytics"}}]}
        before = read_pointer(self.lake.client, LOCATION)[0].metadata_location

        for principal in (self.reader, reader_of(FINANCE, write=True)):
            with self.subTest(principal=principal.grants.datasets), self.assertRaises(CatalogError) as caught:
                self.commit(body, principal=principal)
            self.assertIn(caught.exception.status_code, (403, 404))
        with self.assertRaises(CatalogError) as caught:
            self.commit(body, principal=self.reader)
        self.assertEqual((403, "ForbiddenException"), (caught.exception.status_code, caught.exception.error_type))
        self.assertIn("dataset shop", caught.exception.message)
        self.assertEqual(before, read_pointer(self.lake.client, LOCATION)[0].metadata_location)

    def test_a_commit_against_a_stale_table_is_409(self) -> None:
        body = {
            "requirements": [{"type": "assert-current-schema-id", "current-schema-id": 7}],
            "updates": [{"action": "set-properties", "updates": {"owner": "analytics"}}],
        }
        with self.assertRaises(CatalogError) as caught:
            self.commit(body)
        self.assertEqual((409, "CommitFailedException"), (caught.exception.status_code, caught.exception.error_type))
        with self.assertRaises(CatalogError) as caught:
            self.commit(["not", "a", "commit"])
        self.assertEqual(400, caught.exception.status_code)

    def test_a_commit_cannot_move_the_tables_files_away_from_its_location(self) -> None:
        before = read_pointer(self.lake.client, LOCATION)[0].metadata_location
        for update in (
            {"action": "set-location", "location": "gs://elsewhere/orders"},
            {"action": "set-properties", "updates": {"write.metadata.path": "gs://elsewhere/metadata"}},
            {"action": "set-properties", "updates": {"owner": "analytics", "write.data.path": "gs://elsewhere/data"}},
            {"action": "set-properties", "updates": {"write.py-location-provider.impl": "somewhere.Provider"}},
        ):
            with self.subTest(update=update), self.assertRaises(CatalogError) as caught:
                self.commit({"requirements": [], "updates": [update]})
            self.assertEqual((400, "BadRequestException"), (caught.exception.status_code, caught.exception.error_type))
            self.assertIn("location", caught.exception.message)
        self.assertEqual(before, read_pointer(self.lake.client, LOCATION)[0].metadata_location)
        self.assertNotIn("write.data.path", self.lake.load().metadata.properties)

    def test_metrics_are_accepted_and_unsupported_operations_are_501(self) -> None:
        self.assertIsNone(catalog_metrics_view(NAMESPACE, "orders", self.state, **self.views()))
        for what in ("create_table", "drop_table", "transactions"):
            with self.subTest(what=what), self.assertRaises(CatalogError) as caught:
                catalog_not_supported_view(what)
            self.assertEqual(501, caught.exception.status_code)

    def test_a_refused_authorization_is_answered_in_the_shape_engines_parse(self) -> None:
        with self.assertRaises(CatalogError) as caught:
            with catalog_errors():
                raise forbidden("You do not have permission to do this.", permission="catalog.read")
        self.assertEqual(
            {"error": {"message": "You do not have permission to do this.", "type": "ForbiddenException", "code": 403}},
            caught.exception.to_response(),
        )


if __name__ == "__main__":
    unittest.main()
