from __future__ import annotations

import os
import asyncio
from collections.abc import Iterable
from dataclasses import replace
import unittest

from midstream.config import ControlPlaneConfig
from midstream.lakehouse.schema.forkmap import ForkMap
from midstream.lakehouse.schema.infer import infer
from midstream.lakehouse.schema.policy import evolve
from midstream.lakehouse.tables import build_schema
from midstream.models import (
    LagPoint,
    Materialization,
    MaterializationStatus,
    ResourceObservation,
    RuntimeAutoscale,
    RuntimeBinding,
    RuntimeHealth,
    RuntimeProgress,
)
from midstream.storage import (
    get_materialization,
    get_materialization_status,
    save_materialization,
    save_materialization_schema,
    save_materialization_status,
    save_materialization_table,
    save_runtime_autoscale,
    save_runtime_binding,
)
from midstream.models.table import MaintenanceRun
from midstream.services.schema_projection import build_schema_projection
from table_records import build_table_record, table_spec, table_specs
from midstream.kafka.metadata import TopicListing, TopicMetadata
from midstream.services.topic_catalog import TopicCatalogCache
from midstream.web import (
    WebError,
    analyze_topics_view,
    create_materialization,
    delete_materialization,
    get_materialization_detail,
    get_materialization_schema_version_view,
    get_materialization_schema_view,
    get_materialization_table_view,
    list_materialization_schema_versions_view,
    list_materializations_view,
    list_topics_view,
    request_maintenance,
    resume_materialization,
    settings_view,
    stop_materialization,
    update_materialization,
)
from midstream.web.errors import error_response
from midstream.identity.principals import Principal, grants_for_user
from midstream.models import Dataset, DatasetGrant, PendingTopicChange, TableClaim, TopicChangePlan, User
from midstream.storage.identity import (
    get_user,
    save_dataset,
    save_dataset_grant,
    save_dataset_name,
    save_table_claim,
    save_user,
)
from midstream.models import MaintenanceMessage
from midstream.services.settings import apply_settings_change
from midstream.models import SettingOverride, SettingsChangeRequest
from midstream.storage.settings import get_settings_document, save_settings_document
from midstream import config as control_plane_config


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


def build_materialization(materialization_id: str, *topics: str) -> Materialization:
    return Materialization(
        materialization_id=materialization_id,
        name="shop",
        tables=table_specs(*(topics or ("orders",))),
        destination_uri="abfs://lakehouse/orders",
        runtime_mode="dedicated",
        resolved_runtime_name="kaspr-mat-1",
        namespace="midstream",
        desired_phase="running",
        created_at="2026-07-12T00:00:00Z",
        updated_at="2026-07-12T00:00:00Z",
        created_by="alice",
    )


def build_status(materialization_id: str, desired_phase: str = "running", observed_phase: str = "running") -> MaterializationStatus:
    return MaterializationStatus(
        materialization_id=materialization_id,
        desired_phase=desired_phase,
        observed_phase=observed_phase,
        phase_reason="runtime_ready",
        phase_message="Healthy runtime status aligns with ready resource observation.",
        last_transition_time="2026-07-12T00:01:00Z",
        observation_source="hybrid",
        runtime_health=RuntimeHealth(level="healthy"),
        runtime_progress=RuntimeProgress(
            rows_written=1200,
            bytes_written=1048576,
            files_written=3,
            records_skipped=4,
            latest_offsets={"orders": {"0": 1024}},
        ),
        resource_observation=ResourceObservation(
            namespace="midstream",
            kaspr_app_name="kaspr-mat-1",
            resource_exists=True,
            managed_agent_states={"writer": "running"},
        ),
    )


def build_projection(schema_id: int, *records: dict, table: str = "orders"):
    """The projection of a table's schema evolved from `records`, as discovery and the committer store it."""
    evolution = evolve(build_schema(), ForkMap(), [((), infer(record)) for record in records or ({"f0": "x"},)], schema_id=schema_id)
    return build_schema_projection(
        "mat-1",
        table,
        evolution.schema,
        evolution.fork_map,
        since=f"2026-07-12T00:0{schema_id}:05Z",
        changes=[change.to_dict() for change in evolution.changes],
    )


JANE = "usr-000000000001"
SHOP, FINANCE, SECRET = "ds-000000000001", "ds-000000000002", "ds-000000000003"
NOW = "2026-07-12T00:00:00Z"


def build_directory() -> FakeTable:
    """Jane, an editor, owns dataset shop and writes to finance; secret she cannot even read."""
    directory = FakeTable()
    save_user(
        directory,
        User(
            user_id=JANE,
            email="jane@example.com",
            first_name="Jane",
            last_name="Doe",
            group_ids=["editor"],
            created_at=NOW,
            created_by="system",
            updated_at=NOW,
            updated_by="system",
        ),
    )
    for dataset_id, name in ((SHOP, "shop"), (FINANCE, "finance"), (SECRET, "secret")):
        save_dataset(directory, Dataset(dataset_id=dataset_id, name=name, created_at=NOW, created_by="a", updated_at=NOW, updated_by="a"))
        save_dataset_name(directory, name, dataset_id, updated_at=NOW)
    for dataset_id, role_id in ((SHOP, "dataset-owner"), (FINANCE, "dataset-writer")):
        save_dataset_grant(
            directory,
            DatasetGrant(dataset_id=dataset_id, principal_type="user", principal_id=JANE, role_ids=[role_id], updated_at=NOW, updated_by="a"),
        )
    return directory


def principal_of(directory: FakeTable, user_id: str = JANE) -> Principal:
    user = get_user(directory, user_id)
    return Principal(kind="user", grants=grants_for_user(directory, user), user_id=user.user_id, email=user.email)


def claim_name(directory: FakeTable, dataset_id: str, table: str, materialization_id: str = "mat-0") -> None:
    """`<dataset>.<table>` taken, as the directory agent records a claim."""
    save_table_claim(
        directory,
        TableClaim(
            dataset_id=dataset_id,
            table=table,
            materialization_id=materialization_id,
            source_topic=table,
            claimed_at=NOW,
            claimed_by="someone@example.com",
            request_id="idr-0",
        ),
    )


def tables(*topics: str, dataset: str = "shop") -> list[dict]:
    """A request's `tables`: each topic in `dataset`, named after it."""
    return [{"source_topic": topic, "dataset": dataset} for topic in topics]


class WebCreateTests(unittest.TestCase):
    def setUp(self) -> None:
        self.table = FakeTable()
        self.directory = build_directory()
        self.config = ControlPlaneConfig(default_destination_uri="gs://lake", default_namespace="midstream")

    def _create(
        self,
        body: dict,
        *,
        config: ControlPlaneConfig | None = None,
        materialization_id: str = "mat-1",
        settings_table: FakeTable | None = None,
    ):
        return create_materialization(
            self.table,
            body,
            submitted_by="jane@example.com",
            submitted_by_id=JANE,
            principal=principal_of(self.directory),
            directory_table=self.directory,
            materialization_id=materialization_id,
            request_id="req-1",
            claim_request_id="idr-claim000001",
            accepted_at=NOW,
            config=None if settings_table is not None else (config or self.config),
            settings_table=settings_table,
        )

    def _refused(self, body: dict, **kwargs) -> WebError:
        with self.assertRaises(WebError) as error_info:
            self._create(body, **kwargs)
        return error_info.exception

    def test_create_materialization_returns_request_event_without_persisting_records(self) -> None:
        acceptance = self._create(
            {"name": "Orders of the shop", "tables": tables("orders"), "labels": {"domain": "orders"}}
        )

        self.assertEqual(acceptance.response["observed_phase"], "requested")
        self.assertEqual(acceptance.response["phase_reason"], "claiming_tables")
        self.assertEqual(acceptance.materialization.resolved_runtime_name, "kaspr-mat-1")
        self.assertEqual(acceptance.request_event.action, "create")
        self.assertEqual(acceptance.request_event.spec.namespace, "midstream")
        self.assertEqual(acceptance.request_event.spec.runtime_name, "kaspr-mat-1")
        # The names are claimed next, by the directory; a client waits on the claim's outcome.
        self.assertEqual("idr-claim000001", acceptance.request_event.claim_request_id)
        self.assertEqual("idr-claim000001", acceptance.response["claim_request_id"])
        self.assertEqual("/identity/requests/idr-claim000001", acceptance.response["claim_status_url"])
        self.assertIsNone(get_materialization(self.table, "mat-1"))
        self.assertIsNone(get_materialization_status(self.table, "mat-1"))
        self.assertEqual({}, {key: value for key, value in self.directory.values.items() if key.startswith("dataset-table:")})

    def test_create_materialization_requires_its_tables(self) -> None:
        error = self._refused({"name": "Orders"})

        self.assertEqual(
            error_response(error),
            {
                "error": {
                    "code": "invalid_request",
                    "message": "tables is required: choose at least one topic, with the dataset its table goes in",
                    "details": {"field": "tables"},
                }
            },
        )

    def test_each_topic_gets_a_table_in_its_dataset_named_after_it_unless_named(self) -> None:
        acceptance = self._create(
            {
                "tables": [
                    {"source_topic": "orders", "dataset": "shop"},
                    {"source_topic": " Orders.V2 ", "dataset": "shop", "name": "orders_current"},
                    {"source_topic": "payments-events", "dataset": "finance"},
                ],
            }
        )

        expected = [
            {"name": "orders", "source_topic": "orders", "dataset": "shop", "dataset_id": SHOP},
            {"name": "orders_current", "source_topic": "Orders.V2", "dataset": "shop", "dataset_id": SHOP},
            {"name": "payments_events", "source_topic": "payments-events", "dataset": "finance", "dataset_id": FINANCE},
        ]
        self.assertEqual(expected, [table.to_dict() for table in acceptance.materialization.tables])
        self.assertEqual(expected, [table.to_dict() for table in acceptance.request_event.spec.tables])
        self.assertEqual(expected, acceptance.response["tables"])

    def test_topics_that_would_share_a_table_name_are_told_apart_in_order(self) -> None:
        acceptance = self._create({"tables": tables("orders.v2", "orders-v2", "ORDERS_V2")})

        self.assertEqual(("orders_v2", "orders_v2_2", "orders_v2_3"), acceptance.materialization.table_names)

    def test_one_materialization_cannot_name_two_tables_alike_even_in_two_datasets(self) -> None:
        """Its tables are directories of its location, by name."""
        error = self._refused(
            {"tables": [{"source_topic": "orders", "dataset": "shop"}, {"source_topic": "orders-eu", "dataset": "finance", "name": "orders"}]}
        )

        self.assertEqual("tables.1.name", error.details["field"])
        self.assertIn("already has a table named orders", error.message)

    def test_a_topic_named_like_a_reserved_word_gets_a_table_sql_can_name(self) -> None:
        acceptance = self._create({"tables": tables("order", "select", "2026-orders")})

        self.assertEqual(("order_topic", "select_topic", "t_2026_orders"), acceptance.materialization.table_names)

    def test_tables_are_refused_when_they_cannot_be_materialized(self) -> None:
        for value, field, fragment in (
            ([], "tables", "tables is required"),
            ("orders", "tables", "must be a list"),
            (["orders"], "tables.0", "each table is"),
            ([{"dataset": "shop"}], "tables.0.source_topic", "needs the topic it holds"),
            ([{"source_topic": "orders topic", "dataset": "shop"}], "tables.0.source_topic", "is not a Kafka topic name"),
            ([{"source_topic": "x" * 250, "dataset": "shop"}], "tables.0.source_topic", "is not a Kafka topic name"),
            ([{"source_topic": "orders", "dataset": "shop"}, {"source_topic": "orders", "dataset": "finance"}], "tables.1.source_topic", "chosen twice"),
            ([{"source_topic": "orders", "dataset": "shop", "color": "red"}], "tables.0.color", "not something a table has"),
            ([{"source_topic": "orders"}], "tables.0.dataset", "dataset name is required"),
            ([{"source_topic": "orders", "dataset": "Shop"}], "tables.0.dataset", "lowercase only"),
            ([{"source_topic": "orders", "dataset": "main"}], "tables.0.dataset", "DuckDB"),
            ([{"source_topic": "orders", "dataset": "shop", "name": "Orders"}], "tables.0.name", "lowercase only"),
            ([{"source_topic": "orders", "dataset": "shop", "name": "order"}], "tables.0.name", "reserved SQL word"),
            ([{"source_topic": "orders", "dataset": "shop", "name": "2026"}], "tables.0.name", "cannot name a table"),
        ):
            with self.subTest(value=value):
                error = self._refused({"tables": value})
                self.assertEqual("invalid_request", error.code)
                self.assertEqual({"field": field}, error.details)
                self.assertIn(fragment, error.message)

    def test_a_materialization_reads_at_most_the_configured_number_of_topics(self) -> None:
        config = ControlPlaneConfig(default_destination_uri="gs://lake", max_materialization_topics=2)

        error = self._refused({"tables": tables("a", "b", "c")}, config=config)

        self.assertEqual("a materialization reads at most 2 topics; 3 were given", error.message)
        self.assertEqual(error.details, {"field": "tables"})

    def test_the_retired_topic_fields_are_explained(self) -> None:
        for body in ({"source_topic": "orders"}, {"source_topics": ["orders"]}):
            with self.subTest(body=body):
                error = self._refused({**body})
                self.assertIn("source_topics is now tables", error.message)
                self.assertEqual(error.details, {"field": "tables"})

    def test_a_dataset_that_does_not_exist_is_404(self) -> None:
        error = self._refused({"tables": tables("orders", dataset="marketing")})

        self.assertEqual((404, "not_found"), (error.status_code, error.code))
        self.assertEqual("tables.0.dataset", error.details["field"])
        self.assertEqual("dataset_not_found", error.details["reason"])

    def test_putting_a_table_in_a_dataset_needs_write_access_to_it(self) -> None:
        error = self._refused({"tables": [*tables("orders"), *tables("salaries", dataset="secret")]})

        self.assertEqual((403, "forbidden"), (error.status_code, error.code))
        self.assertEqual(("dataset.write", "secret", "tables.1.dataset"), (error.details["permission"], error.details["dataset"], error.details["field"]))

    def test_a_name_another_table_has_in_the_dataset_is_409(self) -> None:
        claim_name(self.directory, SHOP, "orders")

        error = self._refused({"tables": tables("orders")})

        self.assertEqual((409, "conflict"), (error.status_code, error.code))
        self.assertEqual(("table_name_taken", "tables.0.name", "mat-0"), (error.details["reason"], error.details["field"], error.details["materialization_id"]))
        # The same name in another dataset is another table.
        self.assertEqual(("orders",), self._create({"tables": tables("orders", dataset="finance")}).materialization.table_names)

    def test_two_materializations_may_read_the_same_topic_into_two_names(self) -> None:
        claim_name(self.directory, SHOP, "orders")

        acceptance = self._create({"tables": [{"source_topic": "orders", "dataset": "shop", "name": "orders_again"}]})

        self.assertEqual(("orders_again",), acceptance.materialization.table_names)

    def test_what_a_materialization_does_not_choose_is_refused_naming_the_deployments(self) -> None:
        # Every materialization takes the deployment's destination, namespace, storage and a
        # generated runtime name: a request that names one is refused rather than overruled.
        config = ControlPlaneConfig(default_destination_uri="gs://lake", default_runtime_storage_size="64Mi", default_namespace="midstream")
        for field, value, says in (
            ("destination_uri", "gs://elsewhere/orders", "under the deployment's destination, gs://lake"),
            ("namespace", "team-a", "the deployment's namespace, midstream"),
            ("runtime_name", "orders-runtime", "generated from its id: kaspr-mat-1"),
            ("storage_size", "8Gi", "the deployment's storage, 64Mi"),
        ):
            with self.subTest(field=field):
                error = self._refused({"tables": tables("orders"), field: value}, config=config)
                self.assertEqual(400, error.status_code)
                self.assertEqual({"field": field}, error.details)
                self.assertTrue(error.message.startswith(f"{field} is not chosen per materialization: "), error.message)
                self.assertIn(says, error.message)
        # Null says nothing, as if it were left out.
        accepted = self._create({"tables": tables("orders"), "destination_uri": None, "storage_size": None}, config=config)
        self.assertEqual("gs://lake", accepted.materialization.destination_uri)

    def test_no_materialization_is_created_without_a_destination_for_its_tables(self) -> None:
        for destination in (None, "abfs://lakehouse/orders", "s3://lake/orders", "/data/lake", "gs://"):
            with self.subTest(destination=destination):
                error = self._refused({"tables": tables("orders")}, config=ControlPlaneConfig(default_destination_uri=destination))
                self.assertEqual(409, error.status_code)
                self.assertEqual("destination_not_configured", error.details["reason"])
                self.assertIn("Settings page", error.message)

    def test_create_materialization_bounds_long_runtime_name(self) -> None:
        acceptance = self._create(
            {"tables": tables("fusion-outbound-item-with-an-extraordinarily-long-suffix")}
        )

        self.assertLessEqual(len(acceptance.materialization.resolved_runtime_name), 59)
        self.assertEqual(acceptance.request_event.spec.runtime_name, acceptance.materialization.resolved_runtime_name)

    def test_create_materialization_accepts_runtime_scale(self) -> None:
        acceptance = self._create({"tables": tables("orders"), "replicas": 16})

        self.assertEqual(acceptance.materialization.replicas, 16)
        self.assertEqual(acceptance.request_event.spec.replicas, 16)

    def test_create_materialization_takes_the_deployments_scale_and_place(self) -> None:
        acceptance = self._create({"tables": tables("orders")})

        self.assertEqual(acceptance.materialization.storage_size, "64Mi")
        self.assertEqual(acceptance.materialization.replicas, 1)
        self.assertEqual(acceptance.materialization.namespace, "midstream")
        self.assertEqual(acceptance.materialization.destination_uri, "gs://lake")
        self.assertEqual(acceptance.request_event.spec.storage_size, "64Mi")
        self.assertEqual(acceptance.request_event.spec.namespace, "midstream")

    def test_create_materialization_accepts_skip_tombstones(self) -> None:
        acceptance = self._create({"tables": tables("orders"), "skip_tombstones": True})

        self.assertTrue(acceptance.materialization.skip_tombstones)
        self.assertTrue(acceptance.request_event.spec.skip_tombstones)

    def test_create_materialization_leaves_skip_tombstones_off_by_default(self) -> None:
        acceptance = self._create({"tables": tables("orders")})

        self.assertFalse(acceptance.materialization.skip_tombstones)
        self.assertFalse(acceptance.request_event.spec.skip_tombstones)

    def test_create_materialization_rejects_non_boolean_skip_tombstones(self) -> None:
        error = self._refused({"tables": tables("orders"), "skip_tombstones": "true"})

        self.assertEqual(error.details, {"field": "skip_tombstones"})

    def test_a_create_has_autoscaling_unless_it_asks_for_none(self) -> None:
        self.assertTrue(self._create({"tables": tables("orders")}).request_event.spec.autoscaling)
        acceptance = self._create({"tables": tables("orders"), "autoscaling": False})
        self.assertFalse(acceptance.materialization.autoscaling)
        self.assertFalse(acceptance.request_event.spec.autoscaling)
        self.assertEqual({"field": "autoscaling"}, self._refused({"tables": tables("orders"), "autoscaling": "no"}).details)

    def test_every_create_has_a_live_tail(self) -> None:
        # A 4.1 console sends `true` back, or nothing; neither is part of the request.
        for body in ({"tables": tables("orders")}, {"tables": tables("orders"), "live_tail": True}):
            with self.subTest(body=body):
                acceptance = self._create(body)
                self.assertNotIn("live_tail", acceptance.materialization.to_dict())
                self.assertNotIn("live_tail", acceptance.request_event.spec.to_dict())
                # Still answered, true: a 4.1 console offers Live only when it reads it.
                self.assertIs(True, acceptance.response["live_tail"])

    def test_a_stored_override_of_the_retired_live_tail_default_changes_nothing(self) -> None:
        settings = FakeTable()
        apply_settings_change(
            settings,
            SettingsChangeRequest(
                request_id="set-1",
                scope="control-plane",
                submitted_at=NOW,
                submitted_by="admin",
                values={"default_destination_uri": "gs://chosen-lake"},
            ),
            config=ControlPlaneConfig(),
            # Resolved settings are cached by revision and time, which are unique in a deployment:
            # a time of its own keeps this document from being taken for another test's.
            applied_at="2026-07-12T00:00:01Z",
        )
        # As stored before 4.3: a setting the catalog no longer has is never applied.
        document = get_settings_document(settings, "control-plane")
        save_settings_document(
            settings,
            replace(document, values={**document.values, "live_tail_default": SettingOverride(value=False, updated_at=NOW, updated_by="admin")}),
            updated_at=NOW,
        )

        acceptance = self._create({"tables": tables("orders")}, settings_table=settings)

        self.assertIs(True, acceptance.response["live_tail"])

    def test_a_create_cannot_ask_for_no_live_tail(self) -> None:
        for value in (False, None, "true", 1, {}):
            with self.subTest(value=value):
                error = self._refused({"tables": tables("orders"), "live_tail": value})
                self.assertEqual(("invalid_request", {"field": "live_tail"}), (error.code, error.details))
                self.assertIn("live_tail_enabled", error.message)

    def test_create_materialization_rejects_invalid_replicas(self) -> None:
        error = self._refused({"tables": tables("orders"), "replicas": 0})

        self.assertEqual(error.details, {"field": "replicas"})

    def test_a_materialization_without_a_name_is_labelled_with_its_id(self) -> None:
        for missing in (None, "", "   "):
            with self.subTest(name=missing):
                acceptance = self._create({"name": missing, "tables": tables("orders")})
                self.assertEqual("mat-1", acceptance.materialization.name)
                self.assertEqual("mat-1", acceptance.response["name"])

    def test_a_name_is_a_label_of_free_text(self) -> None:
        acceptance = self._create({"name": "  Orders —  EU, 2026 ", "tables": tables("orders")})

        self.assertEqual("Orders — EU, 2026", acceptance.materialization.name)
        self.assertEqual("Orders — EU, 2026", acceptance.request_event.spec.name)

    def test_a_name_is_not_unique(self) -> None:
        save_materialization(self.table, replace(build_materialization("mat-0"), name="Orders"))

        acceptance = self._create({"name": "Orders", "tables": tables("orders", dataset="finance")})

        self.assertEqual("Orders", acceptance.materialization.name)

    def test_what_a_name_cannot_be(self) -> None:
        for name in ("x" * 129, "line\nbreak", 7):
            with self.subTest(name=name):
                error = self._refused({"name": name, "tables": tables("orders")})
                self.assertEqual(("invalid_request", {"field": "name"}), (error.code, error.details))

    def test_create_materialization_writes_to_the_default_destination_when_none_is_given(self) -> None:
        config = ControlPlaneConfig(default_destination_uri="gs://kafka-materializations", default_namespace="midstream")

        acceptance = self._create({"tables": tables("orders")}, config=config)

        self.assertEqual(acceptance.materialization.destination_uri, "gs://kafka-materializations")
        self.assertEqual(acceptance.request_event.spec.destination_uri, "gs://kafka-materializations")

    def test_create_materialization_takes_defaults_and_limits_from_settings_overrides(self) -> None:
        settings = FakeTable()
        apply_settings_change(
            settings,
            SettingsChangeRequest(
                request_id="set-1",
                scope="control-plane",
                submitted_at=NOW,
                submitted_by="admin",
                values={
                    "default_destination_uri": "gs://chosen-lake",
                    "default_runtime_replicas": 3,
                    "max_runtime_replicas": 4,
                    "default_runtime_storage_size": "2Gi",
                },
            ),
            config=ControlPlaneConfig(),
        )

        acceptance = self._create({"tables": tables("orders")}, settings_table=settings)

        self.assertEqual(acceptance.request_event.spec.destination_uri, "gs://chosen-lake")
        self.assertEqual(acceptance.request_event.spec.replicas, 3)
        self.assertEqual(acceptance.request_event.spec.storage_size, "2Gi")
        error = self._refused({"tables": tables("payments"), "replicas": 5}, settings_table=settings, materialization_id="mat-2")
        self.assertEqual(error.details["field"], "replicas")

class WebSystemSettingsTests(unittest.TestCase):
    def test_settings_report_the_defaults_a_create_request_falls_back_to(self) -> None:
        settings = settings_view(
            config=ControlPlaneConfig(
                default_namespace="midstream",
                default_destination_uri="gs://kafka-materializations",
                default_runtime_replicas=2,
                max_runtime_replicas=32,
                max_materialization_topics=12,
            )
        )

        self.assertEqual(settings["default_destination_uri"], "gs://kafka-materializations")
        self.assertEqual(settings["default_replicas"], 2)
        self.assertEqual(settings["max_replicas"], 32)
        self.assertEqual(settings["default_namespace"], "midstream")
        self.assertEqual(settings["default_storage_size"], "64Mi")
        self.assertEqual(settings["default_partition_granularity"], "day")
        # A materialization's name is a label; datasets and tables have names SQL reads.
        self.assertEqual(settings["label_max_length"], 128)
        self.assertEqual(settings["dataset_name_max_length"], 63)
        # The console derives table names and caps the topic picker with these.
        self.assertEqual(settings["max_topics"], 12)
        self.assertEqual(settings["table_name_max_length"], 63)
        self.assertIn("select", settings["reserved_names"])
        self.assertIn("iceberg", settings["reserved_names"])
        self.assertIn("iceberg_scan", settings["reserved_names"])
        # A dataset cannot be one DuckDB names itself, or `<dataset>.<table>` would be ambiguous.
        self.assertIn("memory", settings["reserved_namespace_names"])
        self.assertNotIn("memory", settings["reserved_names"], "a table may be called memory")

    def test_settings_report_no_default_destination_when_none_is_configured(self) -> None:
        self.assertIsNone(settings_view(config=ControlPlaneConfig())["default_destination_uri"])

    def test_settings_report_whether_the_deployment_tails(self) -> None:
        settings = settings_view(config=ControlPlaneConfig())
        self.assertIs(True, settings["live_tail_enabled"])
        self.assertIs(False, settings_view(config=ControlPlaneConfig(live_tail_enabled=False))["live_tail_enabled"])
        # Always true, for a 4.1 console's create dialog: every materialization has a live tail.
        self.assertIs(True, settings_view(config=ControlPlaneConfig(live_tail_enabled=False))["live_tail_default"])


    def test_settings_report_the_deployments_autoscaling(self) -> None:
        settings = settings_view(config=ControlPlaneConfig())
        self.assertEqual((True, 16, 30), (settings["autoscaling_enabled"], settings["autoscale_max_replicas"], settings["autoscale_target_minutes"]))
        self.assertIs(False, settings_view(config=ControlPlaneConfig(runtime_autoscaling_enabled=False))["autoscaling_enabled"])

class FakeMetadataClient:
    def __init__(
        self,
        topics: dict[str, TopicMetadata],
        *,
        listings: list[TopicListing] | None = None,
        list_error: Exception | None = None,
    ) -> None:
        self.topics = topics
        self.listings = listings or []
        self.list_error = list_error
        self.list_calls = 0

    def describe_topics(self, topics: list[str]) -> dict[str, TopicMetadata]:
        return {topic: self.topics[topic] for topic in topics if topic in self.topics}

    def consumer_group_offsets(self, group_id: str) -> dict[str, dict[int, int]]:
        return {}

    async def list_topics_async(self) -> list[TopicListing]:
        self.list_calls += 1
        if self.list_error is not None:
            raise self.list_error
        return list(self.listings)


class Clock:
    def __init__(self) -> None:
        self.now = 1_000.0

    def __call__(self) -> float:
        return self.now


class WebTopicListTests(unittest.TestCase):
    def setUp(self) -> None:
        self.config = ControlPlaneConfig()
        self.clock = Clock()
        self.cache = TopicCatalogCache(clock=self.clock)
        self.client = FakeMetadataClient(
            {},
            listings=[
                TopicListing(name="__consumer_offsets", partitions=50, replication_factor=2, internal=True),
                TopicListing(name="orders", partitions=32, replication_factor=2),
            ],
        )

    def _list(self, query_params: dict[str, str] | None = None) -> dict:
        return asyncio.run(
            list_topics_view(query_params, metadata_client=self.client, config=self.config, cache=self.cache)
        )

    def test_list_topics_view_returns_every_topic_with_its_category(self) -> None:
        response = self._list()

        self.assertEqual(response["returned"], 2)
        self.assertEqual(
            response["items"],
            [
                {"topic": "__consumer_offsets", "partitions": 50, "replication_factor": 2, "category": "kafka"},
                {"topic": "orders", "partitions": 32, "replication_factor": 2},
            ],
        )
        self.assertTrue(response["fetched_at"].endswith("Z"))
        self.assertNotIn("stale", response)

    def test_refresh_query_reloads_once_the_previous_load_is_old_enough(self) -> None:
        self._list()
        self._list({"refresh": "true"})
        self.assertEqual(self.client.list_calls, 1)

        self.clock.now += self.config.topic_catalog_min_refresh_seconds
        self._list({"refresh": "true"})
        self.assertEqual(self.client.list_calls, 2)

    def test_failed_reload_returns_the_previous_catalog_marked_stale(self) -> None:
        self._list()
        self.clock.now += self.config.topic_catalog_ttl_seconds
        self.client.list_error = RuntimeError("broker unreachable")

        response = self._list()

        self.assertEqual(response["returned"], 2)
        self.assertIs(response["stale"], True)
        self.assertEqual(response["error"], "broker unreachable")

    def test_unreachable_kafka_with_nothing_cached_is_a_503(self) -> None:
        self.client.list_error = RuntimeError("broker unreachable")

        with self.assertRaises(WebError) as error_info:
            self._list()

        self.assertEqual(error_info.exception.status_code, 503)
        self.assertEqual(
            error_response(error_info.exception),
            {
                "error": {
                    "code": "unavailable",
                    "message": "Kafka topics cannot be listed right now",
                    "details": {"reason": "broker unreachable"},
                }
            },
        )


class WebTopicAnalysisTests(unittest.TestCase):
    def setUp(self) -> None:
        self.config = ControlPlaneConfig()
        self.metadata_client = FakeMetadataClient(
            {
                "orders": TopicMetadata(
                    name="orders",
                    exists=True,
                    partitions=32,
                    beginning_offsets={index: 0 for index in range(32)},
                    end_offsets={index: 100 for index in range(32)},
                )
            }
        )

    def test_analyze_returns_recommendation(self) -> None:
        payload = analyze_topics_view(
            {"topics": ["orders"]},
            metadata_client=self.metadata_client,
            config=self.config,
        )

        item = payload["items"][0]
        self.assertTrue(item["exists"])
        self.assertEqual(item["partitions"], 32)
        self.assertEqual(item["estimated_records"], 3200)
        self.assertEqual(item["recommendation"]["replicas"], 16)
        # A runtime's storage is the deployment's, not a topic's: nothing to recommend.
        self.assertNotIn("storage_size", item["recommendation"])

    def test_analyze_accepts_single_topic_key(self) -> None:
        payload = analyze_topics_view(
            {"topic": "orders"},
            metadata_client=self.metadata_client,
            config=self.config,
        )

        self.assertEqual(payload["requested"], 1)

    def test_analyze_requires_topics(self) -> None:
        with self.assertRaises(WebError) as error_info:
            analyze_topics_view({}, metadata_client=self.metadata_client, config=self.config)

        self.assertEqual(error_info.exception.details, {"field": "topics"})

    def test_analyze_rejects_non_object_body(self) -> None:
        with self.assertRaises(WebError):
            analyze_topics_view(None, metadata_client=self.metadata_client, config=self.config)


class WebListAndDetailTests(unittest.TestCase):
    def setUp(self) -> None:
        self.table = FakeTable()
        materialization = build_materialization("mat-1")
        save_materialization(self.table, materialization)
        save_materialization_status(self.table, build_status("mat-1"))

    def test_list_materializations_view_reuses_storage_listing(self) -> None:
        response = list_materializations_view(self.table, limit="30")

        self.assertEqual(response["returned"], 1)
        self.assertEqual(response["items"][0]["materialization_id"], "mat-1")

    def test_get_materialization_detail_reports_tombstone_handling(self) -> None:
        response = get_materialization_detail(self.table, "mat-1")

        self.assertIs(response["materialization"]["skip_tombstones"], False)

    def test_the_detail_says_how_autoscaling_scaled_the_runtime(self) -> None:
        detail = get_materialization_detail(self.table, "mat-1")
        self.assertIs(True, detail["materialization"]["autoscaling"])
        self.assertEqual({"enabled": True, "replicas": None}, {k: detail["autoscale"][k] for k in ("enabled", "replicas")})
        save_runtime_autoscale(
            self.table,
            RuntimeAutoscale(
                materialization_id="mat-1",
                replicas=12,
                reason="5,000,000 records behind, about 2 h to catch up with 4 members",
                changed_at="2026-07-12T00:05:00Z",
                eta_seconds=1500,
                points=(LagPoint(at="2026-07-12T00:06:00Z", lag=4_800_000),),
            ),
        )
        autoscale = get_materialization_detail(self.table, "mat-1")["autoscale"]
        self.assertEqual(12, autoscale["replicas"])
        self.assertEqual(1, autoscale["own_replicas"])
        self.assertEqual((1500, 4_800_000), (autoscale["eta_seconds"], autoscale["lag"]))
        self.assertIn("records behind", autoscale["reason"])

    def test_the_detail_and_the_list_say_every_materialization_has_a_live_tail(self) -> None:
        # Always true, for a 4.1 console, which offers Live only when it reads it: even for a
        # record a control plane before 4.3 stored with the live tail switched off.
        self.table.values["materialization:mat-1"]["payload"]["live_tail"] = False
        self.assertIs(True, get_materialization_detail(self.table, "mat-1")["materialization"]["live_tail"])
        self.assertIs(True, list_materializations_view(self.table, limit="30")["items"][0]["live_tail"])

    def test_get_materialization_detail_returns_binding_when_present(self) -> None:
        save_runtime_binding(
            self.table,
            RuntimeBinding(
                materialization_id="mat-1",
                runtime_mode="dedicated",
                kaspr_app_name="kaspr-mat-1",
                namespace="midstream",
                managed_agent_names=["writer", "committer", "settings"],
                template_version="v1",
                last_applied_spec_hash="abc123",
                last_apply_operation_id="op-1",
                updated_at="2026-07-12T00:02:00Z",
            ),
        )

        response = get_materialization_detail(self.table, "mat-1")

        self.assertEqual(response["materialization"]["source_topics"], ["orders"])
        self.assertEqual(
            response["materialization"]["tables"],
            [{"name": "orders", "source_topic": "orders", "dataset": "shop", "dataset_id": "ds-000000000001"}],
        )
        self.assertEqual(response["materialization"]["datasets"], ["shop"])
        self.assertEqual(response["status"]["phase_reason"], "runtime_ready")
        self.assertEqual(
            response["status"]["phase_message"],
            "Healthy runtime status aligns with ready resource observation.",
        )
        self.assertEqual(
            response["status"]["runtime_progress"],
            {
                "rows_written": 1200,
                "bytes_written": 1048576,
                "files_written": 3,
                "records_skipped": 4,
                "latest_offsets": {"orders": {"0": 1024}},
            },
        )
        self.assertEqual(response["runtime_binding"]["kaspr_app_name"], "kaspr-mat-1")
        self.assertEqual(response["runtime_binding"]["last_applied_spec_hash"], "abc123")
        self.assertEqual(response["materialization"]["created_by"], "alice")
        self.assertEqual(response["materialization"]["desired_phase"], "running")
        self.assertEqual(response["status"]["observation_source"], "hybrid")
        self.assertEqual(
            response["status"]["resource_observation"]["kaspr_app_name"],
            "kaspr-mat-1",
        )

    def test_get_materialization_schema_view_projects_fields_and_forks(self) -> None:
        projection = build_projection(0, {"order_id": "O-1", "total": 5, "customer": {"email": "a@example.com"}}, {"total": "five"})
        save_materialization_schema(self.table, projection)

        response = get_materialization_schema_view(self.table, "mat-1", "orders")

        self.assertEqual(response["schema_id"], 0)
        self.assertEqual(response["since"], "2026-07-12T00:00:05Z")
        self.assertEqual((response["table"], response["source_topic"]), ("orders", "orders"))
        by_name: dict[str, list[dict]] = {}
        for field in response["fields"]:
            by_name.setdefault(field["name"], []).append(field)
        self.assertEqual(by_name["customer.email"][0]["type"], "string")
        self.assertEqual(by_name["customer.email"][0]["path"], ["customer", "email"])
        self.assertEqual({field["type"] for field in by_name["total"]}, {"long", "string"})
        self.assertEqual(sorted(field["fork_version"] for field in by_name["total"]), [1, 2])
        self.assertEqual(response["field_count"], projection.field_count)
        self.assertNotIn("iceberg_schema", response)
        self.assertEqual(response["unmapped"], {"total_values": 0, "last_seen_at": None})

    def test_the_raw_schema_view_carries_the_iceberg_schema_and_fork_map(self) -> None:
        save_materialization_schema(self.table, build_projection(0, {"order_id": "O-1"}))

        response = get_materialization_schema_view(self.table, "mat-1", "orders", raw="true")

        self.assertEqual(response["iceberg_schema"]["type"], "struct")
        self.assertIn("fields", response["fork_map"])
        # What the tail app conforms rows with: the rejected paths and the last column id too.
        self.assertEqual([], response["rejected_paths"])
        self.assertIn("last_column_id", response)
        save_materialization_schema(self.table, replace(build_projection(1, {"order_id": "O-1"}), rejected_paths=[["extra", "deep"]], last_column_id=40))
        response = get_materialization_schema_view(self.table, "mat-1", "orders", raw="true")
        self.assertEqual(([["extra", "deep"]], 40), (response["rejected_paths"], response["last_column_id"]))
        self.assertNotIn("rejected_paths", get_materialization_schema_view(self.table, "mat-1", "orders"))
        with self.assertRaises(WebError):
            get_materialization_schema_view(self.table, "mat-1", "orders", raw="perhaps")

    def test_the_schema_view_says_how_many_values_went_to_unmapped(self) -> None:
        save_materialization_schema(self.table, build_projection(0))
        status = build_status("mat-1")
        save_materialization_status(
            self.table,
            replace(
                status,
                runtime_progress=replace(
                    status.runtime_progress,
                    cumulative_unmapped_values=15,
                    cumulative_unmapped_by_table={"orders": 12, "payments": 3},
                    unmapped_last_seen_by_table={"orders": "2026-07-12T00:05:00Z", "payments": "2026-07-12T00:06:00Z"},
                ),
            ),
        )

        response = get_materialization_schema_view(self.table, "mat-1", "orders")

        self.assertEqual(response["unmapped"], {"total_values": 12, "last_seen_at": "2026-07-12T00:05:00Z"})

    def test_get_materialization_schema_view_returns_empty_shape_before_discovery(self) -> None:
        response = get_materialization_schema_view(self.table, "mat-1", "orders")

        self.assertIsNone(response["schema_id"])
        self.assertEqual(response["fields"], [])

    def test_each_table_has_a_schema_of_its_own(self) -> None:
        save_materialization(self.table, build_materialization("mat-1", "orders", "payments"))
        save_materialization_schema(self.table, build_projection(0, {"order_id": "O-1"}))
        save_materialization_schema(self.table, build_projection(0, {"payment_id": "P-1"}, table="payments"))
        save_materialization_schema(self.table, build_projection(1, {"payment_id": "P-1", "amount": 1.5}, table="payments"))

        orders = get_materialization_schema_view(self.table, "mat-1", "orders")
        payments = get_materialization_schema_view(self.table, "mat-1", "PAYMENTS")

        self.assertEqual((orders["schema_id"], payments["schema_id"]), (0, 1))
        self.assertEqual(("payments", "payments"), (payments["table"], payments["source_topic"]))
        self.assertIn("order_id", [field["name"] for field in orders["fields"]])
        self.assertNotIn("order_id", [field["name"] for field in payments["fields"]])
        versions = list_materialization_schema_versions_view(self.table, "mat-1", "payments")
        self.assertEqual(([1, 0], "payments"), ([item["schema_id"] for item in versions["versions"]], versions["table"]))
        self.assertEqual([0], [item["schema_id"] for item in list_materialization_schema_versions_view(self.table, "mat-1", "orders")["versions"]])
        self.assertEqual(0, get_materialization_schema_version_view(self.table, "mat-1", "payments", 0)["schema_id"])

    def test_a_table_the_materialization_does_not_have_is_not_found(self) -> None:
        for view in (
            lambda: get_materialization_schema_view(self.table, "mat-1", "payments"),
            lambda: list_materialization_schema_versions_view(self.table, "mat-1", "payments"),
            lambda: get_materialization_schema_version_view(self.table, "mat-1", "payments", 0),
            lambda: get_materialization_table_view(self.table, "mat-1", "payments"),
        ):
            with self.assertRaises(WebError) as error_info:
                view()
            self.assertEqual(error_info.exception.status_code, 404)
            self.assertIn("its tables are orders", error_info.exception.message)

    def test_get_materialization_schema_view_raises_not_found_for_unknown_id(self) -> None:
        with self.assertRaises(WebError) as error_info:
            get_materialization_schema_view(self.table, "missing", "orders")

        self.assertEqual(error_info.exception.code, "not_found")

    def test_list_schema_versions_returns_newest_first_with_current_flag(self) -> None:
        for schema_id in (0, 1, 2):
            save_materialization_schema(self.table, build_projection(schema_id, {"order_id": "O", f"f{schema_id}": 1}))

        response = list_materialization_schema_versions_view(self.table, "mat-1", "orders")

        self.assertEqual(response["current_schema_id"], 2)
        self.assertEqual([item["schema_id"] for item in response["versions"]], [2, 1, 0])
        self.assertEqual(response["returned"], 3)
        self.assertTrue(response["versions"][0]["current"])
        self.assertFalse(response["versions"][1]["current"])
        self.assertEqual(response["versions"][0]["change_counts"], {"added": 2})
        self.assertEqual(response["versions"][0]["since"], "2026-07-12T00:02:05Z")

    def test_list_schema_versions_honors_limit(self) -> None:
        for schema_id in (0, 1, 2):
            save_materialization_schema(self.table, build_projection(schema_id))

        response = list_materialization_schema_versions_view(self.table, "mat-1", "orders", limit=2)

        self.assertEqual([item["schema_id"] for item in response["versions"]], [2, 1])

    def test_list_schema_versions_is_empty_before_discovery(self) -> None:
        response = list_materialization_schema_versions_view(self.table, "mat-1", "orders")

        self.assertIsNone(response["current_schema_id"])
        self.assertEqual(response["versions"], [])

    def test_get_schema_version_returns_the_archived_projection(self) -> None:
        save_materialization_schema(self.table, build_projection(0, {"f0": "x"}))
        save_materialization_schema(self.table, build_projection(1, {"f1": "x"}))

        response = get_materialization_schema_version_view(self.table, "mat-1", "orders", "0")

        self.assertEqual(response["schema_id"], 0)
        self.assertEqual(response["source_topic"], "orders")
        self.assertIn("f0", [field["name"] for field in response["fields"]])
        self.assertNotIn("unmapped", response)

    def test_get_schema_version_rejects_a_non_numeric_id(self) -> None:
        with self.assertRaises(WebError) as error_info:
            get_materialization_schema_version_view(self.table, "mat-1", "orders", "latest")

        self.assertEqual(error_info.exception.code, "invalid_request")
        self.assertEqual(error_info.exception.details["field"], "schema_id")

    def test_get_schema_version_raises_not_found_for_unarchived_version(self) -> None:
        save_materialization_schema(self.table, build_projection(0))

        with self.assertRaises(WebError) as error_info:
            get_materialization_schema_version_view(self.table, "mat-1", "orders", 9)

        self.assertEqual(error_info.exception.code, "not_found")
        self.assertEqual(error_info.exception.status_code, 404)

    def test_schema_version_endpoints_raise_not_found_for_unknown_materialization(self) -> None:
        with self.assertRaises(WebError):
            list_materialization_schema_versions_view(self.table, "missing", "orders")
        with self.assertRaises(WebError):
            get_materialization_schema_version_view(self.table, "missing", "orders", 1)

    def test_the_table_view_reports_the_record_and_what_the_committer_last_said(self) -> None:
        record = build_table_record(build_materialization("mat-1"), current_snapshot_id=42, totals={"records": 10, "files": 2, "bytes": 4096})
        record = record.with_maintenance(
            MaintenanceRun(job="compaction", completed_at="2026-07-12T01:00:00Z", counts={"inputs": 8}),
            updated_at="2026-07-12T01:00:00Z",
        )
        save_materialization_table(self.table, record)
        status = build_status("mat-1")
        save_materialization_status(
            self.table,
            replace(
                status,
                runtime_progress=replace(
                    status.runtime_progress,
                    tables={"orders": {"pending_files": 3, "small_files": 7}, "payments": {"pending_files": 1}},
                ),
            ),
        )

        response = get_materialization_table_view(self.table, "mat-1", "orders")

        self.assertEqual(response["identifier"], "shop.orders")
        self.assertEqual(response["source_topic"], "orders")
        self.assertEqual(response["location"], record.location)
        self.assertEqual(response["current_snapshot_id"], 42)
        self.assertEqual(response["totals"], {"records": 10, "files": 2, "bytes": 4096})
        self.assertEqual(response["maintenance"][0]["job"], "compaction")
        self.assertEqual(response["live"], {"pending_files": 3, "small_files": 7})
        self.assertEqual(response["partition_granularity"], "day")

    def test_the_table_view_is_not_found_before_the_table_exists(self) -> None:
        with self.assertRaises(WebError) as error_info:
            get_materialization_table_view(self.table, "mat-1", "orders")

        self.assertEqual(error_info.exception.status_code, 404)

    def test_the_detail_summarizes_each_table_in_topic_order(self) -> None:
        materialization = build_materialization("mat-1", "payments", "orders")
        save_materialization(self.table, materialization)
        save_materialization_table(self.table, build_table_record(materialization, table="orders", schema_id=3))
        save_materialization_table(self.table, build_table_record(materialization, table="payments"))

        response = get_materialization_detail(self.table, "mat-1")

        self.assertEqual(["shop.payments", "shop.orders"], [table["identifier"] for table in response["tables"]])
        self.assertEqual(["payments", "orders"], [table["source_topic"] for table in response["tables"]])
        self.assertEqual(3, response["tables"][1]["schema_id"])
        self.assertEqual(response["materialization"]["partition_granularity"], "day")

    def test_the_detail_lists_only_the_tables_created_so_far(self) -> None:
        self.assertEqual([], get_materialization_detail(self.table, "mat-1")["tables"])

    def test_a_maintenance_request_is_addressed_to_the_maintenance_app_table_by_table(self) -> None:
        materialization = build_materialization("mat-1", "orders", "payments")
        save_materialization(self.table, materialization)
        for name in ("orders", "payments"):
            save_materialization_table(self.table, build_table_record(materialization, table=name))

        accepted = list(
            request_maintenance(
                self.table,
                "mat-1",
                {"job": "compaction"},
                submitted_by="ops@example.com",
                request_id="mnt-1",
                accepted_at="2026-07-12T02:00:00Z",
                config=ControlPlaneConfig(),
            )
        )

        self.assertEqual(["maintenance:mat-1:orders", "maintenance:mat-1:payments"], [value["key"] for value in accepted])
        response = accepted[-1]["response"]
        self.assertEqual(("compaction", None, ["orders", "payments"]), (response["job"], response["table"], response["tables"]))
        message = MaintenanceMessage.from_dict(accepted[0]["message"])
        self.assertEqual(
            (message.kind, message.target.materialization_id, message.target.table, message.job, message.requested_by),
            ("maintenance_request", "mat-1", "orders", "compaction", "ops@example.com"),
        )
        self.assertTrue(message.target.location.endswith("/orders"))
        # The runtimes' maintenance settings as they are now, so the app needs nothing else.
        self.assertIn("compaction_min_input_files", message.settings)

    def test_a_maintenance_request_may_name_one_table(self) -> None:
        materialization = build_materialization("mat-1", "orders", "payments")
        save_materialization(self.table, materialization)
        save_materialization_table(self.table, build_table_record(materialization, table="orders"))

        def request(table_name):
            return list(
                request_maintenance(
                    self.table, "mat-1", {"job": "compaction", "table": table_name}, submitted_by="ops", config=ControlPlaneConfig()
                )
            )

        (accepted,) = request("ORDERS")
        self.assertEqual(("orders", "orders"), (accepted["message"]["target"]["table"], accepted["response"]["table"]))
        with self.assertRaises(WebError) as error_info:
            request("payments")
        self.assertEqual(error_info.exception.details["reason"], "table_not_created")
        for unknown in ("customers", 7):
            with self.subTest(table=unknown), self.assertRaises(WebError) as error_info:
                request(unknown)
            self.assertEqual((error_info.exception.status_code, error_info.exception.details), (400, {"field": "table"}))

    def test_a_maintenance_request_needs_a_known_job_and_a_table(self) -> None:
        with self.assertRaises(WebError) as error_info:
            request_maintenance(self.table, "mat-1", {"job": "compaction"}, submitted_by="ops", config=ControlPlaneConfig())
        self.assertEqual(error_info.exception.details["reason"], "table_not_created")

        save_materialization_table(self.table, build_table_record(build_materialization("mat-1")))
        for body in ({"job": "vacuum"}, {}, "compaction"):
            with self.subTest(body=body), self.assertRaises(WebError) as error_info:
                request_maintenance(self.table, "mat-1", body, submitted_by="ops", config=ControlPlaneConfig())
            self.assertEqual(error_info.exception.code, "invalid_request")

    def test_a_stopped_materializations_tables_are_still_maintained(self) -> None:
        save_materialization(self.table, replace(build_materialization("mat-1"), desired_phase="stopped"))
        save_materialization_table(self.table, build_table_record(build_materialization("mat-1")))

        (accepted,) = request_maintenance(self.table, "mat-1", {"job": "orphan_cleanup"}, submitted_by="ops", config=ControlPlaneConfig())

        self.assertEqual("orphan_cleanup", accepted["message"]["job"])

        save_materialization(self.table, replace(build_materialization("mat-1"), desired_phase="deleting"))
        with self.assertRaises(WebError) as error_info:
            request_maintenance(self.table, "mat-1", {"job": "orphan_cleanup"}, submitted_by="ops", config=ControlPlaneConfig())
        self.assertEqual(error_info.exception.details["reason"], "materialization_deleting")

    def _three_datasets(self) -> Materialization:
        """mat-1 reading into shop (Jane owns it), finance (she writes it) and secret (she has nothing)."""
        materialization = replace(
            build_materialization("mat-1"),
            tables=(
                table_spec("orders", "orders"),
                table_spec("invoices", "finance.invoices", dataset="finance", dataset_id=FINANCE),
                table_spec("salaries", "hr.salaries", dataset="secret", dataset_id=SECRET),
            ),
        )
        save_materialization(self.table, materialization)
        for spec in materialization.tables:
            save_materialization_table(self.table, build_table_record(materialization, table=spec.name))
            save_materialization_schema(self.table, build_projection(0, {"id": "x"}, table=spec.name))
        return materialization

    def test_the_schema_views_need_the_tables_dataset_view_schema(self) -> None:
        self._three_datasets()
        jane = principal_of(build_directory())
        views = {
            "schema": lambda name: get_materialization_schema_view(self.table, "mat-1", name, principal=jane),
            "versions": lambda name: list_materialization_schema_versions_view(self.table, "mat-1", name, principal=jane),
            "version": lambda name: get_materialization_schema_version_view(self.table, "mat-1", name, 0, principal=jane),
        }
        for label, view in views.items():
            with self.subTest(view=label):
                # She owns shop and writes finance: both let her see their schemas.
                self.assertEqual("shop", view("orders")["dataset"])
                self.assertEqual("finance", view("invoices")["dataset"])
                with self.assertRaises(WebError) as refused:
                    view("salaries")
                self.assertEqual(403, refused.exception.status_code)
                self.assertEqual({"reason": "forbidden", "permission": "dataset.view_schema", "dataset": "secret"}, refused.exception.details)
                self.assertIn("on dataset secret", refused.exception.message)

    def test_the_detail_says_which_tables_the_caller_may_read_and_query(self) -> None:
        self._three_datasets()
        jane = principal_of(build_directory())

        response = get_materialization_detail(self.table, "mat-1", principal=jane)

        self.assertEqual(
            [("shop.orders", True, True), ("finance.invoices", True, True), ("secret.salaries", False, False)],
            [(table["identifier"], table["readable"], table["queryable"]) for table in response["tables"]],
        )
        self.assertEqual(["finance", "secret", "shop"], response["materialization"]["datasets"], "by name")
        self.assertNotIn("readable", get_materialization_detail(self.table, "mat-1")["tables"][0], "only for a caller")
        locked = get_materialization_table_view(self.table, "mat-1", "salaries", principal=jane)
        self.assertEqual((False, False), (locked["readable"], locked["queryable"]))

    def test_the_list_names_tables_by_dataset_and_finds_them_by_any_of_their_names(self) -> None:
        self._three_datasets()
        save_materialization(self.table, replace(build_materialization("mat-2", "payments"), name="Payments feed"))

        listing = list_materializations_view(self.table, limit="30")
        by_id = {item["materialization_id"]: item for item in listing["items"]}
        self.assertEqual(["shop.orders", "finance.invoices", "secret.salaries"], by_id["mat-1"]["tables"])
        self.assertEqual(["finance", "secret", "shop"], by_id["mat-1"]["datasets"], "by name")
        self.assertEqual("Payments feed", by_id["mat-2"]["name"])

        for query, expected in (
            ("payments FEED", ["mat-2"]),
            ("MAT-2", ["mat-2"]),
            ("finance", ["mat-1"]),
            ("secret.salaries", ["mat-1"]),
            ("hr.salaries", ["mat-1"]),
            ("shop", ["mat-1", "mat-2"]),
            ("nothing", []),
        ):
            with self.subTest(query=query):
                found = list_materializations_view(self.table, limit="30", q=query)
                self.assertEqual(expected, sorted(item["materialization_id"] for item in found["items"]))
                self.assertEqual(query.strip().lower(), found["q"])

    def test_get_materialization_detail_raises_not_found_for_unknown_id(self) -> None:
        with self.assertRaises(WebError) as error_info:
            get_materialization_detail(self.table, "missing")

        self.assertEqual(error_info.exception.code, "not_found")
        self.assertEqual(error_info.exception.status_code, 404)


class WebMutationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.table = FakeTable()
        self.directory = build_directory()
        # Deployed: every table exists, so the materialization has a runtime to stop or rescale.
        materialization = replace(build_materialization("mat-1", "orders", "payments"), replicas=4)
        save_materialization(self.table, materialization)
        for name in materialization.table_names:
            save_materialization_table(self.table, build_table_record(materialization, table=name))
            claim_name(self.directory, SHOP, name, "mat-1")
        save_materialization_status(self.table, build_status("mat-1"))
        self.config = ControlPlaneConfig()

    def _save(self, **changes: object) -> None:
        save_materialization(self.table, replace(get_materialization(self.table, "mat-1"), **changes))

    def _undeploy(self) -> None:
        """As before the create finished: the second table is not created yet."""
        del self.table.values["materialization-table:mat-1:payments"]

    def _stop(self):
        return stop_materialization(
            self.table,
            "mat-1",
            submitted_by="alice",
            request_id="req-stop",
            accepted_at="2026-07-12T00:03:00Z",
            config=self.config,
        )

    def _update(self, body: object, *, principal: Principal | None = None):
        return update_materialization(
            self.table,
            "mat-1",
            body,
            submitted_by="jane@example.com",
            submitted_by_id=JANE,
            principal=principal or principal_of(self.directory),
            directory_table=self.directory,
            request_id="req-update",
            claim_request_id="idr-claim000002",
            accepted_at="2026-07-12T00:06:00Z",
            config=self.config,
        )

    def _refused(self, body: object, **kwargs) -> WebError:
        with self.assertRaises(WebError) as error_info:
            self._update(body, **kwargs)
        return error_info.exception

    def _delete(self):
        return delete_materialization(
            self.table,
            "mat-1",
            submitted_by="jane@example.com",
            submitted_by_id=JANE,
            principal=principal_of(self.directory),
            directory_table=self.directory,
            request_id="req-delete",
            accepted_at="2026-07-12T00:05:00Z",
            config=self.config,
        )

    def _in_secret(self, name: str) -> None:
        """Move one of the tables to the dataset Jane may not write to."""
        materialization = get_materialization(self.table, "mat-1")
        spec = replace(materialization.table(name), dataset="secret", dataset_id=SECRET)
        self._save(tables=tuple(spec if table.name == name else table for table in materialization.tables))

    def test_stop_materialization_requests_the_stopped_phase_without_persisting(self) -> None:
        acceptance = self._stop()

        self.assertEqual(acceptance.materialization.desired_phase, "stopped")
        self.assertEqual(acceptance.status.desired_phase, "stopped")
        self.assertEqual(acceptance.status.observed_phase, "running")
        self.assertEqual(acceptance.request_event.action, "stop")
        self.assertEqual(acceptance.response["desired_phase"], "stopped")
        # The configured scale travels unchanged, so a resume can restore it.
        self.assertEqual(acceptance.request_event.spec.replicas, 4)
        self.assertEqual(get_materialization(self.table, "mat-1").desired_phase, "running")

    def test_stop_materialization_refuses_one_with_no_runtime_yet(self) -> None:
        self._undeploy()

        with self.assertRaises(WebError) as error_info:
            self._stop()

        self.assertEqual(error_info.exception.code, "conflict")
        self.assertEqual(error_info.exception.status_code, 409)
        self.assertEqual(error_info.exception.details, {"reason": "runtime_not_deployed"})

    def test_stop_and_resume_refuse_a_materialization_being_deleted(self) -> None:
        self._save(desired_phase="deleting")

        for action in (self._stop, lambda: resume_materialization(self.table, "mat-1", submitted_by="ops", config=self.config)):
            with self.subTest(action=action):
                with self.assertRaises(WebError) as error_info:
                    action()
                self.assertEqual(error_info.exception.code, "conflict")
                self.assertEqual(error_info.exception.details, {"reason": "materialization_deleting"})

    def test_lifecycle_requests_raise_not_found_for_unknown_id(self) -> None:
        for action in (stop_materialization, resume_materialization, delete_materialization):
            with self.subTest(action=action.__name__):
                with self.assertRaises(WebError) as error_info:
                    action(self.table, "missing", submitted_by="ops", config=self.config)
                self.assertEqual(error_info.exception.status_code, 404)

    def test_resume_materialization_updates_desired_phase(self) -> None:
        self._save(desired_phase="stopped")

        acceptance = resume_materialization(
            self.table,
            "mat-1",
            submitted_by="alice",
            request_id="req-resume",
            accepted_at="2026-07-12T00:04:00Z",
            config=self.config,
        )

        self.assertEqual(acceptance.materialization.desired_phase, "running")
        self.assertEqual(acceptance.request_event.action, "resume")
        self.assertEqual(acceptance.request_event.spec.replicas, 4)
        self.assertEqual(get_materialization(self.table, "mat-1").desired_phase, "stopped")

    def test_update_materialization_changes_the_replica_count(self) -> None:
        acceptance = self._update({"replicas": 8})

        self.assertEqual(acceptance.request_event.action, "update")
        self.assertEqual(acceptance.request_event.spec.replicas, 8)
        self.assertEqual(acceptance.materialization.replicas, 8)
        self.assertEqual(acceptance.materialization.spec_version, 2)
        self.assertEqual(acceptance.materialization.desired_phase, "running")
        self.assertEqual(acceptance.response["desired_phase"], "running")
        self.assertIsNone(acceptance.request_event.label)
        # Accepted, not applied: request intake persists the change.
        self.assertEqual(get_materialization(self.table, "mat-1").replicas, 4)

    def test_update_materialization_keeps_a_stopped_materialization_stopped(self) -> None:
        self._save(desired_phase="stopped")

        acceptance = self._update({"replicas": 2})

        self.assertEqual(acceptance.materialization.desired_phase, "stopped")
        self.assertEqual(acceptance.request_event.spec.replicas, 2)

    def test_a_materialization_can_be_relabelled_at_any_time(self) -> None:
        self._undeploy()

        acceptance = self._update({"name": "  Shop — orders "})

        self.assertEqual("Shop — orders", acceptance.request_event.label)
        self.assertEqual("Shop — orders", acceptance.materialization.name)
        self.assertEqual("Shop — orders", acceptance.response["name"])
        # The runtime knows nothing of the label: the spec does not change.
        self.assertEqual(1, acceptance.materialization.spec_version)
        self.assertIsNone(acceptance.request_event.topic_change)

    def test_an_update_may_turn_autoscaling_off_and_on_without_a_new_spec(self) -> None:
        acceptance = self._update({"autoscaling": False})
        self.assertFalse(acceptance.request_event.spec.autoscaling)
        self.assertFalse(acceptance.materialization.autoscaling)
        self.assertEqual(1, acceptance.materialization.spec_version, "not part of the runtime's spec")
        self.assertEqual({"field": "autoscaling"}, self._refused({"autoscaling": "off"}).details)

    def test_an_update_may_send_live_tail_true_back_and_it_changes_nothing(self) -> None:
        # A 4.1 console sends back what it read: `live_tail: true`, every materialization's.
        acceptance = self._update({"live_tail": True, "replicas": 6})

        self.assertEqual(6, acceptance.request_event.spec.replicas)
        self.assertNotIn("live_tail", acceptance.request_event.to_dict())
        self.assertNotIn("live_tail", acceptance.request_event.spec.to_dict())
        self.assertIs(True, acceptance.response["live_tail"])
        # On its own it says nothing to change.
        self.assertEqual({"field": "replicas"}, self._refused({"live_tail": True}).details)

    def test_an_update_cannot_turn_the_live_tail_off(self) -> None:
        for body in ({"live_tail": False}, {"live_tail": False, "replicas": 6}, {"live_tail": "false"}, {"live_tail": None}):
            with self.subTest(body=body):
                error = self._refused(body)
                self.assertEqual({"field": "live_tail"}, error.details)
                self.assertIn("live_tail_enabled", error.message)
        self._save(desired_phase="stopped")
        self.assertEqual({"field": "live_tail"}, self._refused({"live_tail": False}).details)

    def test_an_empty_label_falls_back_to_the_id(self) -> None:
        self.assertEqual("mat-1", self._update({"name": ""}).request_event.label)
        error = self._refused({"name": "x" * 129})
        self.assertEqual(error.details, {"field": "name"})

    def test_update_materialization_accepts_settings_repeated_at_their_current_values(self) -> None:
        current = get_materialization_detail(self.table, "mat-1")["materialization"]
        acceptance = self._update(
            {
                "replicas": 6,
                "name": current["name"],
                "source_topics": current["source_topics"],
                "tables": current["tables"],
                "retired_tables": current["retired_tables"],
                "storage_size": current["storage_size"],
                "destination_uri": current["destination_uri"],
            }
        )

        self.assertEqual(acceptance.request_event.spec.replicas, 6)
        self.assertIsNone(acceptance.request_event.label)
        self.assertIsNone(acceptance.request_event.topic_change)
        self.assertEqual(("orders", "payments"), tuple(table.name for table in acceptance.request_event.spec.tables))

    def test_update_materialization_refuses_other_creation_settings(self) -> None:
        for field_name, value, fragment in (
            ("source_topics", ["orders"], "change the topics with tables"),
            ("destination_uri", "gs://elsewhere", "cannot be changed"),
            ("storage_size", "8Gi", "cannot be changed"),
            ("skip_tombstones", True, "cannot be changed"),
        ):
            with self.subTest(field=field_name):
                error = self._refused({"replicas": 6, field_name: value})
                self.assertEqual(error.details, {"field": field_name})
                self.assertIn(fragment, error.message)

    def test_update_materialization_adds_a_topic_in_a_dataset_of_its_choosing(self) -> None:
        acceptance = self._update(
            {
                "tables": [
                    {"source_topic": "orders"},
                    {"source_topic": "payments", "dataset": "shop", "name": "payments"},
                    {"source_topic": "mtm.shipments.v1", "dataset": "finance", "name": "shipments"},
                ]
            }
        )

        change = acceptance.request_event.topic_change
        self.assertEqual(change.source_topics, ("orders", "payments", "mtm.shipments.v1"))
        self.assertEqual(change.table_actions, {})
        shipments = {"name": "shipments", "source_topic": "mtm.shipments.v1", "dataset": "finance", "dataset_id": FINANCE}
        self.assertEqual([shipments], [spec.to_dict() for spec in change.new_tables])
        self.assertEqual(["orders", "payments", "shipments"], [table.name for table in acceptance.materialization.tables])
        self.assertEqual(acceptance.materialization.spec_version, 2)
        self.assertEqual(acceptance.response["topic_change"]["added"], [shipments])
        # The new table's name is claimed before the change applies; a client may wait on it.
        self.assertEqual(("idr-claim000002", "idr-claim000002"), (acceptance.request_event.claim_request_id, acceptance.response["claim_request_id"]))
        # Accepted, not applied: request intake applies it, planning it again.
        self.assertEqual(get_materialization(self.table, "mat-1").table_names, ("orders", "payments"))

    def test_a_new_topics_table_name_defaults_to_one_derived_from_it(self) -> None:
        acceptance = self._update({"tables": [*tables("orders", "payments"), *tables("mtm.shipments.v1", dataset="finance")]})

        self.assertEqual(["mtm_shipments_v1"], [spec.name for spec in acceptance.request_event.topic_change.new_tables])

    def test_adding_a_topic_needs_write_access_to_its_dataset_and_a_free_name(self) -> None:
        refused = self._refused({"tables": [*tables("orders", "payments"), *tables("salaries", dataset="secret")]})
        self.assertEqual((403, "secret"), (refused.status_code, refused.details["dataset"]))

        claim_name(self.directory, FINANCE, "shipments", "mat-9")
        taken = self._refused({"tables": [*tables("orders", "payments"), {"source_topic": "s", "dataset": "finance", "name": "shipments"}]})
        self.assertEqual((409, "table_name_taken"), (taken.status_code, taken.details["reason"]))

    def test_a_tables_dataset_and_name_never_change(self) -> None:
        error = self._refused({"tables": [{"source_topic": "orders", "dataset": "finance"}, *tables("payments")]})

        self.assertIn("never change", error.message)
        self.assertEqual("tables.0", error.details["field"])

    def test_only_one_change_of_topics_waits_for_its_names_at_a_time(self) -> None:
        pending = self._update({"tables": [*tables("orders", "payments"), *tables("refunds", dataset="finance")]})
        self._save(
            pending_topic_change=PendingTopicChange(
                request_id="req-update",
                claim_request_id="idr-claim000002",
                submitted_at="2026-07-12T00:06:00Z",
                plan=TopicChangePlan(
                    tables=pending.materialization.tables,
                    retired_tables=(),
                    added=pending.request_event.topic_change.new_tables,
                    resumed=(),
                    removed=(),
                    retired=(),
                    dropped=(),
                ),
            )
        )

        error = self._refused({"tables": tables("orders"), "removed_tables": {"payments": "drop"}})
        self.assertEqual((409, "topic_change_pending"), (error.status_code, error.details["reason"]))
        # Other settings still change meanwhile.
        self.assertEqual(3, self._update({"replicas": 3}).request_event.spec.replicas)

    def test_update_materialization_removes_a_topic_keeping_or_dropping_its_table(self) -> None:
        for action, kept, dropped in (("keep", ["payments"], []), ("drop", [], ["payments"])):
            with self.subTest(action=action):
                acceptance = self._update({"tables": tables("orders"), "removed_tables": {"payments": action}})

                self.assertEqual(acceptance.request_event.topic_change.table_actions, {"payments": action})
                self.assertEqual(acceptance.materialization.table_names, ("orders",))
                self.assertEqual([table.name for table in acceptance.materialization.retired_tables], kept)
                self.assertIsNone(acceptance.request_event.claim_request_id, "removing takes no name")
                described = acceptance.response["topic_change"]
                self.assertEqual([table["name"] for table in described["kept"]], kept)
                self.assertEqual([table["name"] for table in described["dropped"]], dropped)

    def test_removing_a_topic_needs_write_access_to_its_tables_dataset(self) -> None:
        self._in_secret("payments")

        for action in ("keep", "drop"):
            with self.subTest(action=action):
                error = self._refused({"tables": tables("orders"), "removed_tables": {"payments": action}})
                self.assertEqual((403, "dataset.write", "secret"), (error.status_code, error.details["permission"], error.details["dataset"]))

    def test_update_materialization_asks_what_becomes_of_a_removed_topics_table(self) -> None:
        error = self._refused({"tables": tables("orders")})

        self.assertEqual(error.code, "invalid_request")
        self.assertEqual(error.details, {"field": "removed_tables"})
        self.assertIn("payments", error.message)

    def test_update_materialization_refuses_malformed_topic_changes(self) -> None:
        for body, field_name in (
            ({"tables": "orders"}, "tables"),
            ({"tables": [{"source_topic": "bad topic", "dataset": "shop"}]}, "tables.0.source_topic"),
            ({"tables": [1, 2]}, "tables.0"),
            ({"tables": [*tables("orders"), {"source_topic": "new"}]}, "tables.1.dataset"),
            ({"tables": tables("orders"), "removed_tables": ["payments"]}, "removed_tables"),
            ({"tables": tables("orders"), "removed_tables": {"payments": "archive"}}, "removed_tables"),
            ({"tables": tables("orders", "payments"), "removed_tables": {"orders": "drop"}}, "removed_tables"),
            ({"removed_tables": {"customers": "drop"}}, "removed_tables"),
        ):
            with self.subTest(body=body):
                error = self._refused(body)
                self.assertEqual(error.code, "invalid_request")
                self.assertEqual(error.details, {"field": field_name})

    def test_update_materialization_drops_a_kept_table(self) -> None:
        self._keep("payments")

        acceptance = self._update({"removed_tables": {"payments": "drop"}})

        self.assertEqual(acceptance.request_event.topic_change.source_topics, ("orders",))
        self.assertEqual(acceptance.materialization.retired_tables, ())
        self.assertEqual([table["name"] for table in acceptance.response["topic_change"]["dropped"]], ["payments"])

    def test_update_materialization_repeating_the_topics_changes_none(self) -> None:
        acceptance = self._update({"replicas": 5, "tables": tables("orders", "payments")})

        self.assertIsNone(acceptance.request_event.topic_change)
        self.assertNotIn("topic_change", acceptance.response)
        self.assertEqual(acceptance.materialization.spec_version, 2)

    def test_update_materialization_changes_replicas_and_topics_in_one_version(self) -> None:
        acceptance = self._update({"replicas": 2, "tables": tables("orders", "payments", "refunds")})

        self.assertEqual(acceptance.materialization.replicas, 2)
        self.assertEqual(acceptance.materialization.table_names, ("orders", "payments", "refunds"))
        self.assertEqual(acceptance.materialization.spec_version, 2)

    def test_detail_and_table_views_show_kept_tables(self) -> None:
        self._keep("payments")

        detail = get_materialization_detail(self.table, "mat-1")

        self.assertEqual(
            detail["materialization"]["retired_tables"],
            [{"name": "payments", "source_topic": "payments", "dataset": "shop", "dataset_id": SHOP}],
        )
        self.assertEqual([(table["name"], table["state"]) for table in detail["tables"]], [("orders", "active"), ("payments", "retired")])
        self.assertEqual(get_materialization_table_view(self.table, "mat-1", "payments")["state"], "retired")
        # The maintenance app keeps a kept table healthy too.
        (accepted,) = request_maintenance(
            self.table,
            "mat-1",
            {"job": "compaction", "table": "payments"},
            submitted_by="alice",
            config=self.config,
        )
        self.assertEqual(("payments", "retired"), (accepted["message"]["target"]["table"], accepted["message"]["target"]["state"]))
        self.assertEqual("shop", accepted["message"]["target"]["namespace"])

    def _keep(self, name: str) -> None:
        """As after an update kept `name` read-only when its topic was removed."""
        materialization = get_materialization(self.table, "mat-1")
        spec = materialization.table(name)
        self._save(
            tables=tuple(table for table in materialization.tables if table.name != name),
            retired_tables=(spec,),
        )
        key = f"materialization-table:mat-1:{name}"
        record = build_table_record(materialization, table=name, state="retired")
        save_materialization_table(self.table, record)
        self.assertIn(key, self.table.values)

    def test_update_materialization_refuses_unknown_settings(self) -> None:
        error = self._refused({"replicas": 6, "desired_phase": "stopped"})

        self.assertEqual(error.details, {"field": "desired_phase"})

    def test_update_materialization_validates_replicas(self) -> None:
        for body in ({}, {"replicas": None}, {"replicas": ""}, {"replicas": 0}, {"replicas": 65}, {"replicas": "many"}, {"replicas": True}):
            with self.subTest(body=body):
                error = self._refused(body)
                self.assertEqual(error.code, "invalid_request")
                self.assertEqual(error.details, {"field": "replicas"})

    def test_update_materialization_refuses_a_non_object_body(self) -> None:
        error = self._refused(["replicas", 2])

        self.assertEqual(error.code, "invalid_request")

    def test_update_materialization_refuses_one_with_no_runtime_or_being_deleted(self) -> None:
        self._save(desired_phase="deleting")
        error = self._refused({"replicas": 2})
        self.assertEqual((error.code, error.details), ("conflict", {"reason": "materialization_deleting"}))

        self._save(desired_phase="running")
        self._undeploy()
        error = self._refused({"replicas": 2})
        self.assertEqual((error.code, error.details), ("conflict", {"reason": "runtime_not_deployed"}))

    def test_delete_materialization_updates_desired_phase(self) -> None:
        acceptance = self._delete()

        self.assertEqual(acceptance.materialization.desired_phase, "deleting")
        self.assertEqual(acceptance.request_event.action, "delete")
        self.assertEqual(acceptance.response["observed_phase"], "running")
        self.assertEqual(get_materialization(self.table, "mat-1").desired_phase, "running")

    def test_deleting_needs_write_access_to_every_dataset_of_its_tables(self) -> None:
        self._keep("payments")
        materialization = get_materialization(self.table, "mat-1")
        self._save(retired_tables=(replace(materialization.retired_tables[0], dataset="secret", dataset_id=SECRET),))

        with self.assertRaises(WebError) as error_info:
            self._delete()

        self.assertEqual((403, "secret"), (error_info.exception.status_code, error_info.exception.details["dataset"]))



def setUpModule() -> None:
    # A deployment always names the namespace its runtimes run in (handbook 02 §2.3); these tests
    # reach the configuration without choosing one, so the environment names it for them.
    os.environ.setdefault("CONTROL_PLANE_DEFAULT_NAMESPACE", "midstream")
    control_plane_config.get_config.cache_clear()


if __name__ == "__main__":
    unittest.main()