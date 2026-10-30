"""The control-plane workflow: discovery, the table, the runtime, and deletion back to nothing (spec §9.1)."""

from __future__ import annotations

import asyncio
from collections.abc import Iterable
from dataclasses import replace
import os
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))

from google.api_core.exceptions import ServiceUnavailable  # noqa: E402

from gcs_fakes import FakeGcs  # noqa: E402
from table_records import table_specs  # noqa: E402

from midstream.runtime.templates import PackageSource
from midstream import config as control_plane_config  # noqa: E402
from midstream.config import ControlPlaneConfig  # noqa: E402
from midstream.handlers import handle_work_event_for_routed_output  # noqa: E402
from midstream.handlers.resource_observation import (  # noqa: E402
    build_sweep_observation_trigger,
    handle_observation_work_for_output,
)
from midstream.kafka.admin import ConsumerGroupNotEmptyError  # noqa: E402
from midstream.lakehouse.catalog import GcsPointerCatalog, StaticTableDirectory, TableEntry  # noqa: E402
from midstream.lakehouse.pointer import POINTER_NAME, TablePointer, utc_now, write_pointer  # noqa: E402
from midstream.lakehouse.tables import partition_granularity_of  # noqa: E402
from midstream.models import (  # noqa: E402
    TableClaimResult,
    EventCause,
    Materialization,
    MaterializationStatus,
    MaterializationWorkEvent,
    ResourceObservation,
    RunningMaintenance,
    RuntimeHealth,
)
from midstream.protection.policy import ProtectionPolicy, TableProtection  # noqa: E402
from midstream.runtime_app.config import RuntimeAppConfig  # noqa: E402
from midstream.schema import StubDiscoveryService  # noqa: E402
from midstream.services import process_work_event, process_work_event_async  # noqa: E402
from midstream.services.workflow import MAX_DESTINATION_CLEANUP_RETRY_ATTEMPTS  # noqa: E402
from midstream.storage import (  # noqa: E402
    get_materialization,
    get_materialization_schema,
    get_materialization_status,
    get_materialization_table,
    get_operation_record,
    get_runtime_binding,
    list_materialization_tables,
    list_schema_versions,
    save_materialization,
    save_materialization_status,
    save_materialization_table,
    save_operation_record,
)
from midstream.storage import objects  # noqa: E402

DESTINATION = "gs://lake"
MATERIALIZATION_ID = "mat-a1b2c3d4e5f6"
#: The materialization is named `shop`: its tables' Iceberg namespace, and its directory.
MATERIALIZATION_LOCATION = "gs://lake/materializer/mat-a1b2c3d4e5f6"
LOCATION = f"{MATERIALIZATION_LOCATION}/orders"
PAYMENTS_LOCATION = f"{MATERIALIZATION_LOCATION}/payments"


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


class FakeKasprAppClient:
    def __init__(self) -> None:
        self.applied: list[dict] = []
        self.deleted: list[dict] = []

    def apply(self, resource: dict) -> None:
        self.applied.append(resource)

    def delete(self, *, namespace: str, name: str, kind: str = "KasprApp") -> None:
        self.deleted.append({"namespace": namespace, "name": name, "kind": kind})


class FakeKafkaAdminClient:
    def __init__(self, *, topics: list[str] | None = None, consumer_groups: list[str] | None = None, busy: Iterable[str] = ()) -> None:
        self.topics = list(topics or [])
        self.consumer_groups = list(consumer_groups or [])
        self.busy = set(busy)
        self.deleted_topics: list[str] = []

    def list_topics(self) -> list[str]:
        return list(self.topics)

    def delete_topics(self, topics: list[str]) -> None:
        self.deleted_topics.extend(topics)
        self.topics = [topic for topic in self.topics if topic not in set(topics)]

    def list_consumer_groups(self) -> list[str]:
        return list(self.consumer_groups)

    def delete_consumer_group(self, group_id: str) -> None:
        if group_id in self.busy:
            raise ConsumerGroupNotEmptyError(group_id)
        self.consumer_groups = [group for group in self.consumer_groups if group != group_id]


def build_materialization(**overrides) -> Materialization:
    values = dict(
        materialization_id=MATERIALIZATION_ID,
        name="shop",
        tables=table_specs("orders"),
        destination_uri=DESTINATION,
        runtime_mode="dedicated",
        resolved_runtime_name="kaspr-mat-a1b2c3d4e5f6",
        namespace="midstream",
        desired_phase="running",
        created_at="2026-07-12T00:00:00Z",
        updated_at="2026-07-12T00:00:00Z",
        created_by="jane@example.com",
    )
    values.update(overrides)
    return Materialization(**values)


def build_status(**overrides) -> MaterializationStatus:
    values = dict(
        materialization_id=MATERIALIZATION_ID,
        desired_phase="running",
        observed_phase="requested",
        phase_reason="requested",
        last_transition_time="2026-07-12T00:00:00Z",
        observation_source="hybrid",
        runtime_health=RuntimeHealth(level="unknown"),
        resource_observation=ResourceObservation(
            namespace="midstream",
            kaspr_app_name="kaspr-mat-a1b2c3d4e5f6",
            resource_exists=False,
            managed_agent_states={},
        ),
    )
    values.update(overrides)
    return MaterializationStatus(**values)


def work(step: str, *, work_id: str | None = None, requested_at: str = "2026-07-12T00:00:05Z") -> MaterializationWorkEvent:
    return MaterializationWorkEvent(
        work_id=work_id or f"work-{step}",
        materialization_id=MATERIALIZATION_ID,
        step=step,
        requested_at=requested_at,
        caused_by=EventCause(kind="request", id="req-1"),
        dedupe_key=f"{step}:{MATERIALIZATION_ID}",
    )


def git_package(revision: str) -> PackageSource:
    """The package source of a control plane installed from this repository at `revision`."""
    return PackageSource(
        kind="git",
        reference=revision,
        requirement=f"git+https://github.com/vmirz/midstream.git@{revision}#egg=midstream-platform&subdirectory=python",
    )


SAMPLE = [
    {"order_id": "O-1", "total": 5, "customer": {"email": "a@example.com"}, "items": [{"sku": "A"}]},
    {"order_id": "O-2", "total": "five", "customer": {"email": "b@example.com"}},
]


class WorkflowTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.table = FakeTable()
        self.store = FakeGcs()
        self.client = self.store.client()
        self.kaspr = FakeKasprAppClient()
        self.config = ControlPlaneConfig(discovery_sample_records=50, discovery_sample_seconds=3)
        self.discovery = StubDiscoveryService(SAMPLE)
        patcher = patch("midstream.runtime.templates.installed_package_source", return_value=PackageSource(kind="git", reference="pkg-rev-1", requirement="git+https://github.com/vmirz/midstream.git@pkg-rev-1#egg=midstream-platform&subdirectory=python"))
        patcher.start()
        self.addCleanup(patcher.stop)
        save_materialization(self.table, build_materialization())
        save_materialization_status(self.table, build_status())

    def process(self, event: MaterializationWorkEvent, **kwargs) -> MaterializationWorkEvent | None:
        kwargs.setdefault("kaspr_app_client", self.kaspr)
        kwargs.setdefault("object_store_client", self.client)
        kwargs.setdefault("config", self.config)
        return process_work_event(self.table, event, self.discovery, **kwargs)

    def create(self) -> MaterializationWorkEvent:
        """Run discovery, the table's creation and the runtime's apply; return the observation."""
        follow_up = self.process(work("start_discovery"))
        self.assertEqual("create_table", follow_up.step)
        follow_up = self.process(follow_up)
        self.assertEqual("apply_runtime", follow_up.step)
        follow_up = self.process(follow_up)
        self.assertEqual("observe_runtime", follow_up.step)
        return follow_up

    def catalog(self, name: str = "orders", location: str = LOCATION) -> GcsPointerCatalog:
        entry = TableEntry(identifier=("shop", name), location=location, materialization_id=MATERIALIZATION_ID)
        return GcsPointerCatalog(directory=StaticTableDirectory([entry]), client=self.client, writer="test")

    def two_topics(self, **discovered) -> None:
        """Make the materialization read `orders` and `payments`, each sampled as `discovered` says."""
        save_materialization(self.table, build_materialization(tables=table_specs("orders", "payments")))
        if discovered:
            self.discovery = StubDiscoveryService(records_by_topic=discovered)

    def status(self) -> MaterializationStatus:
        return get_materialization_status(self.table, MATERIALIZATION_ID)


def claim_answer(step: str, *, purpose: str = "create", request_id: str = "idr-claim-1", reason: str | None = None) -> MaterializationWorkEvent:
    """The directory's answer to a claim of table names, as `handlers/identity.py` sends it."""
    return MaterializationWorkEvent(
        work_id=f"work-claim-{request_id}",
        materialization_id=MATERIALIZATION_ID,
        step=step,
        requested_at="2026-07-12T00:00:02Z",
        caused_by=EventCause(kind="request", id="req-1"),
        dedupe_key=f"{step}:{MATERIALIZATION_ID}:{request_id}",
        claim=TableClaimResult(
            request_id=request_id,
            purpose=purpose,
            status="applied" if step == "tables_claimed" else "rejected",
            reason=reason,
            message=None if reason is None else "shop.orders is taken: another materialization's table has that name. Choose another name.",
            details={} if reason is None else {"table": "orders", "dataset": "shop", "source_topic": "orders"},
        ),
    )


class ClaimAnswerTests(WorkflowTestCase):
    """A new materialization waits for its tables' names; the directory's answer moves it on, or ends it."""

    def setUp(self) -> None:
        super().setUp()
        save_materialization_status(self.table, build_status(phase_reason="claiming_tables"))

    def test_claimed_names_send_a_new_materialization_on_to_discovery(self) -> None:
        follow_up = self.process(claim_answer("tables_claimed"))

        self.assertEqual(("start_discovery", "work-req-1"), (follow_up.step, follow_up.work_id))
        self.assertEqual("start_discovery:mat-a1b2c3d4e5f6:req-1", follow_up.dedupe_key)
        self.assertEqual("kaspr-mat-a1b2c3d4e5f6", follow_up.target_runtime.kaspr_app_name)
        status = self.status()
        self.assertEqual(("requested", "tables_claimed"), (status.observed_phase, status.phase_reason))
        self.assertIn("shop.orders", status.phase_message)
        # And from there on, as always: discovery, the table in its dataset, the runtime.
        self.assertEqual("create_table", self.process(follow_up).step)

    def test_an_answer_given_twice_moves_it_on_once(self) -> None:
        self.process(self.process(claim_answer("tables_claimed")))

        self.assertIsNone(self.process(claim_answer("tables_claimed")))
        self.assertIsNone(self.process(claim_answer("claim_rejected", reason="table_name_taken")))
        self.assertEqual("discovered", self.status().observed_phase)

    def test_a_materialization_deleted_meanwhile_goes_nowhere(self) -> None:
        save_materialization(self.table, build_materialization(desired_phase="deleting"))

        self.assertIsNone(self.process(claim_answer("tables_claimed")))
        self.assertEqual([], self.discovery.calls)

    def test_a_refused_name_deletes_the_new_materialization_and_says_why(self) -> None:
        self.assertIsNone(self.process(claim_answer("claim_rejected", reason="table_name_taken")))

        materialization = get_materialization(self.table, MATERIALIZATION_ID)
        status = self.status()
        self.assertEqual("deleted", materialization.desired_phase)
        self.assertEqual(("deleted", "deleted", "table_name_taken"), (status.desired_phase, status.observed_phase, status.phase_reason))
        self.assertTrue(status.phase_message.startswith("Not created: shop.orders is taken"), status.phase_message)
        # Nothing was created, so nothing is dropped, and the runtime was never applied.
        self.assertEqual([], self.kaspr.applied)
        self.assertEqual([], self.store.names())


class DiscoveryTests(WorkflowTestCase):
    def test_discovery_evolves_schema_0_from_the_sample_and_queues_the_table(self) -> None:
        follow_up = self.process(work("start_discovery"))

        self.assertEqual("create_table", follow_up.step)
        self.assertEqual([{"materialization_id": MATERIALIZATION_ID, "sample_records": 50, "sample_seconds": 3}], self.discovery.calls)
        projection = get_materialization_schema(self.table, MATERIALIZATION_ID, "orders")
        self.assertEqual(0, projection.schema_id)
        self.assertEqual("orders", projection.table)
        names = [field["name"] for field in projection.fields]
        self.assertIn("customer.email", names)
        self.assertIn("items[].sku", names)
        totals = [field for field in projection.fields if field["name"] == "total"]
        self.assertEqual({"long", "string"}, {field["type"] for field in totals})
        self.assertTrue(any(change["kind"] == "forked" for change in projection.changes))
        self.assertEqual([0], [item.schema_id for item in list_schema_versions(self.table, MATERIALIZATION_ID, "orders")])
        status = self.status()
        self.assertEqual(("discovered", "schema_discovered"), (status.observed_phase, status.phase_reason))
        self.assertIn("2 sampled record", status.phase_message)

    def test_each_topic_gets_a_schema_0_of_its_own(self) -> None:
        self.two_topics(orders=SAMPLE, payments=[{"payment_id": "P-1", "amount": 3.5}])

        follow_up = self.process(work("start_discovery"))

        self.assertEqual("create_table", follow_up.step)
        self.assertEqual(1, len(self.discovery.calls))
        orders = {field["name"] for field in get_materialization_schema(self.table, MATERIALIZATION_ID, "orders").fields}
        payments = {field["name"] for field in get_materialization_schema(self.table, MATERIALIZATION_ID, "payments").fields}
        self.assertIn("customer.email", orders)
        self.assertNotIn("amount", orders)
        self.assertIn("amount", payments)
        self.assertNotIn("order_id", payments)
        self.assertEqual([0], [item.schema_id for item in list_schema_versions(self.table, MATERIALIZATION_ID, "payments")])
        self.assertEqual("Schema 0 discovered for 2 tables from 3 sampled record(s)", self.status().phase_message)

    def test_a_topic_without_records_is_named_in_the_discovery_message(self) -> None:
        self.two_topics(orders=SAMPLE, payments=[])

        self.process(work("start_discovery"))

        payments = get_materialization_schema(self.table, MATERIALIZATION_ID, "payments")
        self.assertEqual({"metadata"}, {field["kind"] for field in payments.fields})
        self.assertIn("payments hold(s) no records yet", self.status().phase_message)

    def test_an_empty_topic_starts_with_the_kafka_metadata_columns(self) -> None:
        self.discovery = StubDiscoveryService([])

        self.process(work("start_discovery"))

        projection = get_materialization_schema(self.table, MATERIALIZATION_ID, "orders")
        self.assertEqual({"metadata"}, {field["kind"] for field in projection.fields})
        self.assertIn("no records yet", self.status().phase_message)

    def test_discovery_applies_the_runtimes_schema_caps(self) -> None:
        self.discovery = StubDiscoveryService([{"attributes": {f"k{index}": index for index in range(12)}}])

        self.process(work("start_discovery"), runtime_config=RuntimeAppConfig(max_struct_fields=10))

        fields = {field["name"]: field for field in get_materialization_schema(self.table, MATERIALIZATION_ID, "orders").fields}
        self.assertEqual("json", fields["attributes"]["kind"])

    def test_a_topic_kafka_does_not_list_fails_the_create_instead_of_passing_for_an_empty_one(self) -> None:
        self.two_topics()
        self.discovery = StubDiscoveryService(records_by_topic={"orders": SAMPLE}, missing=["payments"])

        self.assertIsNone(self.process(work("start_discovery")))

        status = self.status()
        self.assertEqual(("failed", "source_topic_missing"), (status.observed_phase, status.phase_reason))
        self.assertEqual(
            "Workflow step start_discovery failed: Kafka has no topic payments, or the deployment's Kafka user may not "
            "describe it; once it can be read, delete the materialization and create it again",
            status.phase_message,
        )
        for name in ("orders", "payments"):
            self.assertIsNone(get_materialization_schema(self.table, MATERIALIZATION_ID, name), "no table is made")

    def test_a_materialization_deleted_while_its_topics_were_sampled_is_not_failed_for_a_missing_one(self) -> None:
        table = self.table

        class DeletedMeanwhile(StubDiscoveryService):
            def discover(self, materialization, **budget):
                save_materialization(table, replace(materialization, desired_phase="deleting"))
                return super().discover(materialization, **budget)

        self.discovery = DeletedMeanwhile(missing=["orders"])

        self.assertIsNone(self.process(work("start_discovery")))
        self.assertEqual("discovering", self.status().observed_phase)

    def test_a_materialization_deleted_meanwhile_skips_discovery(self) -> None:
        save_materialization(self.table, build_materialization(desired_phase="deleting"))

        self.assertIsNone(self.process(work("start_discovery")))
        self.assertIsNone(get_materialization_schema(self.table, MATERIALIZATION_ID, "orders"))


class ProtectionDiscoveryTests(WorkflowTestCase):
    """Discovery masks its samples and table creation records what the table protects (design §5.5)."""

    POLICY = ProtectionPolicy({"email": "hash", "phone": "partial", "national_id": "hash", "secret": "redact"})
    PROTECTED_SAMPLE = [
        {"order_id": "O-1", "ssn": 123456789, "phone": "540-948-9848", "password": "pw", "customer": {"email": "a@example.com"}},
        {"order_id": "O-2", "ssn": 987654321, "phone": 5409489848, "password": "pw2"},
    ]

    def setUp(self) -> None:
        super().setUp()
        save_materialization(self.table, build_materialization(protection=self.POLICY, protection_revision=1))
        self.discovery = StubDiscoveryService(self.PROTECTED_SAMPLE)

    def test_schema_zero_holds_the_masked_types(self) -> None:
        self.process(work("start_discovery"))
        projection = get_materialization_schema(self.table, MATERIALIZATION_ID, "orders")
        fields = {item["name"]: item["type"] for item in projection.fields}
        # A national id sampled as a number is a string field: its hashes are strings.
        self.assertEqual("string", fields["ssn"])
        self.assertEqual("string", fields["phone"])
        self.assertNotIn("password", fields)
        self.assertNotIn("phone__v2", fields)

    def test_a_new_table_records_its_protected_fields_in_metadata_v0(self) -> None:
        self.create()
        table = self.catalog().load_table(("shop", "orders"))
        protection = TableProtection.from_json(table.metadata.properties.get("materializer.protection"))
        self.assertEqual(1, protection.revision)
        self.assertEqual(self.POLICY, protection.policy)
        self.assertEqual(
            {("ssn",): ("national_id", "hash"), ("phone",): ("phone", "partial"), ("customer", "email"): ("email", "hash")},
            {item.path: (item.kind, item.mask) for item in protection.active_fields().values()},
        )
        self.assertEqual("jane@example.com", protection.history[0].by)
        projection = get_materialization_schema(self.table, MATERIALIZATION_ID, "orders")
        self.assertEqual(1, projection.protection_revision)
        annotated = {item["name"]: item.get("protection") for item in projection.fields}
        self.assertEqual({"kind": "national_id", "mask": "hash", "since_schema_id": 0}, annotated["ssn"])
        self.assertEqual({"kind": "email", "mask": "hash", "since_schema_id": 0}, annotated["customer.email"])
        self.assertIsNone(annotated["order_id"])

    def test_a_materialization_created_before_data_protection_records_nothing(self) -> None:
        save_materialization(self.table, build_materialization())
        self.create()
        table = self.catalog().load_table(("shop", "orders"))
        self.assertNotIn("materializer.protection", table.metadata.properties)
        projection = get_materialization_schema(self.table, MATERIALIZATION_ID, "orders")
        fields = {item["name"]: item["type"] for item in projection.fields}
        self.assertEqual("long", fields["ssn"])
        self.assertIn("password", fields)


class TableCreationTests(WorkflowTestCase):
    def test_the_table_is_created_with_schema_0_and_recorded(self) -> None:
        follow_up = self.process(self.process(work("start_discovery")))

        self.assertEqual("apply_runtime", follow_up.step)
        self.assertIsNotNone(self.store.get(f"{LOCATION}/metadata/{POINTER_NAME}"))
        iceberg = self.catalog().load_table(("shop", "orders"))
        self.assertEqual(0, iceberg.schema().schema_id)
        self.assertEqual("day", partition_granularity_of(iceberg.metadata.properties))
        self.assertEqual(MATERIALIZATION_ID, iceberg.metadata.properties["materializer.materialization-id"])
        self.assertEqual("orders", iceberg.metadata.properties["materializer.source-topic"])
        record = get_materialization_table(self.table, MATERIALIZATION_ID, "orders")
        self.assertEqual((record.location, record.namespace, record.name, record.source_topic), (LOCATION, "shop", "orders", "orders"))
        self.assertEqual(str(iceberg.metadata.table_uuid), record.table_uuid)
        self.assertEqual(iceberg.metadata_location, record.metadata_location)
        self.assertEqual(iceberg.metadata_location, get_materialization_schema(self.table, MATERIALIZATION_ID, "orders").metadata_location)
        self.assertEqual("table_created", self.status().phase_reason)
        self.assertEqual("Iceberg table shop.orders created", self.status().phase_message)

    def test_each_topic_gets_a_table_in_a_pass_of_its_own(self) -> None:
        self.two_topics(orders=SAMPLE, payments=[{"payment_id": "P-1"}])
        create = self.process(work("start_discovery"))

        second = self.process(create)

        self.assertEqual(("create_table", create.work_id, create.dedupe_key), (second.step, second.work_id, second.dedupe_key))
        self.assertEqual(["orders"], [record.name for record in list_materialization_tables(self.table, MATERIALIZATION_ID)])
        self.assertEqual("Iceberg table shop.orders created (1 of 2)", self.status().phase_message)

        follow_up = self.process(second)

        self.assertEqual("apply_runtime", follow_up.step)
        records = {record.name: record for record in list_materialization_tables(self.table, MATERIALIZATION_ID)}
        self.assertEqual({"orders": LOCATION, "payments": PAYMENTS_LOCATION}, {name: record.location for name, record in records.items()})
        self.assertEqual("payments", records["payments"].source_topic)
        payments = self.catalog("payments", PAYMENTS_LOCATION).load_table(("shop", "payments"))
        self.assertEqual("payments", payments.metadata.properties["materializer.source-topic"])
        self.assertIn("payment_id", [field.name for field in payments.schema().fields])
        self.assertNotEqual(records["orders"].table_uuid, records["payments"].table_uuid)
        self.assertEqual("Iceberg table shop.payments created (2 of 2)", self.status().phase_message)

    def test_a_redelivered_pass_creates_only_the_tables_still_missing(self) -> None:
        self.two_topics(orders=SAMPLE, payments=[])
        create = self.process(work("start_discovery"))
        self.process(create)
        orders_uuid = get_materialization_table(self.table, MATERIALIZATION_ID, "orders").table_uuid

        # The first pass delivered again: it skips orders and creates payments.
        follow_up = self.process(create)

        self.assertEqual("apply_runtime", follow_up.step)
        self.assertEqual(orders_uuid, get_materialization_table(self.table, MATERIALIZATION_ID, "orders").table_uuid)
        self.assertIsNotNone(get_materialization_table(self.table, MATERIALIZATION_ID, "payments"))

    def test_the_table_is_partitioned_as_the_materialization_asked(self) -> None:
        for granularity in ("hour", "month", "none"):
            with self.subTest(granularity=granularity):
                self.setUp()
                save_materialization(self.table, build_materialization(partition_granularity=granularity))
                self.process(self.process(work("start_discovery")))
                iceberg = self.catalog().load_table(("shop", "orders"))
                self.assertEqual(granularity, partition_granularity_of(iceberg.metadata.properties))
                self.assertEqual(granularity, get_materialization_table(self.table, MATERIALIZATION_ID, "orders").partition_granularity)

    def test_a_redelivered_create_finds_its_table_and_goes_on(self) -> None:
        create = self.process(work("start_discovery"))
        self.process(create)
        objects_before = self.store.names()

        follow_up = self.process(create)

        self.assertEqual("apply_runtime", follow_up.step)
        self.assertEqual(objects_before, self.store.names())

    def test_a_create_that_crashed_after_the_pointer_adopts_its_own_table(self) -> None:
        create = self.process(work("start_discovery"))
        self.process(create)
        uuid = get_materialization_table(self.table, MATERIALIZATION_ID, "orders").table_uuid
        del self.table[f"materialization-table:{MATERIALIZATION_ID}:orders"]

        follow_up = self.process(create)

        self.assertEqual("apply_runtime", follow_up.step)
        self.assertEqual(uuid, get_materialization_table(self.table, MATERIALIZATION_ID, "orders").table_uuid)

    def test_a_location_holding_another_materializations_table_is_refused(self) -> None:
        write_pointer(
            self.client,
            LOCATION,
            TablePointer(
                table_uuid="00000000-0000-0000-0000-000000000000",
                identifier=("shop", "orders"),
                materialization_id="mat-ffffffffffff",
                location=LOCATION,
                metadata_location=f"{LOCATION}/metadata/00000-x.metadata.json",
                sequence=0,
                updated_at=utc_now(),
                updated_by="someone-else",
            ),
            generation=0,
        )

        self.assertIsNone(self.process(self.process(work("start_discovery"))))

        status = self.status()
        self.assertEqual(("failed", "table_location_in_use"), (status.observed_phase, status.phase_reason))
        self.assertIsNone(get_materialization_table(self.table, MATERIALIZATION_ID, "orders"))


    def test_a_destination_that_cannot_hold_a_table_fails_the_step(self) -> None:
        # Recorded before the create view refused such destinations.
        save_materialization(self.table, build_materialization(destination_uri="abfs://lakehouse/orders"))

        self.assertIsNone(self.process(self.process(work("start_discovery"))))

        status = self.status()
        self.assertEqual(("failed", "invalid_destination"), (status.observed_phase, status.phase_reason))
        self.assertIn("abfs://lakehouse/orders cannot hold a table", status.phase_message)
        self.assertIsNone(get_materialization_table(self.table, MATERIALIZATION_ID, "orders"))


class RuntimeApplyTests(WorkflowTestCase):
    def test_the_runtime_is_rendered_against_the_table_and_bound(self) -> None:
        self.create()

        kinds = [resource["kind"] for resource in self.kaspr.applied]
        self.assertEqual(["KasprApp", "KasprTable", "KasprAgent", "KasprAgent", "KasprAgent", "KasprTask", "KasprTask", "KasprTask"], kinds)
        # The maintenance clock a runtime ran before 2.0 goes with the apply.
        self.assertEqual([{"namespace": "midstream", "name": "kaspr-mat-a1b2c3d4e5f6-maintenance-timer", "kind": "KasprTask"}], self.kaspr.deleted)
        writer = next(resource for resource in self.kaspr.applied if resource["metadata"]["name"].endswith("-writer"))
        self.assertIn(LOCATION, writer["spec"]["processors"]["init"]["python"])
        binding = get_runtime_binding(self.table, MATERIALIZATION_ID)
        self.assertEqual(
            ["kaspr-mat-a1b2c3d4e5f6-writer", "kaspr-mat-a1b2c3d4e5f6-committer", "kaspr-mat-a1b2c3d4e5f6-settings"],
            binding.managed_agent_names,
        )
        self.assertEqual(["orders"], binding.source_topics)
        self.assertEqual("runtime_apply_in_progress", self.status().phase_reason)

    def test_one_runtime_reads_every_topic(self) -> None:
        self.two_topics(orders=SAMPLE, payments=[])
        follow_up = self.process(self.process(self.process(work("start_discovery"))))

        self.assertEqual("apply_runtime", follow_up.step)
        self.assertEqual("observe_runtime", self.process(follow_up).step)
        writer = next(resource for resource in self.kaspr.applied if resource["metadata"]["name"].endswith("-writer"))
        self.assertEqual("orders,payments", writer["spec"]["input"]["topic"]["name"])
        init = writer["spec"]["processors"]["init"]["python"]
        self.assertIn(LOCATION, init)
        self.assertIn(PAYMENTS_LOCATION, init)
        self.assertEqual(1, sum(resource["kind"] == "KasprApp" for resource in self.kaspr.applied))
        self.assertEqual(["orders", "payments"], get_runtime_binding(self.table, MATERIALIZATION_ID).source_topics)

    def test_resuming_an_interrupted_create_creates_the_missing_tables_first(self) -> None:
        self.two_topics(orders=SAMPLE, payments=[])
        self.process(self.process(work("start_discovery")))  # orders created; payments' pass lost

        resumed = self.process(work("apply_runtime", work_id="work-req-resume"))

        self.assertEqual("create_table", resumed.step)
        self.assertEqual([], self.kaspr.applied)
        apply = self.process(resumed)
        self.assertEqual(("apply_runtime", "work-apply-mat-a1b2c3d4e5f6-work-req-resume"), (apply.step, apply.work_id))
        self.assertEqual("observe_runtime", self.process(apply).step)
        self.assertEqual(
            ["orders", "payments"],
            sorted(record.name for record in list_materialization_tables(self.table, MATERIALIZATION_ID)),
        )

    def drift_reapply(self) -> MaterializationWorkEvent:
        """The re-apply the observation sweep publishes for the runtime, as it publishes it."""
        outputs = [
            output["value"]
            for output in handle_observation_work_for_output(
                build_sweep_observation_trigger(requested_at="2026-07-12T00:10:00Z"),
                self.table,
                self.kaspr,
                observed_at="2026-07-12T00:10:00Z",
            )
            if output["value"]["step"] == "apply_runtime"
        ]
        self.assertEqual(1, len(outputs))
        return MaterializationWorkEvent.from_dict(outputs[0])

    def test_a_runtime_taken_to_another_package_and_back_is_applied_again(self) -> None:
        # A release, a build of a branch, the release again: each control plane re-applies the
        # runtime with its own package, and the sweep names that work after the spec it wants, so
        # the way back asks for work that was done once before.
        self.create()
        for step, revision in enumerate(("pkg-rev-2", "pkg-rev-3", "pkg-rev-2"), start=1):
            with patch("midstream.runtime.templates.installed_package_source", return_value=git_package(revision)):
                reapply = self.drift_reapply()
                self.assertIsNotNone(self.process(reapply), f"re-apply {step}, to {revision}, was skipped")
            app = [resource for resource in self.kaspr.applied if resource["kind"] == "KasprApp"][-1]
            self.assertEqual(revision, app["metadata"]["annotations"]["control-plane/package-revision"])

        applied = len(self.kaspr.applied)
        with patch("midstream.runtime.templates.installed_package_source", return_value=git_package("pkg-rev-2")):
            self.assertIsNone(self.process(reapply), "delivered again once the runtime has its spec")
        self.assertEqual(applied, len(self.kaspr.applied))

    def test_a_materialization_created_before_1_0_cannot_be_applied(self) -> None:
        self.assertIsNone(self.process(work("apply_runtime")))

        status = self.status()
        self.assertEqual(("failed", "table_missing"), (status.observed_phase, status.phase_reason))
        self.assertIn("orders", status.phase_message)
        self.assertIn("delete it and create it again", status.phase_message)
        self.assertEqual([], self.kaspr.applied)

    def test_the_routed_chain_publishes_each_step_as_work(self) -> None:
        routed = list(
            handle_work_event_for_routed_output(
                work("start_discovery").to_dict(),
                self.table,
                self.discovery,
                kaspr_app_client=self.kaspr,
                object_store_client=self.client,
                config=self.config,
            )
        )
        self.assertEqual([("work", "create_table")], [(item["route"], item["value"]["step"]) for item in routed])

    def test_the_awaited_chain_matches_the_blocking_one(self) -> None:
        blocking = FakeTable()
        blocking.values = dict(self.table.values)
        self.create()
        expected = get_materialization_table(self.table, MATERIALIZATION_ID, "orders")

        self.table = blocking
        self.store = FakeGcs()
        self.client = self.store.client()

        async def chain():
            event = work("start_discovery")
            while event is not None and event.step != "observe_runtime":
                event = await process_work_event_async(
                    self.table,
                    event,
                    self.discovery,
                    kaspr_app_client=self.kaspr,
                    object_store_client=self.client,
                    config=self.config,
                )
            return event

        self.assertEqual("observe_runtime", asyncio.run(chain()).step)
        record = get_materialization_table(self.table, MATERIALIZATION_ID, "orders")
        self.assertEqual((expected.location, expected.namespace, expected.name), (record.location, record.namespace, record.name))


class DeletionTests(WorkflowTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.create()
        self.store.put(f"{LOCATION}/data/ts_day=2026-07-12/p00-o0000000000-0000000009-m-0.parquet", b"rows")
        self.store.put("gs://lake/materializer/other-ffffffffffff/orders/data/x.parquet", b"another table")
        save_materialization(self.table, replace(get_materialization(self.table, MATERIALIZATION_ID), desired_phase="deleting"))
        save_materialization_status(
            self.table,
            replace(self.status(), desired_phase="deleting", observed_phase="deleted", phase_reason="resource_missing_after_delete"),
        )

    def delete_chain(self, **kwargs) -> list[MaterializationWorkEvent]:
        """Run Kafka cleanup and everything that follows it; return the events in order."""
        kafka = FakeKafkaAdminClient(topics=["kaspr-mat-a1b2c3d4e5f6-commits", "orders"], consumer_groups=["kaspr-mat-a1b2c3d4e5f6-app"])
        events = []
        event = work("cleanup_runtime_kafka")
        while event is not None:
            events.append(event)
            event = self.process(event, kafka_admin_client=kafka, **kwargs)
        return events

    def test_deleting_drops_the_table_last_the_pointer_and_purges_the_records(self) -> None:
        steps = [event.step for event in self.delete_chain()]

        self.assertEqual(["cleanup_runtime_kafka", "drop_table", "purge_materialization_records"], steps)
        self.assertEqual(["gs://lake/materializer/other-ffffffffffff/orders/data/x.parquet"], self.store.names())
        self.assertIsNone(get_materialization(self.table, MATERIALIZATION_ID))
        self.assertIsNone(get_materialization_table(self.table, MATERIALIZATION_ID, "orders"))
        self.assertIsNone(get_materialization_schema(self.table, MATERIALIZATION_ID, "orders"))
        self.assertEqual([], list_schema_versions(self.table, MATERIALIZATION_ID, "orders"))

    def test_with_cleanup_off_the_table_only_leaves_the_catalog(self) -> None:
        self.config = replace(self.config, destination_cleanup_enabled=False)

        self.delete_chain()

        self.assertIsNone(self.store.get(f"{LOCATION}/metadata/{POINTER_NAME}"))
        self.assertIsNotNone(self.store.get(f"{LOCATION}/metadata/catalog-pointer.dropped.json"))
        self.assertIsNotNone(self.store.get(f"{LOCATION}/data/ts_day=2026-07-12/p00-o0000000000-0000000009-m-0.parquet"))

    def test_a_large_table_is_dropped_in_passes_under_one_operation(self) -> None:
        for index in range(5):
            self.store.put(f"{LOCATION}/data/ts_day=2026-07-12/extra-{index}.parquet", b"rows")
        self.config = replace(self.config, purge_records_on_delete=False)

        with patch.object(objects, "CLEANUP_PASS_MAX_OBJECTS", 2):
            events = self.delete_chain()

        drops = [event for event in events if event.step == "drop_table"]
        self.assertGreater(len(drops), 2)
        self.assertEqual(1, len({event.work_id for event in drops}))
        record = get_operation_record(self.table, materialization_id=MATERIALIZATION_ID, operation_id=drops[0].work_id)
        self.assertEqual("applied", record["payload"]["status"])
        self.assertEqual(len(drops), record["payload"]["passes"])
        self.assertEqual(["gs://lake/materializer/other-ffffffffffff/orders/data/x.parquet"], self.store.names())

    def test_a_job_the_maintenance_app_forgot_is_waited_out_once_not_before_every_pass(self) -> None:
        """A maintenance app restarted mid-job never reports that job done, so the table's record says it
        runs for ever. Waited out once, it is forgotten: before, every pass of a large table's drop
        waited the full five minutes for it again."""
        for index in range(5):
            self.store.put(f"{LOCATION}/data/ts_day=2026-07-12/extra-{index}.parquet", b"rows")
        self.config = replace(self.config, purge_records_on_delete=False)
        record = get_materialization_table(self.table, MATERIALIZATION_ID, "orders")
        stale = RunningMaintenance(job="compaction", started_at="2026-07-12T00:00:00Z", member_id="maintenance-0")
        save_materialization_table(self.table, replace(record, maintenance_job=stale))

        with patch.object(objects, "CLEANUP_PASS_MAX_OBJECTS", 2):
            drop = self.process(work("cleanup_runtime_kafka"), kafka_admin_client=FakeKafkaAdminClient())
            wait = self.process(drop)
            self.assertIsNotNone(wait.observation_policy.scheduled_for, "it waits for the job first")
            # Five minutes on, the job still recorded as running.
            operation = get_operation_record(self.table, materialization_id=MATERIALIZATION_ID, operation_id=drop.work_id)
            waited = dict(operation["payload"]["maintenance_wait"], since="2020-01-01T00:00:00Z")
            save_operation_record(
                self.table,
                materialization_id=MATERIALIZATION_ID,
                operation_id=drop.work_id,
                updated_at="2026-07-12T01:00:00Z",
                payload=dict(operation["payload"], maintenance_wait=waited),
            )

            passes = []
            event = self.process(wait)
            while event is not None:
                passes.append(event)
                self.assertIsNone(event.observation_policy.scheduled_for if event.observation_policy else None, "no pass waits again")
                event = self.process(event)

        self.assertIsNone(get_materialization_table(self.table, MATERIALIZATION_ID, "orders").maintenance_job, "forgotten")
        self.assertGreater(len(passes), 1, "a drop of several passes")
        self.assertEqual(["gs://lake/materializer/other-ffffffffffff/orders/data/x.parquet"], self.store.names())

    def test_a_failing_drop_is_retried_with_backoff_then_fails_loudly(self) -> None:
        self.store.fail = lambda operation, bucket, name: ServiceUnavailable("GCS is down") if operation == "list_blobs" else None
        event = self.process(work("cleanup_runtime_kafka"), kafka_admin_client=FakeKafkaAdminClient())
        attempts = []
        while event is not None and event.step == "drop_table":
            follow_up = self.process(event)
            if follow_up is not None:
                attempts.append(follow_up.observation_policy.attempt)
            event = follow_up

        self.assertEqual(list(range(1, MAX_DESTINATION_CLEANUP_RETRY_ATTEMPTS + 1)), attempts)
        self.assertEqual("failed", self.status().observed_phase)
        self.assertIsNotNone(self.store.get(f"{LOCATION}/metadata/{POINTER_NAME}"))

    def test_a_materialization_without_a_table_goes_straight_to_the_purge(self) -> None:
        del self.table[f"materialization-table:{MATERIALIZATION_ID}:orders"]

        steps = [event.step for event in self.delete_chain()]

        self.assertEqual(["cleanup_runtime_kafka", "drop_table", "purge_materialization_records"], steps)
        self.assertIsNotNone(self.store.get(f"{LOCATION}/metadata/{POINTER_NAME}"))


class MultiTableDeletionTests(WorkflowTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.two_topics(orders=SAMPLE, payments=[{"payment_id": "P-1"}])
        event = work("start_discovery")
        while event.step != "observe_runtime":
            event = self.process(event)
        for location in (LOCATION, PAYMENTS_LOCATION):
            for index in range(3):
                self.store.put(f"{location}/data/ts_day=2026-07-12/p00-o{index:010d}-m-0.parquet", b"rows")
        self.store.put("gs://lake/materializer/other-ffffffffffff/orders/data/x.parquet", b"another table")
        save_materialization(self.table, replace(get_materialization(self.table, MATERIALIZATION_ID), desired_phase="deleting"))
        save_materialization_status(
            self.table,
            replace(self.status(), desired_phase="deleting", observed_phase="deleted", phase_reason="resource_missing_after_delete"),
        )

    def delete_chain(self) -> list[MaterializationWorkEvent]:
        kafka = FakeKafkaAdminClient(topics=["kaspr-mat-a1b2c3d4e5f6-commits", "orders", "payments"])
        events = []
        event = work("cleanup_runtime_kafka")
        while event is not None:
            events.append(event)
            event = self.process(event, kafka_admin_client=kafka)
        return events

    def test_every_table_is_dropped_under_one_operation_then_the_records_purged(self) -> None:
        self.config = replace(self.config, purge_records_on_delete=False)

        with patch.object(objects, "CLEANUP_PASS_MAX_OBJECTS", 2):
            events = self.delete_chain()

        drops = [event for event in events if event.step == "drop_table"]
        self.assertEqual(1, len({event.work_id for event in drops}))
        record = get_operation_record(self.table, materialization_id=MATERIALIZATION_ID, operation_id=drops[0].work_id)
        self.assertEqual("applied", record["payload"]["status"])
        self.assertEqual(["orders", "payments"], record["payload"]["tables_dropped"])
        self.assertEqual(len(drops), record["payload"]["passes"])
        self.assertEqual(["gs://lake/materializer/other-ffffffffffff/orders/data/x.parquet"], self.store.names())

    def test_the_purge_removes_every_tables_records(self) -> None:
        steps = [event.step for event in self.delete_chain()]

        self.assertEqual("purge_materialization_records", steps[-1])
        self.assertEqual([], list_materialization_tables(self.table, MATERIALIZATION_ID))
        for name in ("orders", "payments"):
            self.assertIsNone(get_materialization_schema(self.table, MATERIALIZATION_ID, name))
            self.assertEqual([], list_schema_versions(self.table, MATERIALIZATION_ID, name))
        self.assertEqual([], [key for key in self.table.values if MATERIALIZATION_ID in key])

    def test_a_failing_pass_resumes_with_the_table_it_was_on(self) -> None:
        self.config = replace(self.config, purge_records_on_delete=False)
        event = self.process(work("cleanup_runtime_kafka"), kafka_admin_client=FakeKafkaAdminClient())
        while event.step == "drop_table":
            record = get_operation_record(self.table, materialization_id=MATERIALIZATION_ID, operation_id=event.work_id)
            if record is not None and record["payload"].get("tables_dropped") == ["orders"]:
                break
            event = self.process(event)
        self.assertIsNone(self.store.get(f"{LOCATION}/metadata/{POINTER_NAME}"))
        self.assertIsNotNone(self.store.get(f"{PAYMENTS_LOCATION}/metadata/{POINTER_NAME}"))
        self.store.fail = lambda operation, bucket, name: ServiceUnavailable("GCS is down") if operation == "list_blobs" else None

        retry = self.process(event)

        self.assertEqual(1, retry.observation_policy.attempt)
        record = get_operation_record(self.table, materialization_id=MATERIALIZATION_ID, operation_id=event.work_id)
        self.assertEqual(["orders"], record["payload"]["tables_dropped"])
        self.store.fail = None
        while retry is not None and retry.step == "drop_table":
            retry = self.process(retry)
        self.assertIsNone(self.store.get(f"{PAYMENTS_LOCATION}/metadata/{POINTER_NAME}"))
        self.assertEqual(["gs://lake/materializer/other-ffffffffffff/orders/data/x.parquet"], self.store.names())


def setUpModule() -> None:
    # A deployment always names the namespace its runtimes run in (handbook 02 §2.3); the sweep
    # reaches the configuration without choosing one, so the environment names it for it.
    os.environ.setdefault("CONTROL_PLANE_DEFAULT_NAMESPACE", "midstream")
    control_plane_config.get_config.cache_clear()


def tearDownModule() -> None:
    # Renders read the deployment's configuration, which is cached: leave none behind for other modules.
    control_plane_config.get_config.cache_clear()


if __name__ == "__main__":
    unittest.main()
