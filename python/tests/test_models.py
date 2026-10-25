from __future__ import annotations

from dataclasses import replace
import unittest

from materializer_control_plane.models import (
    EventCause,
    Materialization,
    MaterializationRequestEvent,
    MaterializationRequestSpec,
    MaterializationStatus,
    MaterializationWorkEvent,
    ObservationPolicy,
    RecordEnvelope,
    ResourceCondition,
    ResourceObservation,
    RuntimeBinding,
    RuntimeHealth,
    RuntimeProgress,
    RuntimeStatusEvent,
    MaterializationSchema,
    MaterializationTable,
    SettingChange,
    SettingFallback,
    SettingOverride,
    SettingsChangeRecord,
    SettingsBootstrapRequest,
    SettingsChangeRequest,
    SettingsDocument,
    SettingsRejection,
    TableEvent,
    TableSpec,
    TargetRuntime,
    TopicChangeRequest,
)
from materializer_control_plane.models.events import TableClaimResult
from materializer_control_plane.models.materialization import PendingTopicChange, TopicChangePlan
from materializer_control_plane.models.status import ConsumerLag, MemberProgress, TopicLag
from materializer_control_plane.models.table import MaintenanceRun, TableError


class ModelRoundTripTests(unittest.TestCase):
    def test_materialization_round_trip(self) -> None:
        materialization = Materialization(
            materialization_id="mat-123",
            name="orders-materialization",
            tables=(TableSpec(name="orders", source_topic="orders", dataset="shop", dataset_id="ds-000000000001"), TableSpec(name="orders_v2", source_topic="Orders.v2", dataset="shop", dataset_id="ds-000000000001")),
            destination_uri="abfs://lakehouse/orders",
            runtime_mode="dedicated",
            resolved_runtime_name="kaspr-mat-123",
            namespace="midstream",
            desired_phase="running",
            created_at="2026-07-12T00:00:00Z",
            updated_at="2026-07-12T00:00:00Z",
            created_by="vmirzoyan",
            resource_profile="small",
            partition_granularity="hour",
            labels={"domain": "orders"},
            annotations={"owner": "analytics"},
        )

        round_tripped = Materialization.from_dict(materialization.to_dict())

        self.assertEqual(round_tripped, materialization)
        self.assertEqual(("orders", "Orders.v2"), materialization.source_topics)
        self.assertEqual(("orders", "orders_v2"), materialization.table_names)
        self.assertEqual(["orders", "Orders.v2"], materialization.to_dict()["source_topics"])
        self.assertEqual("Orders.v2", materialization.table("ORDERS_V2").source_topic)
        self.assertIsNone(materialization.table("payments"))

    def test_a_materialization_needs_distinct_tables_and_topics(self) -> None:
        def build(*tables: TableSpec) -> Materialization:
            return Materialization(
                materialization_id="mat-123",
                namespace="midstream",
                name="shop",
                tables=tables,
                destination_uri="gs://lake",
                desired_phase="running",
                created_at="2026-07-12T00:00:00Z",
                updated_at="2026-07-12T00:00:00Z",
            )

        with self.assertRaisesRegex(ValueError, "at least one source topic"):
            build()
        with self.assertRaisesRegex(ValueError, "table names must be unique"):
            build(TableSpec(name="orders", source_topic="orders", dataset="shop", dataset_id="ds-000000000001"), TableSpec(name="ORDERS", source_topic="orders2", dataset="shop", dataset_id="ds-000000000001"))
        with self.assertRaisesRegex(ValueError, "source topics must be unique"):
            build(TableSpec(name="orders", source_topic="orders", dataset="shop", dataset_id="ds-000000000001"), TableSpec(name="orders_2", source_topic="orders", dataset="shop", dataset_id="ds-000000000001"))

    def test_a_materialization_round_trips_its_kept_tables(self) -> None:
        kept = TableSpec(name="payments", source_topic="payments", dataset="shop", dataset_id="ds-000000000001")
        materialization = Materialization(
            materialization_id="mat-123",
            namespace="midstream",
            name="shop",
            tables=(TableSpec(name="orders", source_topic="orders", dataset="shop", dataset_id="ds-000000000001"),),
            retired_tables=(kept,),
            destination_uri="gs://lake",
            desired_phase="running",
            created_at="2026-07-12T00:00:00Z",
            updated_at="2026-07-12T00:00:00Z",
        )

        self.assertEqual(Materialization.from_dict(materialization.to_dict()), materialization)
        self.assertEqual(("orders",), materialization.source_topics)
        self.assertIsNone(materialization.table("payments"))
        self.assertEqual(kept, materialization.any_table("PAYMENTS"))
        stored = materialization.to_dict()
        del stored["retired_tables"]
        self.assertEqual((), Materialization.from_dict(stored).retired_tables, "records written before 2.0")
        with self.assertRaisesRegex(ValueError, "table names must be unique"):
            Materialization.from_dict({**materialization.to_dict(), "retired_tables": [{"name": "Orders", "source_topic": "orders.old", "dataset": "shop", "dataset_id": "ds-000000000001"}]})
        with self.assertRaisesRegex(ValueError, "source topics must be unique"):
            Materialization.from_dict({**materialization.to_dict(), "retired_tables": [{"name": "old", "source_topic": "orders", "dataset": "shop", "dataset_id": "ds-000000000001"}]})

    def test_a_materialization_round_trips_its_pending_change_of_topics(self) -> None:
        orders = TableSpec(name="orders", source_topic="sales.orders", dataset="sales", dataset_id="ds-000000000001")
        invoices = TableSpec(name="invoices", source_topic="finance.invoices", dataset="finance", dataset_id="ds-000000000002")
        plan = TopicChangePlan(
            tables=(orders, invoices),
            retired_tables=(),
            added=(invoices,),
            resumed=(),
            removed=(),
            retired=(),
            dropped=(),
        )
        pending = PendingTopicChange(request_id="req-2", claim_request_id="idr-claim-req-2", submitted_at="2026-10-22T00:00:00Z", plan=plan)
        materialization = Materialization(
            materialization_id="mat-a1b2c3d4e5f6",
            namespace="midstream",
            name="",
            tables=(orders,),
            destination_uri="gs://lake",
            desired_phase="running",
            created_at="2026-10-22T00:00:00Z",
            updated_at="2026-10-22T00:00:00Z",
            pending_topic_change=pending,
        )

        self.assertEqual(Materialization.from_dict(materialization.to_dict()), materialization)
        self.assertEqual(PendingTopicChange.from_dict(pending.to_dict()), pending)
        self.assertEqual((invoices,), pending.added)
        self.assertTrue(plan.changes_runtime)
        self.assertEqual("mat-a1b2c3d4e5f6", materialization.label, "an unnamed materialization goes by its id")
        self.assertEqual("Shop", replace(materialization, name="Shop").label)
        stored = materialization.to_dict()
        del stored["pending_topic_change"]
        self.assertIsNone(Materialization.from_dict(stored).pending_topic_change)
        with self.assertRaisesRegex(ValueError, "claim_request_id"):
            PendingTopicChange(request_id="req-2", claim_request_id="", submitted_at="2026-10-22T00:00:00Z", plan=plan)

    def test_a_materialization_lists_each_dataset_of_its_tables_once(self) -> None:
        def spec(name: str, dataset: str, dataset_id: str) -> TableSpec:
            return TableSpec(name=name, source_topic=f"{dataset}.{name}", dataset=dataset, dataset_id=dataset_id)

        materialization = Materialization(
            materialization_id="mat-a1b2c3d4e5f6",
            namespace="midstream",
            name="shop",
            tables=(spec("orders", "sales", "ds-1"), spec("invoices", "finance", "ds-2"), spec("refunds", "sales", "ds-1")),
            retired_tables=(spec("ledger", "audit", "ds-3"),),
            destination_uri="gs://lake",
            desired_phase="running",
            created_at="2026-10-22T00:00:00Z",
            updated_at="2026-10-22T00:00:00Z",
        )

        self.assertEqual(("ds-1", "ds-2", "ds-3"), materialization.dataset_ids)

    def test_table_spec_round_trip(self) -> None:
        spec = TableSpec(name="orders_v2", source_topic="Orders.v2", dataset="shop", dataset_id="ds-000000000001")
        self.assertEqual(TableSpec.from_dict(spec.to_dict()), spec)
        self.assertEqual(
            {"name": "orders_v2", "source_topic": "Orders.v2", "dataset": "shop", "dataset_id": "ds-000000000001"},
            spec.to_dict(),
        )
        self.assertEqual("shop.orders_v2", spec.qualified_name)
        with self.assertRaisesRegex(KeyError, "dataset"):
            TableSpec.from_dict({"name": "orders", "source_topic": "orders"})

    def test_a_materialization_partitions_by_day_unless_it_says_otherwise(self) -> None:
        stored = {
            "materialization_id": "mat-123",
            "name": "orders",
            "namespace": "midstream",
            "tables": [{"name": "orders", "source_topic": "orders", "dataset": "shop", "dataset_id": "ds-000000000001"}],
            "destination_uri": "gs://lake",
            "desired_phase": "running",
            "created_at": "2026-07-12T00:00:00Z",
            "updated_at": "2026-07-12T00:00:00Z",
        }
        self.assertEqual("day", Materialization.from_dict(stored).partition_granularity)
        with self.assertRaisesRegex(ValueError, "partition_granularity"):
            Materialization.from_dict({**stored, "partition_granularity": "week"})

    def test_schema_projection_round_trip(self) -> None:
        schema = MaterializationSchema(
            materialization_id="mat-123",
            table="orders",
            schema_id=2,
            since="2026-07-12T00:00:00Z",
            fields=[{"field_id": 10, "path": ["order_id"], "name": "order_id", "type": "string", "kind": "value"}],
            field_count=1,
            fork_map={"entries": []},
            iceberg_schema={"type": "struct", "schema-id": 2, "fields": []},
            metadata_location="gs://lake/t/metadata/00002-a.metadata.json",
            changes=[{"kind": "added", "path": ["order_id"]}],
            last_column_id=10,
            rejected_paths=[["too", "many"]],
        )

        self.assertEqual(MaterializationSchema.from_dict(schema.to_dict()), schema)

    def test_table_record_round_trip(self) -> None:
        record = MaterializationTable(
            materialization_id="mat-123",
            dataset="shop",
            dataset_id="ds-000000000001",
            name="orders",
            source_topic="orders",
            location="gs://lake/materializer/mat-123/orders",
            table_uuid="6b1c0000-0000-0000-0000-000000000000",
            partition_granularity="day",
            created_at="2026-07-12T00:00:00Z",
            updated_at="2026-07-12T00:01:00Z",
            metadata_location="gs://lake/materializer/mat-123/orders/metadata/00001-a.metadata.json",
            current_snapshot_id=42,
            sequence_number=1,
            last_commit_at="2026-07-12T00:01:00Z",
            totals={"records": 10, "files": 1, "bytes": 2048},
            offsets={"0": 9},
            valid_through="2026-07-12T00:00:59Z",
            snapshots=1,
            schema_id=0,
            maintenance=(MaintenanceRun(job="compaction", completed_at="2026-07-12T00:30:00Z", counts={"inputs": 8}, partition="2026-07-12", event_id="evt-1"),),
            last_error=TableError(reason="commit_failed", message="GCS is down", occurred_at="2026-07-12T00:00:30Z"),
        )

        self.assertEqual(MaterializationTable.from_dict(record.to_dict()), record)
        self.assertEqual(("shop", "orders"), record.identifier)
        self.assertEqual("shop", record.namespace)
        self.assertEqual("shop.orders", record.qualified_name)
        self.assertEqual("active", record.state)
        for state in ("retired", "dropping", "dropped"):
            kept = MaterializationTable.from_dict({**record.to_dict(), "state": state})
            self.assertEqual(state, kept.state)
            self.assertEqual(state == "dropping", kept.error_holds)
        stored = record.to_dict()
        del stored["state"]
        self.assertEqual("active", MaterializationTable.from_dict(stored).state)
        from materializer_control_plane.models import RunningMaintenance

        running = RunningMaintenance(job="compaction", started_at="2026-07-12T00:10:00Z", partition="2026-07-12", attempt=2, member_id="maintenance-0")
        busy = MaterializationTable.from_dict({**record.to_dict(), "maintenance_job": running.to_dict()})
        self.assertEqual(running, busy.maintenance_job)
        self.assertEqual(busy, MaterializationTable.from_dict(busy.to_dict()))
        self.assertIsNone(record.maintenance_job)
        self.assertIsNone(RunningMaintenance.from_dict({"job": "compaction"}), "a job without its start is none")
        with self.assertRaisesRegex(ValueError, "state must be one of"):
            MaterializationTable.from_dict({**record.to_dict(), "state": "archived"})

    def test_table_event_round_trip_and_kinds(self) -> None:
        event = TableEvent(
            event_id="evt-table-1",
            kind="table_committed",
            materialization_id="mat-123",
            table="orders",
            occurred_at="2026-07-12T00:01:00Z",
            runtime_app_ref="kaspr-mat-123",
            member_id="member-0",
            payload={"snapshot_id": 42},
        )

        self.assertEqual(TableEvent.from_dict(event.to_dict()), event)
        started = TableEvent.from_dict({**event.to_dict(), "kind": "maintenance_started", "runtime_app_ref": "materializer-maintenance"})
        self.assertEqual("maintenance_started", started.kind)
        with self.assertRaisesRegex(ValueError, "kind must be one of"):
            TableEvent(event_id="e", kind="schema_proposed", materialization_id="mat-123", table="orders", occurred_at="2026-07-12T00:01:00Z")
        with self.assertRaisesRegex(ValueError, "table is required"):
            TableEvent.from_dict({**event.to_dict(), "table": ""})

    def test_request_event_round_trip(self) -> None:
        event = MaterializationRequestEvent(
            request_id="req-1",
            materialization_id="mat-123",
            action="create",
            submitted_at="2026-07-12T00:00:00Z",
            submitted_by="vmirzoyan",
            spec=MaterializationRequestSpec(
                name="orders-materialization",
                tables=(TableSpec(name="orders", source_topic="orders", dataset="shop", dataset_id="ds-000000000001"), TableSpec(name="payments", source_topic="payments", dataset="shop", dataset_id="ds-000000000001")),
                destination_uri="abfs://lakehouse/orders",
                runtime_mode="dedicated",
                namespace="midstream",
                resource_profile="small",
                labels={"domain": "orders"},
            ),
        )

        self.assertEqual(MaterializationRequestEvent.from_dict(event.to_dict()), event)
        claimed = replace(event, claim_request_id="idr-claim-req-1", label="Orders")
        self.assertEqual(MaterializationRequestEvent.from_dict(claimed.to_dict()), claimed)
        self.assertNotIn("claim_request_id", event.to_dict(), "left out when unset")
        self.assertNotIn("label", event.to_dict())
        # An update before 4.3 could switch a live tail; every materialization has one now.
        from_4_1 = {**replace(event, action="update").to_dict(), "live_tail": False}
        self.assertEqual(MaterializationRequestEvent.from_dict(from_4_1), replace(event, action="update"))

    def test_a_request_event_round_trips_its_change_of_topics(self) -> None:
        change = TopicChangeRequest(source_topics=("orders", "refunds"), table_actions={"payments": "keep"})
        event = MaterializationRequestEvent(
            request_id="req-2",
            materialization_id="mat-123",
            action="update",
            submitted_at="2026-07-12T00:00:00Z",
            submitted_by="vmirzoyan",
            spec=MaterializationRequestSpec(
                name="shop",
                tables=(TableSpec(name="orders", source_topic="orders", dataset="shop", dataset_id="ds-000000000001"),),
                destination_uri="gs://lake",
                runtime_mode="dedicated",
                namespace="midstream",
            ),
            topic_change=change,
        )

        self.assertEqual(MaterializationRequestEvent.from_dict(event.to_dict()), event)
        self.assertEqual(change, TopicChangeRequest.from_dict(change.to_dict()))
        refunds = TableSpec(name="refunds", source_topic="refunds", dataset="finance", dataset_id="ds-000000000002")
        adding = TopicChangeRequest(source_topics=("orders", "refunds"), new_tables=(refunds,))
        self.assertEqual(adding, TopicChangeRequest.from_dict(adding.to_dict()))
        self.assertEqual(refunds, adding.new_table("refunds"))
        self.assertIsNone(adding.new_table("orders"))
        with self.assertRaisesRegex(ValueError, "must be one of: drop, keep"):
            TopicChangeRequest(source_topics=("orders",), table_actions={"payments": "archive"})
        with self.assertRaisesRegex(ValueError, "at least one topic"):
            TopicChangeRequest(source_topics=())

    def test_work_event_round_trip(self) -> None:
        event = MaterializationWorkEvent(
            work_id="work-1",
            materialization_id="mat-123",
            step="observe_runtime",
            requested_at="2026-07-12T00:00:00Z",
            caused_by=EventCause(kind="scheduler", id="observe:mat-123"),
            dedupe_key="observe:mat-123:op-1:1",
            target_runtime=TargetRuntime(
                namespace="midstream",
                kaspr_app_name="kaspr-mat-123",
            ),
            observation_policy=ObservationPolicy(
                attempt=1,
                max_attempts=4,
                scheduled_for="2026-07-12T00:00:15Z",
                mode="one_shot",
            ),
        )

        self.assertEqual(MaterializationWorkEvent.from_dict(event.to_dict()), event)

    def test_a_claim_answer_round_trips_on_its_work_event(self) -> None:
        claim = TableClaimResult(
            request_id="idr-claim-req-1",
            purpose="create",
            status="rejected",
            reason="table_name_taken",
            message="sales.orders is taken",
            details={"index": 0, "dataset": "sales", "table": "orders"},
        )
        event = MaterializationWorkEvent(
            work_id="work-claim-idr-claim-req-1",
            materialization_id="mat-123",
            step="claim_rejected",
            requested_at="2026-10-22T00:00:00Z",
            caused_by=EventCause(kind="request", id="req-1"),
            dedupe_key="claim_rejected:mat-123:idr-claim-req-1",
            claim=claim,
        )

        self.assertEqual(MaterializationWorkEvent.from_dict(event.to_dict()), event)
        self.assertEqual(TableClaimResult.from_dict(claim.to_dict()), claim)
        self.assertFalse(claim.applied)
        self.assertTrue(replace(claim, status="applied", reason=None).applied)
        for step in ("tables_claimed", "claim_rejected"):
            with self.subTest(step=step), self.assertRaisesRegex(ValueError, "requires claim"):
                replace(event, step=step, claim=None)
        with self.assertRaisesRegex(ValueError, "purpose"):
            replace(claim, purpose="delete")
        with self.assertRaisesRegex(ValueError, "status"):
            replace(claim, status="pending")

    def test_a_consumer_lag_record_carries_the_measurement_it_records(self) -> None:
        event = MaterializationWorkEvent(
            work_id="work-record-consumer-lag-mat-123-2026-07-12T00:10:00Z",
            materialization_id="mat-123",
            step="record_consumer_lag",
            requested_at="2026-07-12T00:10:00Z",
            caused_by=EventCause(kind="scheduler", id="refresh-consumer-lag"),
            dedupe_key="record_consumer_lag:mat-123:2026-07-12T00:10:00Z",
            consumer_lag=ConsumerLag(
                total_lag=7,
                topics={"orders": TopicLag(total_lag=7, partition_lag={"0": 7})},
                measured_at="2026-07-12T00:10:00Z",
            ),
        )

        self.assertEqual(MaterializationWorkEvent.from_dict(event.to_dict()), event)
        with self.assertRaisesRegex(ValueError, "requires consumer_lag"):
            MaterializationWorkEvent.from_dict({**event.to_dict(), "consumer_lag": None})

    def test_status_round_trip(self) -> None:
        status = MaterializationStatus(
            materialization_id="mat-123",
            desired_phase="running",
            observed_phase="deploying",
            phase_reason="waiting_for_runtime_ready",
            phase_message="KasprApp exists and is waiting for healthy runtime signals.",
            last_transition_time="2026-07-12T00:00:00Z",
            last_heartbeat="2026-07-12T00:00:00Z",
            last_resource_observation_time="2026-07-12T00:00:15Z",
            observation_source="hybrid",
            runtime_health=RuntimeHealth(
                level="unknown",
                reason=None,
                message=None,
            ),
            runtime_progress=RuntimeProgress(
                rows_written=1200,
                bytes_written=1048576,
                files_written=3,
                records_skipped=4,
                latest_offsets={"orders": {"0": 1024}, "payments": {"0": 7}},
            ),
            resource_observation=ResourceObservation(
                namespace="midstream",
                kaspr_app_name="kaspr-materializer-orders",
                resource_exists=True,
                metadata_generation=4,
                observed_generation=4,
                ready_condition=ResourceCondition(
                    status="True",
                    reason="Healthy",
                    message="App is ready",
                    last_transition_time="2026-07-12T00:00:10Z",
                ),
                progressing_condition=ResourceCondition(
                    status="False",
                    reason="ReconcileComplete",
                    message="All resources are in desired state",
                    last_transition_time="2026-07-12T00:00:10Z",
                ),
                rollout_in_progress=False,
                available_members="2/2",
                rebalance_required=False,
                rebalancing_members="0/2",
                managed_agent_states={"writer": "running", "committer": "running"},
                managed_agents_total=2,
                managed_agents_running=2,
                managed_agents_paused=0,
                managed_agents_unknown=0,
            ),
            last_error=None,
            status_version=1,
        )

        self.assertEqual(MaterializationStatus.from_dict(status.to_dict()), status)

    def test_runtime_binding_round_trip(self) -> None:
        binding = RuntimeBinding(
            materialization_id="mat-123",
            runtime_mode="dedicated",
            kaspr_app_name="kaspr-materializer-orders",
            namespace="midstream",
            managed_agent_names=["writer", "committer"],
            template_version="v1",
            last_applied_spec_hash="abc123",
            last_apply_operation_id="op-1",
            source_topics=["orders", "payments"],
            destination_uri="abfs://lakehouse/orders",
            created_at="2026-07-12T00:00:00Z",
            updated_at="2026-07-12T00:00:00Z",
        )

        self.assertEqual(RuntimeBinding.from_dict(binding.to_dict()), binding)
        self.assertFalse(binding.draining)
        from dataclasses import replace

        draining = replace(binding, drain_started_at="2026-07-12T01:00:00Z", topics_before_drain=["orders"])
        self.assertEqual(RuntimeBinding.from_dict(draining.to_dict()), draining)
        self.assertTrue(draining.draining)

    def test_runtime_status_event_round_trip(self) -> None:
        event = RuntimeStatusEvent(
            event_id="evt-1",
            materialization_id="mat-123",
            runtime_app_ref="kaspr-materializer-orders",
            member_id="member-0",
            agent_name="writer",
            phase="running",
            health="healthy",
            schema_versions={"orders": 2, "payments": 0},
            rows_written=500,
            bytes_written=1024,
            files_written=1,
            records_skipped=7,
            latest_offsets={"orders": {"0": 42}, "payments": {"0": 3, "1": 9}},
            table_counters={"orders": {"rows_written": 400}, "payments": {"rows_written": 100}},
            event_time="2026-07-12T00:00:00Z",
            phase_reason="processing",
            phase_message="Runtime is actively processing source records.",
            reported_at="2026-07-12T00:00:01Z",
        )

        self.assertEqual(RuntimeStatusEvent.from_dict(event.to_dict()), event)

    def test_runtime_status_event_round_trips_the_settings_revision(self) -> None:
        event = RuntimeStatusEvent(
            event_id="evt-1",
            materialization_id="mat-123",
            runtime_app_ref="kaspr-mat-123",
            member_id="member-0",
            agent_name="writer",
            phase="running",
            health="healthy",
            reported_at="2026-07-12T00:00:01Z",
            settings_revision=3,
        )
        self.assertEqual(RuntimeStatusEvent.from_dict(event.to_dict()), event)
        self.assertEqual(event.to_dict()["settings_revision"], 3)
        older = {key: value for key, value in event.to_dict().items() if key != "settings_revision"}
        self.assertIsNone(RuntimeStatusEvent.from_dict(older).settings_revision)
        self.assertIsNone(RuntimeStatusEvent.from_dict({**older, "settings_revision": "3"}).settings_revision)
        member = MemberProgress(reported_at="2026-07-12T00:00:01Z", settings_revision=3)
        self.assertEqual(MemberProgress.from_dict(member.to_dict()), member)

    def test_runtime_status_event_round_trips_the_settings_lineage(self) -> None:
        event = RuntimeStatusEvent(
            event_id="evt-1",
            materialization_id="mat-123",
            runtime_app_ref="kaspr-mat-123",
            member_id="member-0",
            agent_name="writer",
            phase="running",
            health="healthy",
            reported_at="2026-07-12T00:00:01Z",
            settings_revision=0,
            settings_lineage="20261024T100000Z-4e5f6a7b",
        )
        self.assertEqual(RuntimeStatusEvent.from_dict(event.to_dict()), event)
        # "" (settings without a lineage) is sent and kept apart from None (a runtime before lineages).
        unnamed = RuntimeStatusEvent.from_dict({**event.to_dict(), "settings_lineage": ""})
        self.assertEqual(unnamed.to_dict()["settings_lineage"], "")
        older = {key: value for key, value in event.to_dict().items() if key != "settings_lineage"}
        self.assertIsNone(RuntimeStatusEvent.from_dict(older).settings_lineage)
        self.assertIsNone(RuntimeStatusEvent.from_dict({**older, "settings_lineage": 7}).settings_lineage)
        for lineage in ("20261024T100000Z-4e5f6a7b", "", None):
            with self.subTest(lineage=lineage):
                member = MemberProgress(reported_at="2026-07-12T00:00:01Z", settings_revision=0, settings_lineage=lineage)
                self.assertEqual(MemberProgress.from_dict(member.to_dict()), member)

    def test_settings_models_round_trip(self) -> None:
        document = SettingsDocument(
            scope="control-plane",
            revision=2,
            values={"max_runtime_replicas": SettingOverride(value=8, updated_at="2026-10-08T10:00:00Z", updated_by="jane")},
            updated_at="2026-10-08T10:00:00Z",
            updated_by="jane",
            recent_request_ids=["set-1", "set-2"],
            last_rejection=SettingsRejection(
                request_id="set-3", reason="settings_changed", message="changed", rejected_at="2026-10-08T10:01:00Z"
            ),
        )
        self.assertEqual(SettingsDocument.from_dict(document.to_dict()), document)
        runtime = SettingsDocument(scope="runtime", revision=3, lineage="20261024T100000Z-4e5f6a7b", lineage_revision=2)
        self.assertEqual(SettingsDocument.from_dict(runtime.to_dict()), runtime)
        # Stored before lineages: none until the next start gives it one.
        stored_before = {key: value for key, value in runtime.to_dict().items() if key not in ("lineage", "lineage_revision")}
        self.assertIsNone(SettingsDocument.from_dict(stored_before).lineage)
        self.assertIsNone(SettingsDocument.from_dict(stored_before).lineage_revision)
        self.assertIsNone(SettingsDocument.from_dict({**stored_before, "lineage": ""}).lineage)
        # A lineage cannot have begun after the scope's own revision; one read back that says so is dropped.
        for lineage_revision in (4, -1, "2", True):
            with self.subTest(lineage_revision=lineage_revision):
                self.assertIsNone(SettingsDocument.from_dict({**runtime.to_dict(), "lineage_revision": lineage_revision}).lineage_revision)
        with self.assertRaises(ValueError):
            SettingsDocument(scope="runtime", revision=3, lineage="20261024T100000Z-4e5f6a7b", lineage_revision=4)
        bootstrap = SettingsBootstrapRequest(requested_at="2026-10-24T10:00:00Z")
        self.assertEqual(bootstrap.to_dict(), {"kind": "bootstrap", "scope": "runtime", "requested_at": "2026-10-24T10:00:00Z"})
        self.assertEqual(SettingsBootstrapRequest.from_dict(bootstrap.to_dict()), bootstrap)
        self.assertTrue(SettingsBootstrapRequest.matches(bootstrap.to_dict()))
        request = SettingsChangeRequest(
            request_id="set-4",
            scope="console",
            submitted_at="2026-10-08T10:00:00Z",
            submitted_by="jane",
            values={"pageRowLimit": 500},
            reset=["logLevel"],
            base_revision=2,
            fallbacks={"logLevel": SettingFallback(value="info", source="deployment")},
        )
        self.assertEqual(SettingsChangeRequest.from_dict(request.to_dict()), request)
        record = SettingsChangeRecord(
            scope="runtime",
            revision=1,
            request_id="set-5",
            updated_at="2026-10-08T10:00:00Z",
            updated_by="jane",
            changes=[SettingChange(key="min_rows_per_file", before=10000, after=500, before_source="default", after_source="override")],
        )
        self.assertEqual(SettingsChangeRecord.from_dict(record.to_dict()), record)

    def test_settings_models_validate(self) -> None:
        with self.assertRaises(ValueError):
            SettingsDocument(scope="secrets")
        with self.assertRaises(ValueError):
            SettingsChangeRequest(request_id="set-1", scope="console", submitted_at="t", submitted_by="jane")
        with self.assertRaises(ValueError):
            SettingsChangeRequest(
                request_id="set-1", scope="console", submitted_at="t", submitted_by="jane",
                values={"a": 1}, reset=["a"],
            )
        with self.assertRaises(ValueError):
            SettingChange(key="a", before=1, after=2, before_source="guess", after_source="override")
        with self.assertRaises(ValueError):
            SettingFallback(value=1, source="override")

    def test_runtime_status_event_round_trip_without_schema_versions(self) -> None:
        event = RuntimeStatusEvent(
            event_id="evt-2",
            materialization_id="mat-123",
            runtime_app_ref="kaspr-materializer-orders",
            member_id="member-0",
            agent_name="writer",
            phase="running",
            health="unknown",
            rows_written=0,
            bytes_written=0,
            files_written=0,
            records_skipped=0,
            latest_offsets={},
            event_time="2026-07-12T00:00:00Z",
            phase_reason="waiting_for_table",
            reported_at="2026-07-12T00:00:01Z",
        )

        self.assertEqual(RuntimeStatusEvent.from_dict(event.to_dict()), event)

    def test_runtime_status_event_round_trips_the_table_block(self) -> None:
        event = RuntimeStatusEvent(
            event_id="evt-3",
            materialization_id="mat-123",
            runtime_app_ref="kaspr-materializer-orders",
            member_id="member-0",
            agent_name="writer",
            phase="running",
            health="healthy",
            rows_written=3,
            bytes_written=100,
            files_written=1,
            records_skipped=0,
            latest_offsets={"orders": {"0": 2}},
            event_time="2026-07-12T00:00:00Z",
            reported_at="2026-07-12T00:00:01Z",
            unmapped_values=4,
            files_pending_commit=2,
            tables={
                "orders": {"current_snapshot_id": 7, "total_records": 3},
                "payments": {"current_snapshot_id": None, "total_records": 0},
            },
        )

        self.assertEqual(RuntimeStatusEvent.from_dict(event.to_dict()), event)

    def test_runtime_status_event_rejects_flat_offsets(self) -> None:
        with self.assertRaisesRegex(ValueError, "latest_offsets"):
            RuntimeStatusEvent(
                event_id="evt-4",
                materialization_id="mat-123",
                runtime_app_ref="kaspr-mat-123",
                member_id="member-0",
                agent_name="writer",
                phase="running",
                health="healthy",
                reported_at="2026-07-12T00:00:01Z",
                latest_offsets={"0": 2},
            )

    def test_consumer_lag_round_trips_per_topic(self) -> None:
        lag = ConsumerLag(
            total_lag=12,
            topics={
                "orders": TopicLag(total_lag=10, partition_lag={"0": 4, "1": 6}, end_offsets={"0": 100, "1": 200}, partitions=2),
                "payments": TopicLag(total_lag=2, partition_lag={"0": 2}, end_offsets={"0": 9}, partitions=1),
            },
            measured_at="2026-07-12T00:00:00Z",
        )
        self.assertEqual(ConsumerLag.from_dict(lag.to_dict()), lag)

    def test_record_envelope_round_trip(self) -> None:
        envelope = RecordEnvelope(
            entity_type="materialization",
            entity_id="mat-123",
            version=3,
            updated_at="2026-07-12T00:00:00Z",
            payload={"desired_phase": "running"},
        )

        self.assertEqual(RecordEnvelope.from_dict(envelope.to_dict()), envelope)


class ModelValidationTests(unittest.TestCase):
    def test_status_rejects_invalid_phase(self) -> None:
        with self.assertRaisesRegex(ValueError, "desired_phase must be one of"):
            MaterializationStatus(
                materialization_id="mat-123",
                desired_phase="invalid",
                observed_phase="requested",
                phase_reason="requested",
                last_transition_time="2026-07-12T00:00:00Z",
                observation_source="hybrid",
                runtime_health=RuntimeHealth(level="unknown"),
                resource_observation=ResourceObservation(
                    namespace="midstream",
                    kaspr_app_name="kaspr-materializer-orders",
                    resource_exists=False,
                    managed_agent_states={},
                ),
            )

    def test_materialization_round_trips_skip_tombstones_and_defaults_it_for_older_records(self) -> None:
        materialization = Materialization(
            materialization_id="mat-123",
            namespace="midstream",
            tables=(TableSpec(name="orders", source_topic="orders", dataset="shop", dataset_id="ds-000000000001"),),
            destination_uri="gs://lakehouse/orders",
            desired_phase="running",
            created_at="2026-07-12T00:00:00Z",
            updated_at="2026-07-12T00:00:00Z",
            skip_tombstones=True,
        )

        self.assertEqual(Materialization.from_dict(materialization.to_dict()), materialization)
        stored_before_the_setting = materialization.to_dict()
        del stored_before_the_setting["skip_tombstones"]
        self.assertFalse(Materialization.from_dict(stored_before_the_setting).skip_tombstones)

    def test_a_4_1_record_of_a_live_tail_switch_reads_without_it(self) -> None:
        # 4.1 and 4.2 let a materialization switch its live tail off; now every one has it.
        materialization = Materialization(
            materialization_id="mat-123",
            namespace="midstream",
            tables=(TableSpec(name="orders", source_topic="orders", dataset="shop", dataset_id="ds-000000000001"),),
            destination_uri="gs://lakehouse/orders",
            desired_phase="running",
            created_at="2026-07-12T00:00:00Z",
            updated_at="2026-07-12T00:00:00Z",
        )
        self.assertNotIn("live_tail", materialization.to_dict())
        for value in (True, False):
            with self.subTest(live_tail=value):
                self.assertEqual(Materialization.from_dict({**materialization.to_dict(), "live_tail": value}), materialization)
        spec = MaterializationRequestSpec(
            tables=(TableSpec(name="orders", source_topic="orders", dataset="shop", dataset_id="ds-000000000001"),),
            destination_uri="gs://lakehouse/orders",
        )
        self.assertNotIn("live_tail", spec.to_dict())
        self.assertEqual(MaterializationRequestSpec.from_dict({**spec.to_dict(), "live_tail": False}), spec)

    def test_materialization_rejects_non_boolean_skip_tombstones(self) -> None:
        with self.assertRaisesRegex(ValueError, "skip_tombstones must be a boolean"):
            Materialization(
                materialization_id="mat-123",
                namespace="midstream",
                tables=(TableSpec(name="orders", source_topic="orders", dataset="shop", dataset_id="ds-000000000001"),),
                destination_uri="gs://lakehouse/orders",
                desired_phase="running",
                created_at="2026-07-12T00:00:00Z",
                updated_at="2026-07-12T00:00:00Z",
                skip_tombstones="yes",
            )

    def test_request_spec_round_trips_skip_tombstones_and_defaults_it_for_older_events(self) -> None:
        spec = MaterializationRequestSpec(
            tables=(TableSpec(name="orders", source_topic="orders", dataset="shop", dataset_id="ds-000000000001"),),
            destination_uri="gs://lakehouse/orders",
            skip_tombstones=True,
        )

        self.assertEqual(MaterializationRequestSpec.from_dict(spec.to_dict()), spec)
        emitted_before_the_setting = spec.to_dict()
        del emitted_before_the_setting["skip_tombstones"]
        self.assertFalse(MaterializationRequestSpec.from_dict(emitted_before_the_setting).skip_tombstones)

    def test_request_spec_rejects_unknown_runtime_mode(self) -> None:
        with self.assertRaisesRegex(ValueError, "runtime_mode must be one of"):
            MaterializationRequestSpec(
                tables=(TableSpec(name="orders", source_topic="orders", dataset="shop", dataset_id="ds-000000000001"),),
                destination_uri="abfs://lakehouse/orders",
                runtime_mode="experimental",
            )


if __name__ == "__main__":
    unittest.main()