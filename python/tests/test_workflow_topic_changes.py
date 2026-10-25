"""Changing a running materialization's topics: new tables, the drain, offsets, and the drops."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))

from google.api_core.exceptions import ServiceUnavailable  # noqa: E402

from test_workflow import (  # noqa: E402
    LOCATION,
    MATERIALIZATION_ID,
    PAYMENTS_LOCATION,
    SAMPLE,
    WorkflowTestCase,
    work,
)

from materializer_control_plane.handlers import handle_work_event_for_routed_output  # noqa: E402
from materializer_control_plane.kafka.metadata import TopicMetadata  # noqa: E402
from materializer_control_plane.lakehouse.pointer import POINTER_NAME  # noqa: E402
from materializer_control_plane.models import (  # noqa: E402
    EventCause,
    IdentityRequestOutcome,
    TableClaimResult,
    MaterializationRequestEvent,
    MaterializationRequestSpec,
    MaterializationWorkEvent,
    TopicChangeRequest,
)
from materializer_control_plane.services.table_claims import claim_outcome_work  # noqa: E402
from materializer_control_plane.table_names import table_name_for_topic  # noqa: E402
from table_records import table_spec  # noqa: E402
from materializer_control_plane.schema import StubDiscoveryService  # noqa: E402
from materializer_control_plane.services import process_request_event  # noqa: E402
from materializer_control_plane.services.observation import apply_runtime_status_event, build_sweep_observation_outputs  # noqa: E402
from materializer_control_plane.services.workflow import (  # noqa: E402
    DRAIN_MAX_ATTEMPTS,
    MAX_DESTINATION_CLEANUP_RETRY_ATTEMPTS,
    is_live_drop,
)
from materializer_control_plane.storage import (  # noqa: E402
    get_materialization,
    get_materialization_schema,
    get_materialization_status,
    get_materialization_table,
    get_runtime_binding,
    list_materialization_tables,
    list_schema_versions,
    save_materialization,
    save_materialization_table,
)

REFUNDS_LOCATION = LOCATION.rsplit("/", 1)[0] + "/refunds"
GROUP = "kaspr-mat-a1b2c3d4e5f6-app"


class FakeGroupAdmin:
    """The consumer group as the drain sees it: its state, and the offsets committed for it."""

    def __init__(self, state: str = "Empty") -> None:
        self.state = state
        self.committed: list[tuple[str, dict[str, dict[int, int]]]] = []

    def consumer_group_state(self, group_id: str) -> str:
        return self.state

    def commit_group_offsets(self, group_id: str, offsets: dict[str, dict[int, int]]) -> None:
        self.committed.append((group_id, offsets))


class FakeMetadata:
    def __init__(self, beginning: dict[str, dict[int, int]]) -> None:
        self.beginning = beginning
        self.described: list[list[str]] = []

    def describe_topics(self, topics: list[str]) -> dict[str, TopicMetadata]:
        self.described.append(list(topics))
        return {
            topic: TopicMetadata(
                name=topic,
                exists=topic in self.beginning,
                partitions=len(self.beginning.get(topic, {})),
                beginning_offsets=dict(self.beginning.get(topic, {})),
            )
            for topic in topics
        }


class TopicChangeTestCase(WorkflowTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.two_topics(orders=SAMPLE, payments=[{"payment_id": "P-1"}], refunds=[{"refund_id": "R-1"}])
        event = work("start_discovery")
        while event.step != "observe_runtime":
            event = self.process(event)
        self.group = FakeGroupAdmin()
        self.metadata = FakeMetadata({"refunds": {0: 5, 1: 0}, "payments": {0: 0, 1: 3}})
        self.kaspr.applied.clear()

    def process(self, event, **kwargs):
        kwargs.setdefault("kafka_admin_client", getattr(self, "group", None))
        kwargs.setdefault("metadata_client", getattr(self, "metadata", None))
        return super().process(event, **kwargs)

    def change(
        self,
        topics,
        actions=None,
        *,
        request_id: str = "req-topics",
        claim: str = "applied",
        names: dict[str, str] | None = None,
    ) -> MaterializationWorkEvent | None:
        """Accept and apply an update of the topics, as the view and request intake do, and - when it
        adds tables - answer their claim as the directory would (`claim`), returning what the
        workflow does next."""
        materialization = get_materialization(self.table, MATERIALIZATION_ID)
        existing = {spec.source_topic for spec in materialization.all_tables}
        chosen = names or {}
        new_tables = tuple(table_spec(chosen.get(topic) or table_name_for_topic(topic), topic) for topic in topics if topic not in existing)
        request = MaterializationRequestEvent(
            request_id=request_id,
            materialization_id=MATERIALIZATION_ID,
            action="update",
            submitted_at="2026-07-12T01:00:00Z",
            submitted_by="jane@example.com",
            spec=MaterializationRequestSpec(
                name=materialization.name,
                tables=materialization.tables,
                destination_uri=materialization.destination_uri,
                runtime_mode=materialization.runtime_mode,
                runtime_name=materialization.resolved_runtime_name,
                namespace=materialization.namespace,
                replicas=materialization.replicas,
            ),
            topic_change=TopicChangeRequest(source_topics=tuple(topics), table_actions=dict(actions or {}), new_tables=new_tables),
            claim_request_id=f"idr-{request_id}",
        )
        result = process_request_event(self.table, request, config=self.config)
        claims = [item for item in result.identity if item.action == "claim_tables"]
        if not claims:
            return result.work[0] if result.work else None
        (claim_request,) = claims
        outcome = IdentityRequestOutcome(
            request_id=claim_request.request_id,
            action="claim_tables",
            status=claim,
            completed_at="2026-07-12T01:00:01Z",
            actor_id=claim_request.actor_id,
            reason=None if claim == "applied" else "table_name_taken",
            message=None if claim == "applied" else "shop.refunds is taken",
        )
        answer = MaterializationWorkEvent.from_dict(claim_outcome_work(claim_request, outcome)["value"])
        return self.process(answer)

    def app_replicas(self) -> int:
        app = [resource for resource in self.kaspr.applied if resource["kind"] == "KasprApp"][-1]
        return app["spec"]["replicas"]

    def writer_topics(self) -> str:
        writer = [resource for resource in self.kaspr.applied if resource["metadata"]["name"].endswith("-writer")][-1]
        return writer["spec"]["input"]["topic"]["name"]

    def until(self, event, step: str):
        """Process events, the first of several each time, until one for `step` comes up."""
        while event is not None and event.step != step:
            follow_up = self.process(event)
            event = follow_up[0] if isinstance(follow_up, tuple) else follow_up
        return event

    def states(self) -> dict[str, str]:
        return {record.name: record.state for record in list_materialization_tables(self.table, MATERIALIZATION_ID)}


class ClaimedTopicChangeTests(TopicChangeTestCase):
    """An update that adds topics waits for their tables' names: refused, it is not applied at all."""

    def test_a_refused_name_leaves_the_materialization_as_it_was(self) -> None:
        self.assertIsNone(self.change(["orders", "refunds"], {"payments": "drop"}, claim="rejected"))

        materialization = get_materialization(self.table, MATERIALIZATION_ID)
        self.assertEqual(("orders", "payments"), materialization.table_names)
        self.assertIsNone(materialization.pending_topic_change)
        self.assertEqual({"orders": "active", "payments": "active"}, self.states(), "nothing is dropped either")
        self.assertEqual("The change of topics was not applied: shop.refunds is taken", get_materialization_status(self.table, MATERIALIZATION_ID).phase_message)
        self.assertEqual([], self.kaspr.applied)

    def test_an_answer_to_a_claim_no_longer_waited_for_changes_nothing(self) -> None:
        self.change(["orders", "payments", "refunds"])
        stale = MaterializationWorkEvent(
            work_id="work-claim-idr-old",
            materialization_id=MATERIALIZATION_ID,
            step="tables_claimed",
            requested_at="2026-07-12T01:00:02Z",
            caused_by=EventCause(kind="request", id="req-old"),
            dedupe_key="tables_claimed:old",
            claim=TableClaimResult(request_id="idr-old", purpose="update", status="applied"),
        )

        self.assertIsNone(self.process(stale))
        self.assertEqual(("orders", "payments", "refunds"), get_materialization(self.table, MATERIALIZATION_ID).table_names)


class AddingTopicsTests(TopicChangeTestCase):
    def test_an_added_topic_is_discovered_created_and_read_after_the_drain(self) -> None:
        event = self.change(["orders", "payments", "refunds"])
        self.assertEqual("discover_tables", event.step)

        sampled = []
        discover = self.discovery.discover
        self.discovery.discover = lambda materialization, **budget: sampled.append(materialization.source_topics) or discover(materialization, **budget)

        create = self.process(event)

        self.assertEqual("create_table", create.step)
        self.assertEqual([("refunds",)], sampled, "only the new topic is sampled")
        refunds = {field["name"] for field in get_materialization_schema(self.table, MATERIALIZATION_ID, "refunds").fields}
        self.assertIn("refund_id", refunds)

        apply = self.until(create, "apply_runtime")
        self.assertEqual(REFUNDS_LOCATION, get_materialization_table(self.table, MATERIALIZATION_ID, "refunds").location)

        wait = self.process(apply)

        self.assertEqual("await_runtime_drained", wait.step)
        self.assertEqual(0, self.app_replicas(), "held at zero until the group empties")
        self.assertEqual("orders,payments,refunds", self.writer_topics())
        binding = get_runtime_binding(self.table, MATERIALIZATION_ID)
        self.assertTrue(binding.draining)
        self.assertEqual(["orders", "payments"], binding.topics_before_drain)
        self.assertEqual(("deploying", "changing_topics"), (self.status().observed_phase, self.status().phase_reason))
        self.assertIsNotNone(wait.observation_policy.scheduled_for)

        final = self.process(wait)

        self.assertEqual([(GROUP, {"refunds": {0: 5, 1: 0}})], self.group.committed)
        self.assertEqual("apply_runtime", final.step)
        self.assertFalse(get_runtime_binding(self.table, MATERIALIZATION_ID).draining)

        observe = self.process(final)

        self.assertEqual("observe_runtime", observe.step)
        self.assertEqual(1, self.app_replicas())
        self.assertEqual("orders,payments,refunds", self.writer_topics())
        self.assertEqual(["orders", "payments", "refunds"], get_runtime_binding(self.table, MATERIALIZATION_ID).source_topics)

    def test_the_awaited_drain_matches_the_blocking_one(self) -> None:
        import asyncio

        from materializer_control_plane.services import process_work_event_async

        wait = self.until(self.change(["orders", "payments", "refunds"]), "await_runtime_drained")

        final = asyncio.run(
            process_work_event_async(
                self.table,
                wait,
                self.discovery,
                kaspr_app_client=self.kaspr,
                kafka_admin_client=self.group,
                object_store_client=self.client,
                config=self.config,
                metadata_client=self.metadata,
            )
        )

        self.assertEqual("apply_runtime", final.step)
        self.assertEqual([(GROUP, {"refunds": {0: 5, 1: 0}})], self.group.committed)

    def test_the_drain_waits_for_the_group_to_empty(self) -> None:
        wait = self.until(self.change(["orders", "payments", "refunds"]), "await_runtime_drained")
        self.group.state = "Stable"

        again = self.process(wait)

        self.assertEqual(("await_runtime_drained", 2), (again.step, again.observation_policy.attempt))
        self.assertEqual([], self.group.committed)
        self.assertIn("waiting for consumer group", self.status().phase_message)
        self.group.state = "Empty"
        self.assertEqual("apply_runtime", self.process(again).step)

    def test_a_group_that_never_empties_fails_the_change(self) -> None:
        wait = self.until(self.change(["orders", "payments", "refunds"]), "await_runtime_drained")
        self.group.state = "Stable"
        wait = replace(wait, observation_policy=replace(wait.observation_policy, attempt=DRAIN_MAX_ATTEMPTS))

        self.assertIsNone(self.process(wait))

        status = self.status()
        self.assertEqual(("failed", "consumer_group_not_empty"), (status.observed_phase, status.phase_reason))
        self.assertIn(GROUP, status.phase_message)
        # Still draining: resuming applies the runtime at zero again and waits again.
        self.assertTrue(get_runtime_binding(self.table, MATERIALIZATION_ID).draining)
        self.assertEqual("await_runtime_drained", self.process(work("apply_runtime", work_id="work-req-resume")).step)

    def test_heartbeats_and_sweeps_leave_the_drain_alone(self) -> None:
        from materializer_control_plane.models import RuntimeStatusEvent

        self.until(self.change(["orders", "payments", "refunds"]), "await_runtime_drained")

        apply_runtime_status_event(
            self.table,
            RuntimeStatusEvent(
                event_id="evt-1",
                materialization_id=MATERIALIZATION_ID,
                runtime_app_ref="kaspr-mat-a1b2c3d4e5f6",
                member_id="m-0",
                agent_name="writer",
                phase="running",
                health="healthy",
                reported_at="2026-07-12T01:00:30Z",
            ),
        )
        outputs = build_sweep_observation_outputs(self.table, observed_at="2026-07-12T01:01:00Z")

        self.assertEqual(("deploying", "changing_topics"), (self.status().observed_phase, self.status().phase_reason))
        self.assertEqual([], [output for output in outputs if output["value"]["step"] == "apply_runtime"])


class RemovingTopicsTests(TopicChangeTestCase):
    def setUp(self) -> None:
        super().setUp()
        for location in (LOCATION, PAYMENTS_LOCATION):
            self.store.put(f"{location}/data/ts_day=2026-07-12/p00-o0000000000-m-0.parquet", b"rows")

    def finish(self, event) -> list[MaterializationWorkEvent]:
        """Run the chain to its end, every follow-up of every step; return the events in order."""
        events, pending = [], [event]
        while pending:
            current = pending.pop(0)
            events.append(current)
            if current.step == "observe_runtime":
                continue
            follow_up = self.process(current)
            pending.extend(follow_up if isinstance(follow_up, tuple) else [follow_up] if follow_up else [])
        return events

    def test_a_dropped_tables_files_and_records_go_once_the_runtime_reads_without_it(self) -> None:
        event = self.change(["orders"], {"payments": "drop"})
        self.assertEqual("apply_runtime", event.step)
        self.assertEqual("dropping", self.states()["payments"])

        events = self.finish(event)

        steps = [item.step for item in events]
        self.assertEqual(["apply_runtime", "await_runtime_drained", "apply_runtime", "observe_runtime", "drop_table"], steps)
        self.assertTrue(is_live_drop(events[-1]))
        self.assertEqual("orders", self.writer_topics())
        self.assertEqual([], self.group.committed, "no topic was added")
        self.assertEqual({"orders": "active"}, self.states())
        self.assertIsNone(get_materialization_schema(self.table, MATERIALIZATION_ID, "payments"))
        self.assertEqual([], list_schema_versions(self.table, MATERIALIZATION_ID, "payments"))
        self.assertEqual([], [name for name in self.store.names() if name.startswith(PAYMENTS_LOCATION)])
        self.assertIsNotNone(self.store.get(f"{LOCATION}/metadata/{POINTER_NAME}"))
        self.assertIsNotNone(get_materialization(self.table, MATERIALIZATION_ID), "a live drop purges nothing else")

    def test_with_cleanup_off_a_dropped_table_keeps_its_files_and_its_name(self) -> None:
        self.config = replace(self.config, destination_cleanup_enabled=False)

        self.finish(self.change(["orders"], {"payments": "drop"}))

        self.assertEqual({"orders": "active", "payments": "dropped"}, self.states())
        self.assertIsNotNone(self.store.get(f"{PAYMENTS_LOCATION}/data/ts_day=2026-07-12/p00-o0000000000-m-0.parquet"))
        self.assertIsNone(self.store.get(f"{PAYMENTS_LOCATION}/metadata/{POINTER_NAME}"))
        # Adding the topic back makes a new table, which cannot take the dropped one's name: its
        # location, under the materialization's, is never used again.
        self.assertIsNone(self.change(["orders", "payments"], request_id="req-taken"))
        self.assertEqual(("orders",), get_materialization(self.table, MATERIALIZATION_ID).table_names)
        self.assertIn("already has a table named payments", get_materialization_status(self.table, MATERIALIZATION_ID).phase_message)
        self.change(["orders", "payments"], request_id="req-again", names={"payments": "payments_2"})
        self.assertEqual(("orders", "payments_2"), get_materialization(self.table, MATERIALIZATION_ID).table_names)

    def test_a_kept_table_stays_readable_and_resumes_where_it_stopped(self) -> None:
        payments = get_materialization_table(self.table, MATERIALIZATION_ID, "payments")
        save_materialization_table(self.table, replace(payments, offsets={"0": 41}))

        self.finish(self.change(["orders"], {"payments": "keep"}))

        self.assertEqual({"orders": "active", "payments": "retired"}, self.states())
        self.assertIsNotNone(self.store.get(f"{PAYMENTS_LOCATION}/metadata/{POINTER_NAME}"))
        self.assertEqual("orders", self.writer_topics())

        self.finish(self.change(["orders", "payments"], request_id="req-resume"))

        self.assertEqual({"orders": "active", "payments": "active"}, self.states())
        self.assertEqual("orders,payments", self.writer_topics())
        # Partition 0 after the table's last offset; partition 1, never committed, from its start.
        self.assertEqual([(GROUP, {"payments": {0: 42, 1: 3}})], self.group.committed)

    def test_dropping_a_kept_table_needs_no_runtime_change(self) -> None:
        self.finish(self.change(["orders"], {"payments": "keep"}))
        self.kaspr.applied.clear()

        events = self.finish(self.change(["orders"], {"payments": "drop"}, request_id="req-drop"))

        self.assertEqual(["drop_table"], [event.step for event in events])
        self.assertEqual([], self.kaspr.applied)
        self.assertEqual({"orders": "active"}, self.states())

    def test_a_table_kept_by_a_change_still_draining_is_dropped_once_the_drain_is_over(self) -> None:
        """Kept a moment ago, the table may still be written by members that have not left yet."""
        wait = self.until(self.change(["orders"], {"payments": "keep"}), "await_runtime_drained")

        drop = self.change(["orders"], {"payments": "drop"}, request_id="req-drop")

        self.assertEqual("apply_runtime", drop.step)
        self.assertIsNotNone(self.store.get(f"{PAYMENTS_LOCATION}/metadata/{POINTER_NAME}"))
        self.assertEqual("await_runtime_drained", self.process(drop).step, "it waits on the same drain")
        steps = [item.step for item in self.finish(wait)]
        self.assertEqual(["await_runtime_drained", "apply_runtime", "observe_runtime", "drop_table"], steps)
        self.assertEqual({"orders": "active"}, self.states())
        self.assertEqual([], [name for name in self.store.names() if name.startswith(PAYMENTS_LOCATION)])

    def test_a_drop_that_keeps_failing_leaves_the_table_dropping_with_its_error(self) -> None:
        self.finish(self.change(["orders"], {"payments": "keep"}))
        drop = self.change(["orders"], {"payments": "drop"}, request_id="req-drop")
        self.store.fail = lambda operation, bucket, name: ServiceUnavailable("GCS is down") if operation == "list_blobs" else None

        event, attempts = drop, 0
        while event is not None:
            event = self.process(event)
            attempts += 1

        self.assertEqual(MAX_DESTINATION_CLEANUP_RETRY_ATTEMPTS + 1, attempts)
        record = get_materialization_table(self.table, MATERIALIZATION_ID, "payments")
        self.assertEqual(("dropping", "drop_failed"), (record.state, record.last_error.reason))
        self.assertNotEqual("failed", self.status().observed_phase, "the materialization goes on running")

        # The next apply - a resume, say - starts the drop again.
        self.store.fail = None
        follow_up = self.process(work("apply_runtime", work_id="work-req-resume"))
        self.assertEqual(["observe_runtime", "drop_table"], [item.step for item in follow_up])
        self.assertIsNone(self.process(follow_up[1]))
        self.assertEqual({"orders": "active"}, self.states())

    def test_a_drop_waits_for_the_maintenance_job_running_on_the_table(self) -> None:
        from materializer_control_plane.models import RunningMaintenance
        from materializer_control_plane.storage import get_operation_record

        self.finish(self.change(["orders"], {"payments": "keep"}))
        drop = self.change(["orders"], {"payments": "drop"}, request_id="req-drop")
        record = get_materialization_table(self.table, MATERIALIZATION_ID, "payments")
        running = RunningMaintenance(job="compaction", started_at="2026-07-12T00:59:00Z", member_id="maintenance-0")
        save_materialization_table(self.table, replace(record, maintenance_job=running))

        wait = self.process(drop)

        self.assertEqual(("drop_table", drop.work_id), (wait.step, wait.work_id))
        self.assertIsNotNone(wait.observation_policy.scheduled_for)
        self.assertIsNotNone(self.store.get(f"{PAYMENTS_LOCATION}/metadata/{POINTER_NAME}"), "nothing deleted yet")
        operation = get_operation_record(self.table, materialization_id=MATERIALIZATION_ID, operation_id=drop.work_id)
        self.assertEqual("payments", operation["payload"]["maintenance_wait"]["table"])

        # Still running: the next check waits again, from the same moment.
        since = operation["payload"]["maintenance_wait"]["since"]
        self.assertEqual("drop_table", self.process(wait).step)
        operation = get_operation_record(self.table, materialization_id=MATERIALIZATION_ID, operation_id=drop.work_id)
        self.assertEqual(since, operation["payload"]["maintenance_wait"]["since"])

        # The app reported the job stopped: the drop goes on.
        save_materialization_table(self.table, replace(get_materialization_table(self.table, MATERIALIZATION_ID, "payments"), maintenance_job=None))
        self.assertIsNone(self.process(wait))
        self.assertEqual({"orders": "active"}, self.states())

    def test_a_drop_stops_waiting_for_a_maintenance_app_that_never_answers(self) -> None:
        from materializer_control_plane.models import RunningMaintenance
        from materializer_control_plane.storage import get_operation_record, save_operation_record

        self.finish(self.change(["orders"], {"payments": "keep"}))
        drop = self.change(["orders"], {"payments": "drop"}, request_id="req-drop")
        record = get_materialization_table(self.table, MATERIALIZATION_ID, "payments")
        save_materialization_table(self.table, replace(record, maintenance_job=RunningMaintenance(job="orphan_cleanup", started_at="2026-07-12T00:00:00Z")))
        wait = self.process(drop)
        operation = get_operation_record(self.table, materialization_id=MATERIALIZATION_ID, operation_id=drop.work_id)
        payload = dict(operation["payload"], maintenance_wait={"table": "payments", "since": "2020-01-01T00:00:00Z", "job": "orphan_cleanup"})
        save_operation_record(self.table, materialization_id=MATERIALIZATION_ID, operation_id=drop.work_id, updated_at="2026-07-12T01:00:00Z", payload=payload)

        self.assertIsNone(self.process(wait))

        self.assertEqual({"orders": "active"}, self.states())

    def test_deleting_the_materialization_stops_a_live_drop_and_drops_every_table(self) -> None:
        self.finish(self.change(["orders"], {"payments": "keep"}))
        drop = self.change(["orders"], {"payments": "drop"}, request_id="req-drop")
        save_materialization(self.table, replace(get_materialization(self.table, MATERIALIZATION_ID), desired_phase="deleting"))

        self.assertIsNone(self.process(drop))

        self.assertEqual("dropping", self.states()["payments"], "left for the delete's own drop")

    def test_the_routed_handler_publishes_every_follow_up(self) -> None:
        wait = self.until(self.change(["orders"], {"payments": "drop"}), "await_runtime_drained")
        final = self.process(wait)

        with patch("materializer_control_plane.services.workflow.get_metadata_client", return_value=self.metadata):
            routed = list(
                handle_work_event_for_routed_output(
                    final.to_dict(),
                    self.table,
                    self.discovery,
                    kaspr_app_client=self.kaspr,
                    kafka_admin_client=self.group,
                    object_store_client=self.client,
                    config=self.config,
                )
            )

        work_items = [(item["route"], item["value"]["step"]) for item in routed if "step" in item.get("value", {})]
        self.assertEqual([("scheduler", "observe_runtime"), ("work", "drop_table")], work_items)

    def test_a_scheduled_live_drop_retry_has_a_request_of_its_own(self) -> None:
        from materializer_control_plane.services.workflow import build_live_drop_event, scheduled_request_id

        live = build_live_drop_event(MATERIALIZATION_ID, stamp="req-1", requested_at="2026-07-12T01:00:00Z")

        self.assertEqual(f"drop_tables:{MATERIALIZATION_ID}", scheduled_request_id(live))
        self.assertEqual(f"drop_table:{MATERIALIZATION_ID}", scheduled_request_id(work("drop_table")))


if __name__ == "__main__":
    unittest.main()
