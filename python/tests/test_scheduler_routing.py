from __future__ import annotations

import os
from collections.abc import Iterable
import unittest
from unittest.mock import patch

from midstream.runtime.templates import PackageSource
from midstream.handlers import handle_observation_work_for_output, handle_work_event_for_routed_output
from midstream.models import (
    EventCause,
    Materialization,
    MaterializationStatus,
    MaterializationWorkEvent,
    ObservationPolicy,
    ResourceCondition,
    ResourceObservation,
    RuntimeHealth,
    TargetRuntime,
)
from midstream.schema.discovery import StubDiscoveryService
from midstream.storage import (
    materialization_routing_key,
    save_materialization,
    save_materialization_table,
    save_materialization_status,
    save_runtime_binding,
)
from midstream.runtime import build_runtime_binding
from midstream.config import ControlPlaneConfig
from midstream.models import SettingsChangeRequest
from midstream.services.settings import apply_settings_change
from table_records import build_table_record, table_specs
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


class FakeKasprAppClient:
    def __init__(self, observation: dict | None = None) -> None:
        self.applied: list[dict] = []
        self.deleted: list[tuple[str, str]] = []
        self.observation = observation

    def apply(self, resource: dict) -> None:
        self.applied.append(resource)

    def delete(self, *, namespace: str, name: str, kind: str | None = None) -> None:
        self.deleted.append((namespace, name))

    def observe(self, *, namespace: str, name: str):
        from midstream.observation.reader import KasprAppObservationInput

        if self.observation is None:
            return KasprAppObservationInput(
                namespace=namespace,
                kaspr_app_name=name,
                resource_exists=False,
                managed_agent_states={},
            )
        return KasprAppObservationInput.from_dict(self.observation)


class FakeKafkaAdminClient:
    def __init__(
        self,
        *,
        consumer_groups: list[str] | None = None,
        nonempty_consumer_groups: list[str] | None = None,
    ) -> None:
        self.consumer_groups = [] if consumer_groups is None else list(consumer_groups)
        self.nonempty_consumer_groups = set() if nonempty_consumer_groups is None else set(nonempty_consumer_groups)

    def list_topics(self) -> list[str]:
        return []

    def delete_topics(self, topics: list[str]) -> None:
        return None

    def list_consumer_groups(self) -> list[str]:
        return list(self.consumer_groups)

    def delete_consumer_group(self, group_id: str) -> None:
        from midstream.kafka.admin import ConsumerGroupNotEmptyError

        if group_id in self.nonempty_consumer_groups:
            raise ConsumerGroupNotEmptyError(group_id)
        self.consumer_groups = [existing for existing in self.consumer_groups if existing != group_id]


def build_materialization(*, desired_phase: str = "running", updated_at: str = "2026-07-12T00:00:00Z") -> Materialization:
    return Materialization(
        materialization_id="mat-1",
        name="orders-materialization",
        tables=table_specs("orders"),
        destination_uri="abfs://lakehouse/orders",
        runtime_mode="dedicated",
        resolved_runtime_name="kaspr-mat-1",
        namespace="midstream",
        desired_phase=desired_phase,
        created_at="2026-07-12T00:00:00Z",
        updated_at=updated_at,
        created_by="vmirzoyan",
    )


def build_status(*, observed_phase: str = "requested") -> MaterializationStatus:
    return MaterializationStatus(
        materialization_id="mat-1",
        desired_phase="running",
        observed_phase=observed_phase,
        phase_reason=observed_phase,
        last_transition_time="2026-07-12T00:00:00Z",
        observation_source="hybrid",
        runtime_health=RuntimeHealth(level="unknown"),
        resource_observation=ResourceObservation(
            namespace="midstream",
            kaspr_app_name="kaspr-mat-1",
            resource_exists=True,
            managed_agent_states={"writer": "running"},
            ready_condition=ResourceCondition(
                status="False",
                reason="Deploying",
                message="not ready",
                last_transition_time="2026-07-12T00:00:00Z",
            ),
        ),
    )


class SchedulerRoutingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.package_revision_patcher = patch("midstream.runtime.templates.installed_package_source", return_value=PackageSource(kind="git", reference="pkg-rev-1", requirement="git+https://github.com/vmirz/midstream.git@pkg-rev-1#egg=midstream-platform&subdirectory=python"))
        self.package_revision_patcher.start()
        self.addCleanup(self.package_revision_patcher.stop)

    def test_workflow_retries_missing_state_through_scheduler(self) -> None:
        routed_outputs = list(handle_work_event_for_routed_output(
            MaterializationWorkEvent(
                work_id="work-create-mat-1",
                materialization_id="mat-1",
                step="start_discovery",
                requested_at="2026-07-12T00:00:00Z",
                caused_by=EventCause(kind="request", id="req-1"),
                dedupe_key="start_discovery:mat-1:req-1",
            ).to_dict(),
            FakeTable(),
            StubDiscoveryService(),
            kaspr_app_client=FakeKasprAppClient(),
        ))

        self.assertEqual(len(routed_outputs), 1)
        routed = routed_outputs[0]
        self.assertEqual(routed["route"], "scheduler")
        self.assertEqual(routed["key"], materialization_routing_key("mat-1"))
        self.assertEqual(
            routed["headers"]["x-scheduler-request-id"],
            b"missing-state:mat-1:start_discovery",
        )
        self.assertEqual(routed["headers"]["x-scheduler-action"], b"REPLACE")
        self.assertEqual(routed["value"]["observation_policy"]["attempt"], 1)

    def test_workflow_routes_observe_follow_up_to_scheduler(self) -> None:
        table = FakeTable()
        save_materialization(table, build_materialization())
        save_materialization_status(table, build_status(observed_phase="discovered"))
        save_materialization_table(table, build_table_record(build_materialization()))

        routed_outputs = list(handle_work_event_for_routed_output(
            MaterializationWorkEvent(
                work_id="work-apply-mat-1",
                materialization_id="mat-1",
                step="apply_runtime",
                requested_at="2026-07-12T00:00:00Z",
                caused_by=EventCause(kind="request", id="req-1"),
                dedupe_key="apply_runtime:mat-1:req-1",
            ).to_dict(),
            table,
            StubDiscoveryService(),
            kaspr_app_client=FakeKasprAppClient(),
        ))

        # Without runtime settings changed, an apply publishes nothing but its follow-up.
        self.assertEqual(len(routed_outputs), 1)
        routed = routed_outputs[0]
        self.assertEqual(routed["route"], "scheduler")
        self.assertEqual(routed["key"], materialization_routing_key("mat-1"))
        self.assertEqual(routed["headers"]["x-scheduler-request-id"], b"observe:mat-1")

    def test_apply_runtime_brings_the_runtime_onto_current_runtime_settings(self) -> None:
        table = FakeTable()
        settings = FakeTable()
        save_materialization(table, build_materialization())
        save_materialization_status(table, build_status(observed_phase="discovered"))
        save_materialization_table(table, build_table_record(build_materialization()))
        apply_settings_change(
            settings,
            SettingsChangeRequest(
                request_id="set-1",
                scope="runtime",
                submitted_at="2026-07-12T00:00:00Z",
                submitted_by="admin",
                values={"max_open_file_age_seconds": 90},
            ),
            config=ControlPlaneConfig(),
        )

        routed_outputs = list(handle_work_event_for_routed_output(
            MaterializationWorkEvent(
                work_id="work-apply-mat-1",
                materialization_id="mat-1",
                step="apply_runtime",
                requested_at="2026-07-12T00:00:00Z",
                caused_by=EventCause(kind="request", id="req-1"),
                dedupe_key="apply_runtime:mat-1:req-1",
            ).to_dict(),
            table,
            StubDiscoveryService(),
            kaspr_app_client=FakeKasprAppClient(),
            settings_table=settings,
        ))

        self.assertEqual([output["route"] for output in routed_outputs], ["topic", "scheduler"])
        delivery = routed_outputs[0]
        self.assertEqual(delivery["topic"], ControlPlaneConfig().schema_authority_updates_topic)
        self.assertEqual(delivery["value"]["kind"], "runtime_settings")
        self.assertEqual(delivery["value"]["target_materialization_id"], "mat-1")
        self.assertEqual(delivery["value"]["values"], {"max_open_file_age_seconds": 90})

    def test_delete_workflow_routes_observe_follow_up_directly(self) -> None:
        table = FakeTable()
        save_materialization(table, build_materialization(desired_phase="deleting"))
        save_materialization_status(table, build_status(observed_phase="deleting"))

        routed_outputs = list(handle_work_event_for_routed_output(
            MaterializationWorkEvent(
                work_id="work-delete-mat-1",
                materialization_id="mat-1",
                step="delete_runtime",
                requested_at="2026-07-12T00:00:00Z",
                caused_by=EventCause(kind="request", id="req-delete"),
                dedupe_key="delete_runtime:mat-1:req-delete",
            ).to_dict(),
            table,
            StubDiscoveryService(),
            kaspr_app_client=FakeKasprAppClient(),
        ))

        self.assertEqual(len(routed_outputs), 1)
        routed = routed_outputs[0]
        self.assertEqual(routed["route"], "work")
        self.assertEqual(routed["value"]["step"], "observe_runtime")
        self.assertNotIn("x-scheduler-request-id", routed["headers"])

    def test_cleanup_runtime_kafka_retry_routes_through_scheduler(self) -> None:
        table = FakeTable()
        save_materialization(table, build_materialization(desired_phase="deleting"))
        save_materialization_status(
            table,
            MaterializationStatus.from_dict(
                {**build_status(observed_phase="deleted").to_dict(), "desired_phase": "deleting"}
            ),
        )
        save_runtime_binding(
            table,
            build_runtime_binding(
                materialization=build_materialization(desired_phase="deleting"),
                runtime_app_name="kaspr-mat-1",
                managed_agent_names=["writer", "committer", "settings"],
                template_version="v1",
                spec_hash="abc123",
                operation_id="work-apply-mat-1",
                updated_at="2026-07-12T00:00:00Z",
            ),
        )

        routed_outputs = list(handle_work_event_for_routed_output(
            MaterializationWorkEvent(
                work_id="work-cleanup-kafka-mat-1",
                materialization_id="mat-1",
                step="cleanup_runtime_kafka",
                requested_at="2026-07-12T00:00:00Z",
                caused_by=EventCause(kind="resource_observation", id="runtime-deleted:mat-1"),
                dedupe_key="cleanup_runtime_kafka:mat-1",
                target_runtime=TargetRuntime(
                    namespace="midstream",
                    kaspr_app_name="kaspr-mat-1",
                ),
            ).to_dict(),
            table,
            StubDiscoveryService(),
            kaspr_app_client=FakeKasprAppClient(),
            kafka_admin_client=FakeKafkaAdminClient(
                consumer_groups=["kaspr-mat-1-app"],
                nonempty_consumer_groups=["kaspr-mat-1-app"],
            ),
        ))

        self.assertEqual(len(routed_outputs), 1)
        routed = routed_outputs[0]
        self.assertEqual(routed["route"], "scheduler")
        self.assertEqual(routed["headers"]["x-scheduler-request-id"], b"cleanup_runtime_kafka:mat-1")
        self.assertEqual(routed["value"]["step"], "cleanup_runtime_kafka")
        self.assertEqual(routed["value"]["observation_policy"]["attempt"], 1)

    def test_workflow_drops_missing_state_after_retry_budget_exhausts(self) -> None:
        routed_outputs = list(handle_work_event_for_routed_output(
            MaterializationWorkEvent(
                work_id="work-create-mat-1",
                materialization_id="mat-1",
                step="start_discovery",
                requested_at="2026-07-12T00:00:00Z",
                caused_by=EventCause(kind="scheduler", id="retry-missing-state:mat-1"),
                dedupe_key="start_discovery:mat-1:req-1:missing-state:4",
                observation_policy=ObservationPolicy(
                    attempt=4,
                    max_attempts=4,
                    scheduled_for="2026-07-12T00:05:00Z",
                    mode="one_shot",
                ),
            ).to_dict(),
            FakeTable(),
            StubDiscoveryService(),
            kaspr_app_client=FakeKasprAppClient(),
        ))

        self.assertEqual(routed_outputs, [])

    def test_workflow_drops_stale_missing_state_retry_after_progress(self) -> None:
        for observed_phase in ("deploying", "degraded"):
            with self.subTest(observed_phase=observed_phase):
                table = FakeTable()
                save_materialization(table, build_materialization())
                save_materialization_status(table, build_status(observed_phase=observed_phase))

                routed_outputs = list(handle_work_event_for_routed_output(
                    MaterializationWorkEvent(
                        work_id="work-create-mat-1",
                        materialization_id="mat-1",
                        step="start_discovery",
                        requested_at="2026-07-12T00:00:00Z",
                        caused_by=EventCause(kind="scheduler", id="retry-missing-state:mat-1"),
                        dedupe_key="start_discovery:mat-1:work-create:missing-state:1",
                        observation_policy=ObservationPolicy(
                            attempt=1,
                            max_attempts=4,
                            scheduled_for="2026-07-12T00:00:15Z",
                            mode="one_shot",
                        ),
                    ).to_dict(),
                    table,
                    StubDiscoveryService(),
                    kaspr_app_client=FakeKasprAppClient(),
                ))

                self.assertEqual(routed_outputs, [])

    def test_workflow_drops_request_caused_start_discovery_after_delete_progress(self) -> None:
        table = FakeTable()
        save_materialization(table, build_materialization(desired_phase="deleting"))
        save_materialization_status(table, build_status(observed_phase="deleting"))

        routed_outputs = list(handle_work_event_for_routed_output(
            MaterializationWorkEvent(
                work_id="work-create-mat-1",
                materialization_id="mat-1",
                step="start_discovery",
                requested_at="2026-07-12T00:00:00Z",
                caused_by=EventCause(kind="request", id="req-1"),
                dedupe_key="start_discovery:mat-1:req-1",
            ).to_dict(),
            table,
            StubDiscoveryService(),
            kaspr_app_client=FakeKasprAppClient(),
        ))

        self.assertEqual(routed_outputs, [])

    def test_observation_work_reschedules_when_runtime_still_deploying(self) -> None:
        table = FakeTable()
        save_materialization(table, build_materialization(updated_at="2026-07-12T00:00:15Z"))
        save_materialization_status(table, build_status(observed_phase="deploying"))
        save_runtime_binding(
            table,
            build_runtime_binding(
                materialization=build_materialization(),
                runtime_app_name="kaspr-materializer-orders",
                managed_agent_names=["writer", "schema-proposer"],
                template_version="v1",
                spec_hash="abc123",
                operation_id="work-apply-mat-1",
                updated_at="2026-07-12T00:00:00Z",
            ),
        )

        routed_outputs = list(
            handle_observation_work_for_output(
                MaterializationWorkEvent(
                    work_id="work-observe-mat-1",
                    materialization_id="mat-1",
                    step="observe_runtime",
                    requested_at="2026-07-12T00:00:15Z",
                    caused_by=EventCause(kind="scheduler", id="observe:mat-1"),
                    dedupe_key="observe:mat-1:1",
                    target_runtime=__import__("midstream.models", fromlist=["TargetRuntime"]).TargetRuntime(
                        namespace="midstream",
                        kaspr_app_name="kaspr-materializer-orders",
                    ),
                    observation_policy=ObservationPolicy(
                        attempt=1,
                        max_attempts=4,
                        scheduled_for="2026-07-12T00:00:15Z",
                        mode="one_shot",
                    ),
                ).to_dict(),
                table,
                FakeKasprAppClient(
                    observation={
                        "namespace": "midstream",
                        "kaspr_app_name": "kaspr-materializer-orders",
                        "resource_exists": True,
                        "managed_agent_states": {"writer": "running"},
                        "metadata_generation": 2,
                        "observed_generation": 1,
                        "rollout_in_progress": True,
                    }
                ),
                observed_at="2026-07-12T00:00:30Z",
            )
        )

        self.assertEqual(len(routed_outputs), 1)
        routed = routed_outputs[0]

        self.assertEqual(routed["route"], "scheduler")
        self.assertEqual(routed["key"], materialization_routing_key("mat-1"))
        self.assertEqual(routed["headers"]["x-scheduler-request-id"], b"observe:mat-1")

    def test_observation_work_keeps_observing_a_ready_but_silent_runtime(self) -> None:
        table = FakeTable()
        save_materialization(table, build_materialization(updated_at="2026-07-12T00:00:15Z"))
        save_materialization_status(table, build_status(observed_phase="deploying"))
        save_materialization_table(table, build_table_record(build_materialization()))
        save_runtime_binding(
            table,
            build_runtime_binding(
                materialization=build_materialization(),
                runtime_app_name="kaspr-materializer-orders",
                managed_agent_names=["writer", "committer"],
                template_version="v1",
                spec_hash="abc123",
                operation_id="work-apply-mat-1",
                updated_at="2026-07-12T00:00:00Z",
            ),
        )

        routed_outputs = list(
            handle_observation_work_for_output(
                MaterializationWorkEvent(
                    work_id="work-observe-mat-1",
                    materialization_id="mat-1",
                    step="observe_runtime",
                    requested_at="2026-07-12T00:00:15Z",
                    caused_by=EventCause(kind="scheduler", id="observe:mat-1"),
                    dedupe_key="observe:mat-1:1",
                    target_runtime=__import__("midstream.models", fromlist=["TargetRuntime"]).TargetRuntime(
                        namespace="midstream",
                        kaspr_app_name="kaspr-materializer-orders",
                    ),
                    observation_policy=ObservationPolicy(
                        attempt=1,
                        max_attempts=4,
                        scheduled_for="2026-07-12T00:00:15Z",
                        mode="one_shot",
                    ),
                ).to_dict(),
                table,
                FakeKasprAppClient(
                    observation={
                        "namespace": "midstream",
                        "kaspr_app_name": "kaspr-materializer-orders",
                        "resource_exists": True,
                        "managed_agent_states": {"writer": "running"},
                        "ready_condition": {
                            "status": "True",
                            "reason": "Healthy",
                            "message": "Ready",
                            "last_transition_time": "2026-07-12T00:00:00Z",
                        },
                        "metadata_generation": 1,
                        "observed_generation": 1,
                        "rollout_in_progress": False,
                    }
                ),
                observed_at="2026-07-12T00:00:30Z",
            )
        )

        # The runtime loads its table itself; the control plane only keeps watching for its heartbeat.
        self.assertEqual(["scheduler"], [routed["route"] for routed in routed_outputs])
        self.assertEqual(routed_outputs[0]["value"]["step"], "observe_runtime")



def setUpModule() -> None:
    # A deployment always names the namespace its runtimes run in (handbook 02 §2.3); these tests
    # reach the configuration without choosing one, so the environment names it for them.
    os.environ.setdefault("CONTROL_PLANE_DEFAULT_NAMESPACE", "midstream")
    control_plane_config.get_config.cache_clear()


if __name__ == "__main__":
    unittest.main()