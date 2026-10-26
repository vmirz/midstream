from __future__ import annotations

import os
import asyncio
from collections.abc import Iterable
import copy
import threading
import types
import unittest
from unittest.mock import patch

from midstream.runtime.templates import PackageSource
from midstream.handlers import handle_resource_observation, handle_runtime_status
from midstream.handlers.resource_observation import (
    build_sweep_observation_trigger,
    handle_observation_work_for_output,
    handle_observation_work_for_output_async,
)
from midstream.kubernetes import KubernetesKasprAppClient
from midstream.models import (
    EventCause,
    Materialization,
    MaterializationStatus,
    MaterializationWorkEvent,
    ObservationPolicy,
    ResourceCondition,
    ResourceObservation,
    RuntimeBinding,
    RuntimeHealth,
    RuntimeMemberState,
    RuntimeStatusEvent,
    TableSpec,
    TargetRuntime,
)
from midstream.observation import (
    build_one_shot_observation_policy,
    derive_status_from_resource_observation,
    heartbeat_is_stale,
    normalize_resource_observation,
    observation_backoff_seconds,
)
from midstream.observation.mapping import available_member_count
from midstream.observation.reader import KasprAppObservationInput
from midstream.observation.restarts import restart_instability
from midstream.services import apply_resource_observation, apply_runtime_status_event
from midstream.storage import (
    get_runtime_binding,
    get_materialization_status,
    save_materialization,
    save_materialization_status,
    save_materialization_table,
    save_runtime_binding,
)
from table_records import build_table_record
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
        self.observation = observation

    def observe(self, *, namespace: str, name: str) -> KasprAppObservationInput:
        if self.observation is None:
            return KasprAppObservationInput(
                namespace=namespace,
                kaspr_app_name=name,
                resource_exists=False,
                managed_agent_states={},
            )
        return KasprAppObservationInput.from_dict(self.observation)


def build_materialization(*, desired_phase: str = "running") -> Materialization:
    return Materialization(
        materialization_id="mat-1",
        name="orders-materialization",
        tables=(TableSpec(name="orders", source_topic="orders", dataset="shop", dataset_id="ds-000000000001"),),
        destination_uri="abfs://lakehouse/orders",
        runtime_mode="dedicated",
        resolved_runtime_name="kaspr-mat-1",
        namespace="midstream",
        desired_phase=desired_phase,
        created_at="2026-07-12T00:00:00Z",
        updated_at="2026-07-12T00:00:00Z",
        created_by="alice",
    )


def build_status(
    *,
    desired_phase: str = "running",
    observed_phase: str = "deploying",
    last_heartbeat: str | None = None,
    ready_status: str | None = None,
    runtime_health_level: str = "unknown",
) -> MaterializationStatus:
    ready_condition = None
    if ready_status is not None:
        ready_condition = ResourceCondition(
            status=ready_status,
            reason="Healthy" if ready_status == "True" else "NotReady",
            message="condition",
            last_transition_time="2026-07-12T00:00:00Z",
        )
    return MaterializationStatus(
        materialization_id="mat-1",
        desired_phase=desired_phase,
        observed_phase=observed_phase,
        phase_reason=observed_phase,
        last_transition_time="2026-07-12T00:00:00Z",
        last_heartbeat=last_heartbeat,
        observation_source="hybrid",
        runtime_health=RuntimeHealth(level=runtime_health_level),
        resource_observation=ResourceObservation(
            namespace="midstream",
            kaspr_app_name="kaspr-mat-1",
            resource_exists=True,
            ready_condition=ready_condition,
            managed_agent_states={"writer": "running"},
        ),
    )


class ObservationPolicyTests(unittest.TestCase):
    def test_backoff_schedule_matches_contract(self) -> None:
        self.assertEqual(observation_backoff_seconds(1), 15)
        self.assertEqual(observation_backoff_seconds(2), 45)
        self.assertEqual(observation_backoff_seconds(3), 120)
        self.assertEqual(observation_backoff_seconds(4), 300)
        self.assertEqual(observation_backoff_seconds(9), 300)

    def test_build_one_shot_policy_sets_next_schedule(self) -> None:
        policy = build_one_shot_observation_policy(
            attempt=2,
            requested_at="2026-07-12T00:00:00Z",
        )

        self.assertEqual(policy.attempt, 2)
        self.assertEqual(policy.max_attempts, 4)
        self.assertEqual(policy.scheduled_for, "2026-07-12T00:00:45Z")

    def test_heartbeat_stale_detection_uses_two_minutes(self) -> None:
        self.assertFalse(
            heartbeat_is_stale(
                "2026-07-12T00:00:30Z",
                now="2026-07-12T00:02:00Z",
            )
        )
        self.assertTrue(
            heartbeat_is_stale(
                "2026-07-12T00:00:00Z",
                now="2026-07-12T00:02:01Z",
            )
        )


class ObservationMappingTests(unittest.TestCase):
    def test_normalize_resource_observation_filters_owned_agents(self) -> None:
        normalized = normalize_resource_observation(
            KasprAppObservationInput(
                namespace="midstream",
                kaspr_app_name="kaspr-materializer-orders",
                resource_exists=True,
                managed_agent_states={
                    "writer": "running",
                    "committer": "paused",
                    "foreign": "running",
                },
            ),
            managed_agent_names=["writer", "committer"],
        )

        self.assertEqual(normalized.managed_agents_total, 2)
        self.assertEqual(normalized.managed_agents_running, 1)
        self.assertEqual(normalized.managed_agents_paused, 1)
        self.assertNotIn("foreign", normalized.managed_agent_states)

    def test_delete_flow_marks_deleted_when_resource_missing(self) -> None:
        status = derive_status_from_resource_observation(
            build_status(desired_phase="deleting", observed_phase="deleting"),
            normalize_resource_observation(
                KasprAppObservationInput(
                    namespace="midstream",
                    kaspr_app_name="kaspr-materializer-orders",
                    resource_exists=False,
                )
            ),
            observed_at="2026-07-12T00:05:00Z",
        )

        self.assertEqual(status.observed_phase, "deleted")
        self.assertEqual(status.phase_reason, "resource_missing_after_delete")

    def test_ready_and_all_paused_marks_paused(self) -> None:
        status = derive_status_from_resource_observation(
            build_status(ready_status="True"),
            normalize_resource_observation(
                KasprAppObservationInput(
                    namespace="midstream",
                    kaspr_app_name="kaspr-materializer-orders",
                    resource_exists=True,
                    ready_condition=ResourceCondition(
                        status="True",
                        reason="Healthy",
                        message="Ready",
                        last_transition_time="2026-07-12T00:00:00Z",
                    ),
                    managed_agent_states={"writer": "paused", "schema-proposer": "paused"},
                )
            ),
            observed_at="2026-07-12T00:05:00Z",
        )

        self.assertEqual(status.observed_phase, "paused")
        self.assertEqual(status.phase_reason, "all_managed_agents_paused")

    def test_ready_plus_recent_heartbeat_marks_running(self) -> None:
        status = derive_status_from_resource_observation(
            build_status(
                last_heartbeat="2026-07-12T00:04:30Z",
                ready_status="True",
                runtime_health_level="healthy",
            ),
            normalize_resource_observation(
                KasprAppObservationInput(
                    namespace="midstream",
                    kaspr_app_name="kaspr-materializer-orders",
                    resource_exists=True,
                    ready_condition=ResourceCondition(
                        status="True",
                        reason="Healthy",
                        message="Ready",
                        last_transition_time="2026-07-12T00:00:00Z",
                    ),
                    managed_agent_states={"writer": "running"},
                )
            ),
            observed_at="2026-07-12T00:05:00Z",
        )

        self.assertEqual(status.observed_phase, "running")
        self.assertEqual(status.phase_reason, "runtime_ready")

    def test_ready_plus_recent_degraded_heartbeat_preserves_runtime_status(self) -> None:
        current_status = build_status(
            observed_phase="degraded",
            last_heartbeat="2026-07-12T00:04:30Z",
            ready_status="True",
            runtime_health_level="degraded",
        )
        current_status = MaterializationStatus(
            materialization_id=current_status.materialization_id,
            desired_phase=current_status.desired_phase,
            observed_phase=current_status.observed_phase,
            phase_reason="runtime_data_validation_degraded",
            phase_message="Schema proposal conflicts with another pending additive proposal.",
            last_transition_time=current_status.last_transition_time,
            last_heartbeat=current_status.last_heartbeat,
            last_resource_observation_time=current_status.last_resource_observation_time,
            observation_source=current_status.observation_source,
            runtime_health=RuntimeHealth(
                level="degraded",
                reason="data_validation",
                message="Schema proposal conflicts with another pending additive proposal.",
            ),
            resource_observation=current_status.resource_observation,
            runtime_progress=current_status.runtime_progress,
            last_error=current_status.last_error,
        )

        status = derive_status_from_resource_observation(
            current_status,
            normalize_resource_observation(
                KasprAppObservationInput(
                    namespace="midstream",
                    kaspr_app_name="kaspr-materializer-orders",
                    resource_exists=True,
                    ready_condition=ResourceCondition(
                        status="True",
                        reason="Healthy",
                        message="Ready",
                        last_transition_time="2026-07-12T00:00:00Z",
                    ),
                    managed_agent_states={"writer": "running"},
                )
            ),
            observed_at="2026-07-12T00:05:00Z",
            observation_policy=ObservationPolicy(attempt=4, max_attempts=4, mode="one_shot"),
        )

        self.assertEqual(status.observed_phase, "degraded")
        self.assertEqual(status.phase_reason, "runtime_data_validation_degraded")
        self.assertEqual(
            status.phase_message,
            "Schema proposal conflicts with another pending additive proposal.",
        )

    def test_ready_but_runtime_silent_stays_deploying_before_timeout(self) -> None:
        status = derive_status_from_resource_observation(
            build_status(ready_status="True"),
            normalize_resource_observation(
                KasprAppObservationInput(
                    namespace="midstream",
                    kaspr_app_name="kaspr-materializer-orders",
                    resource_exists=True,
                    ready_condition=ResourceCondition(
                        status="True",
                        reason="Healthy",
                        message="Ready",
                        last_transition_time="2026-07-12T00:00:00Z",
                    ),
                    managed_agent_states={"writer": "running"},
                )
            ),
            observed_at="2026-07-12T00:05:00Z",
            observation_policy=ObservationPolicy(attempt=1, max_attempts=4, mode="one_shot"),
        )

        self.assertEqual(status.observed_phase, "deploying")
        self.assertEqual(status.phase_reason, "waiting_for_runtime_ready")

    def test_ready_but_runtime_silent_degrades_after_timeout(self) -> None:
        status = derive_status_from_resource_observation(
            build_status(ready_status="True"),
            normalize_resource_observation(
                KasprAppObservationInput(
                    namespace="midstream",
                    kaspr_app_name="kaspr-materializer-orders",
                    resource_exists=True,
                    ready_condition=ResourceCondition(
                        status="True",
                        reason="Healthy",
                        message="Ready",
                        last_transition_time="2026-07-12T00:00:00Z",
                    ),
                    managed_agent_states={"writer": "running"},
                )
            ),
            observed_at="2026-07-12T00:05:00Z",
            observation_policy=ObservationPolicy(attempt=4, max_attempts=4, mode="one_shot"),
        )

        self.assertEqual(status.observed_phase, "degraded")
        self.assertEqual(status.phase_reason, "resource_ready_but_runtime_silent")

    def test_mixed_agent_states_stay_deploying_before_timeout(self) -> None:
        status = derive_status_from_resource_observation(
            build_status(),
            normalize_resource_observation(
                KasprAppObservationInput(
                    namespace="midstream",
                    kaspr_app_name="kaspr-materializer-orders",
                    resource_exists=True,
                    ready_condition=ResourceCondition(
                        status="False",
                        reason="Starting",
                        message="Converging",
                        last_transition_time="2026-07-12T00:00:00Z",
                    ),
                    managed_agent_states={"writer": "running", "schema-proposer": "unknown"},
                )
            ),
            observed_at="2026-07-12T00:05:00Z",
            observation_policy=ObservationPolicy(attempt=1, max_attempts=4, mode="one_shot"),
        )

        self.assertEqual(status.observed_phase, "deploying")
        self.assertEqual(status.phase_reason, "waiting_for_runtime_agents")

    def test_mixed_agent_states_degrade_after_timeout(self) -> None:
        status = derive_status_from_resource_observation(
            build_status(),
            normalize_resource_observation(
                KasprAppObservationInput(
                    namespace="midstream",
                    kaspr_app_name="kaspr-materializer-orders",
                    resource_exists=True,
                    ready_condition=ResourceCondition(
                        status="False",
                        reason="Starting",
                        message="Converging",
                        last_transition_time="2026-07-12T00:00:00Z",
                    ),
                    managed_agent_states={"writer": "running", "schema-proposer": "unknown"},
                )
            ),
            observed_at="2026-07-12T00:05:00Z",
            observation_policy=ObservationPolicy(attempt=4, max_attempts=4, mode="one_shot"),
        )

        self.assertEqual(status.observed_phase, "degraded")
        self.assertEqual(status.phase_reason, "mixed_agent_states")


def scaled_app(available_members: str | None, *, generation: int = 3, observed_generation: int = 3) -> ResourceObservation:
    """A KasprApp as the operator reports it: ready whenever available members equal desired."""
    return normalize_resource_observation(
        KasprAppObservationInput(
            namespace="midstream",
            kaspr_app_name="kaspr-mat-1",
            resource_exists=True,
            metadata_generation=generation,
            observed_generation=observed_generation,
            ready_condition=ResourceCondition(
                status="True",
                reason="Healthy",
                message="App is ready",
                last_transition_time="2026-07-12T00:00:00Z",
            ),
            rollout_in_progress=False,
            available_members=available_members,
            managed_agent_states={"writer": "unknown"},
        )
    )


class StoppedObservationTests(unittest.TestCase):
    """A stop scales the runtime to zero, and only the member count can confirm it has."""

    def test_zero_members_confirm_the_stop(self) -> None:
        status = derive_status_from_resource_observation(
            build_status(desired_phase="stopped", observed_phase="deploying", runtime_health_level="healthy"),
            scaled_app("0/0"),
            observed_at="2026-07-12T00:05:00Z",
        )

        self.assertEqual(status.observed_phase, "stopped")
        self.assertEqual(status.phase_reason, "runtime_scaled_to_zero")
        # Nothing is running, so nothing can be said about its health.
        self.assertEqual(status.runtime_health.level, "unknown")
        self.assertEqual(status.last_transition_time, "2026-07-12T00:05:00Z")

    def test_a_recent_heartbeat_does_not_keep_a_stopped_runtime_running(self) -> None:
        """Rule 6 would preserve `running` while the last heartbeat is fresh; a stop must not wait it out."""
        status = derive_status_from_resource_observation(
            build_status(
                desired_phase="stopped",
                observed_phase="running",
                last_heartbeat="2026-07-12T00:04:50Z",
                ready_status="True",
                runtime_health_level="healthy",
            ),
            scaled_app("0/0"),
            observed_at="2026-07-12T00:05:00Z",
        )

        self.assertEqual(status.observed_phase, "stopped")

    def test_members_still_running_are_waited_for(self) -> None:
        status = derive_status_from_resource_observation(
            build_status(desired_phase="stopped", observed_phase="deploying"),
            scaled_app("3/0"),
            observed_at="2026-07-12T00:05:00Z",
            observation_policy=ObservationPolicy(attempt=1, max_attempts=4, mode="one_shot"),
        )

        self.assertEqual(status.observed_phase, "deploying")
        self.assertEqual(status.phase_reason, "waiting_for_runtime_scale_down")

    def test_members_that_never_stop_degrade_after_the_observation_budget(self) -> None:
        status = derive_status_from_resource_observation(
            build_status(desired_phase="stopped", observed_phase="deploying"),
            scaled_app("2/0"),
            observed_at="2026-07-12T00:13:00Z",
            observation_policy=ObservationPolicy(attempt=4, max_attempts=4, mode="one_shot"),
        )

        self.assertEqual(status.observed_phase, "degraded")
        self.assertEqual(status.phase_reason, "runtime_scale_down_stalled")
        self.assertIn("2/0", status.phase_message)

    def test_an_unobserved_stop_is_still_deploying(self) -> None:
        """The operator has not seen the new spec yet, so `0/0` could not have come from it."""
        status = derive_status_from_resource_observation(
            build_status(desired_phase="stopped", observed_phase="running"),
            scaled_app("16/16", generation=4, observed_generation=3),
            observed_at="2026-07-12T00:05:00Z",
        )

        self.assertEqual(status.observed_phase, "deploying")
        self.assertEqual(status.phase_reason, "resource_generation_not_observed")

    def test_an_unknown_member_count_is_not_taken_for_zero(self) -> None:
        for available in (None, "", "n/a"):
            with self.subTest(available=available):
                status = derive_status_from_resource_observation(
                    build_status(desired_phase="stopped", observed_phase="deploying"),
                    scaled_app(available),
                    observed_at="2026-07-12T00:05:00Z",
                )
                self.assertEqual(status.phase_reason, "waiting_for_runtime_scale_down")

    def test_member_counts_parse_from_the_operator_status_string(self) -> None:
        self.assertEqual(available_member_count("0/0"), 0)
        self.assertEqual(available_member_count("16/16"), 16)
        self.assertEqual(available_member_count("3"), 3)
        self.assertIsNone(available_member_count(None))
        self.assertIsNone(available_member_count("unknown"))


class ObservationServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.package_revision_patcher = patch(
            "midstream.runtime.templates.installed_package_source",
            return_value=PackageSource(kind="git", reference="pkg-rev-1", requirement="git+https://github.com/vmirz/midstream.git@pkg-rev-1#egg=midstream-platform&subdirectory=python"),
        )
        self.package_revision_patcher.start()
        self.addCleanup(self.package_revision_patcher.stop)
        self.table = FakeTable()
        save_materialization(self.table, build_materialization())
        save_materialization_status(self.table, build_status())
        save_runtime_binding(
            self.table,
            RuntimeBinding(
                materialization_id="mat-1",
                runtime_mode="dedicated",
                kaspr_app_name="kaspr-materializer-orders",
                namespace="midstream",
                managed_agent_names=["writer", "committer"],
                template_version="v1",
                last_applied_spec_hash="hash",
                last_apply_operation_id="op-1",
                updated_at="2026-07-12T00:00:00Z",
            ),
        )

    def test_runtime_status_event_updates_heartbeat_and_running_phase(self) -> None:
        save_materialization_status(
            self.table,
            build_status(ready_status="True", runtime_health_level="unknown"),
        )

        updated = apply_runtime_status_event(
            self.table,
            RuntimeStatusEvent(
                event_id="evt-1",
                materialization_id="mat-1",
                runtime_app_ref="kaspr-materializer-orders",
                member_id="member-0",
                agent_name="writer",
                phase="running",
                health="healthy",
                schema_versions={"orders": 1},
                rows_written=12,
                bytes_written=128,
                files_written=1,
                records_skipped=3,
                latest_offsets={"orders": {"0": 22}},
                reported_at="2026-07-12T00:03:00Z",
            ),
        )

        self.assertEqual(updated.observed_phase, "running")
        self.assertEqual(updated.phase_reason, "runtime_ready")
        self.assertEqual(updated.last_heartbeat, "2026-07-12T00:03:00Z")
        self.assertEqual(updated.runtime_progress.rows_written, 12)
        self.assertEqual(updated.runtime_progress.records_skipped, 3)

    def test_a_heartbeat_records_the_runtime_settings_the_member_runs(self) -> None:
        save_materialization_status(self.table, build_status(ready_status="True", runtime_health_level="unknown"))

        def heartbeat(member_id: str, **settings) -> MaterializationStatus:
            return apply_runtime_status_event(
                self.table,
                RuntimeStatusEvent(
                    event_id=f"evt-{member_id}",
                    materialization_id="mat-1",
                    runtime_app_ref="kaspr-materializer-orders",
                    member_id=member_id,
                    agent_name="writer",
                    phase="running",
                    health="healthy",
                    reported_at="2026-07-12T00:03:00Z",
                    **settings,
                ),
            )

        heartbeat("member-0", settings_revision=2, settings_lineage="20261024T100000Z-4e5f6a7b")
        heartbeat("member-1", settings_revision=5, settings_lineage="")
        updated = heartbeat("member-2", settings_revision=5)
        members = updated.runtime_progress.member_progress
        self.assertEqual(
            {member_id: (member.settings_lineage, member.settings_revision) for member_id, member in members.items()},
            {
                "member-0": ("20261024T100000Z-4e5f6a7b", 2),
                # Settings without a lineage, from a runtime that knows lineages.
                "member-1": ("", 5),
                # A runtime from before lineages.
                "member-2": (None, 5),
            },
        )

    def test_heartbeats_keep_each_tables_offsets_blocks_and_unmapped_counts(self) -> None:
        save_materialization_status(self.table, build_status(ready_status="True", runtime_health_level="unknown"))

        def heartbeat(member_id: str, at: str, **values) -> MaterializationStatus:
            return apply_runtime_status_event(
                self.table,
                RuntimeStatusEvent(
                    event_id=f"evt-{member_id}-{at}",
                    materialization_id="mat-1",
                    runtime_app_ref="kaspr-mat-1",
                    member_id=member_id,
                    agent_name="writer",
                    phase="running",
                    health="healthy",
                    reported_at=at,
                    **values,
                ),
            )

        heartbeat(
            "member-0",
            "2026-07-12T00:03:00Z",
            latest_offsets={"orders": {"0": 5}, "payments": {"0": 2}},
            tables={"orders": {"current_snapshot_id": 3}, "payments": {"current_snapshot_id": 7}},
            table_counters={"orders": {"unmapped_values": 4}, "payments": {"unmapped_values": 1}},
        )
        # Another member owns other partitions and sends no table blocks: it runs no committer.
        updated = heartbeat(
            "member-1",
            "2026-07-12T00:03:10Z",
            latest_offsets={"orders": {"1": 9}},
            table_counters={"orders": {"unmapped_values": 2}},
        )

        progress = updated.runtime_progress
        self.assertEqual({"orders": {"0": 5, "1": 9}, "payments": {"0": 2}}, progress.latest_offsets)
        self.assertEqual({"current_snapshot_id": 7}, progress.table("payments"))
        self.assertEqual({"orders": 6, "payments": 1}, progress.cumulative_unmapped_by_table)
        self.assertEqual("2026-07-12T00:03:10Z", progress.unmapped_last_seen_by_table["orders"])
        self.assertEqual("2026-07-12T00:03:00Z", progress.unmapped_last_seen_by_table["payments"])

    def test_runtime_status_event_marks_data_validation_degradation_explicitly(self) -> None:
        save_materialization_status(
            self.table,
            build_status(ready_status="True", runtime_health_level="healthy"),
        )

        updated = apply_runtime_status_event(
            self.table,
            RuntimeStatusEvent(
                event_id="evt-2",
                materialization_id="mat-1",
                runtime_app_ref="kaspr-materializer-orders",
                member_id="member-0",
                agent_name="writer",
                phase="degraded",
                health="degraded",
                health_reason="data_validation",
                schema_versions={"orders": 1},
                latest_offsets={"orders": {"0": 22}},
                last_error="payload.body type changed",
                reported_at="2026-07-12T00:04:00Z",
            ),
        )

        self.assertEqual(updated.observed_phase, "degraded")
        self.assertEqual(updated.phase_reason, "runtime_data_validation_degraded")
        self.assertEqual(updated.runtime_health.reason, "data_validation")
        self.assertEqual(updated.runtime_health.message, "payload.body type changed")

    def test_runtime_status_event_surfaces_a_runtime_loading_its_table(self) -> None:
        save_materialization_status(
            self.table,
            build_status(
                ready_status="True",
                observed_phase="degraded",
                runtime_health_level="degraded",
            ),
        )

        updated = apply_runtime_status_event(
            self.table,
            RuntimeStatusEvent(
                event_id="evt-3",
                materialization_id="mat-1",
                runtime_app_ref="kaspr-materializer-orders",
                member_id="member-0",
                agent_name="writer",
                phase="running",
                health="unknown",
                schema_versions={"orders": 1},
                phase_reason="waiting_for_table",
                phase_message="The runtime is loading its Iceberg tables.",
                reported_at="2026-07-12T00:04:30Z",
            ),
        )

        self.assertEqual(updated.observed_phase, "deploying")
        self.assertEqual(updated.phase_reason, "waiting_for_table")
        self.assertEqual(updated.phase_message, "The runtime is loading its Iceberg tables.")

    def test_runtime_status_event_cannot_bring_a_stopping_runtime_back(self) -> None:
        """Members keep reporting until they exit; their reports count rows, not the lifecycle."""
        save_materialization(self.table, build_materialization(desired_phase="stopped"))
        save_materialization_status(
            self.table,
            build_status(
                desired_phase="stopped",
                observed_phase="deploying",
                ready_status="True",
                runtime_health_level="healthy",
            ),
        )

        updated = apply_runtime_status_event(
            self.table,
            RuntimeStatusEvent(
                event_id="evt-stop",
                materialization_id="mat-1",
                runtime_app_ref="kaspr-mat-1",
                member_id="member-0",
                agent_name="writer",
                phase="running",
                health="healthy",
                schema_versions={"orders": 1},
                rows_written=40,
                latest_offsets={"orders": {"0": 99}},
                phase_reason="processing",
                reported_at="2026-07-12T00:05:00Z",
            ),
        )

        self.assertEqual(updated.observed_phase, "deploying")
        self.assertEqual(updated.phase_reason, "deploying")
        self.assertEqual(updated.last_heartbeat, "2026-07-12T00:05:00Z")
        self.assertEqual(updated.runtime_progress.rows_written, 40)
        self.assertEqual(get_materialization_status(self.table, "mat-1").observed_phase, "deploying")

    def test_resource_observation_service_persists_stopped_state(self) -> None:
        save_materialization(self.table, build_materialization(desired_phase="stopped"))
        save_materialization_status(self.table, build_status(desired_phase="stopped", observed_phase="deploying"))

        updated = apply_resource_observation(
            self.table,
            "mat-1",
            KasprAppObservationInput(
                namespace="midstream",
                kaspr_app_name="kaspr-materializer-orders",
                resource_exists=True,
                metadata_generation=5,
                observed_generation=5,
                ready_condition=ResourceCondition(
                    status="True",
                    reason="Healthy",
                    message="App is ready",
                    last_transition_time="2026-07-12T00:05:30Z",
                ),
                available_members="0/0",
            ),
            observed_at="2026-07-12T00:06:00Z",
        )

        self.assertEqual(updated.observed_phase, "stopped")
        self.assertEqual(get_materialization_status(self.table, "mat-1").observed_phase, "stopped")

    def test_resource_observation_service_persists_paused_state(self) -> None:
        updated = apply_resource_observation(
            self.table,
            "mat-1",
            KasprAppObservationInput(
                namespace="midstream",
                kaspr_app_name="kaspr-materializer-orders",
                resource_exists=True,
                ready_condition=ResourceCondition(
                    status="True",
                    reason="Healthy",
                    message="Ready",
                    last_transition_time="2026-07-12T00:00:00Z",
                ),
                managed_agent_states={"writer": "paused", "schema-proposer": "paused", "foreign": "running"},
            ),
            observed_at="2026-07-12T00:06:00Z",
        )

        stored = get_materialization_status(self.table, "mat-1")
        self.assertEqual(updated.observed_phase, "paused")
        self.assertEqual(stored.observed_phase, "paused")
        self.assertNotIn("foreign", stored.resource_observation.managed_agent_states)

    def test_resource_observation_preserves_runtime_binding_until_kafka_cleanup_completes(self) -> None:
        save_materialization(self.table, build_materialization(desired_phase="deleting"))
        save_materialization_status(
            self.table,
            build_status(desired_phase="deleting", observed_phase="deleting"),
        )

        updated = apply_resource_observation(
            self.table,
            "mat-1",
            KasprAppObservationInput(
                namespace="midstream",
                kaspr_app_name="kaspr-materializer-orders",
                resource_exists=False,
            ),
            observed_at="2026-07-12T00:06:00Z",
        )

        self.assertEqual(updated.observed_phase, "deleted")
        self.assertIsNotNone(get_runtime_binding(self.table, "mat-1"))

    def test_observation_work_emits_cleanup_work_after_runtime_delete_is_confirmed(self) -> None:
        save_materialization(self.table, build_materialization(desired_phase="deleting"))
        save_materialization_status(
            self.table,
            build_status(desired_phase="deleting", observed_phase="deleting"),
        )

        outputs = list(
            handle_observation_work_for_output(
                MaterializationWorkEvent(
                    work_id="work-observe-mat-1",
                    materialization_id="mat-1",
                    step="observe_runtime",
                    requested_at="2026-07-12T00:05:00Z",
                    caused_by=EventCause(kind="request", id="req-delete"),
                    dedupe_key="observe:mat-1:work-delete:1",
                    target_runtime=__import__("midstream.models", fromlist=["TargetRuntime"]).TargetRuntime(
                        namespace="midstream",
                        kaspr_app_name="kaspr-mat-1",
                    ),
                    observation_policy=ObservationPolicy(
                        attempt=1,
                        max_attempts=4,
                        scheduled_for="2026-07-12T00:05:15Z",
                        mode="one_shot",
                    ),
                ).to_dict(),
                self.table,
                FakeKasprAppClient(),
                observed_at="2026-07-12T00:06:00Z",
            )
        )

        self.assertEqual(len(outputs), 1)
        self.assertEqual(outputs[0]["route"], "work")
        self.assertEqual(outputs[0]["value"]["step"], "cleanup_runtime_kafka")
        self.assertEqual(outputs[0]["value"]["materialization_id"], "mat-1")
        self.assertEqual(
            outputs[0]["value"]["target_runtime"],
            {"namespace": "midstream", "kaspr_app_name": "kaspr-mat-1"},
        )

    def test_a_status_from_a_materialization_the_control_plane_does_not_hold_is_dropped(self) -> None:
        # Purged while its runtime still reported, or written by an incompatible earlier release:
        # raising would stop the agent on the event, and every runtime's status behind it.
        event = RuntimeStatusEvent(
            event_id="evt-9",
            materialization_id="mat-gone",
            runtime_app_ref="kaspr-mat-gone",
            member_id="member-0",
            agent_name="writer",
            phase="running",
            health="healthy",
            schema_versions={"orders": 1},
            reported_at="2026-07-12T00:03:00Z",
        )
        before = dict(self.table.values)

        with self.assertLogs("midstream.services.observation", level="WARNING") as logged:
            self.assertIsNone(apply_runtime_status_event(self.table, event))
            self.assertIsNone(handle_runtime_status(event.to_dict(), self.table))

        self.assertEqual(before, self.table.values, "nothing is written")
        self.assertEqual(1, len(logged.records), "logged once")
        self.assertIn("mat-gone", logged.records[0].getMessage())

    def test_handlers_adapt_dict_inputs(self) -> None:
        runtime_status = handle_runtime_status(
            RuntimeStatusEvent(
                event_id="evt-2",
                materialization_id="mat-1",
                runtime_app_ref="kaspr-materializer-orders",
                member_id="member-1",
                agent_name="writer",
                phase="degraded",
                health="degraded",
                schema_versions={"orders": 1},
                reported_at="2026-07-12T00:03:00Z",
                last_error="boom",
            ).to_dict(),
            self.table,
        )
        resource_status = handle_resource_observation(
            "mat-1",
            {
                "namespace": "midstream",
                "kaspr_app_name": "kaspr-materializer-orders",
                "resource_exists": False,
            },
            self.table,
            observed_at="2026-07-12T00:10:00Z",
        )

        self.assertEqual(runtime_status.observed_phase, "degraded")
        self.assertEqual(resource_status.observed_phase, "degraded")

    def test_build_sweep_observation_trigger_returns_work_event_dict(self) -> None:
        trigger = build_sweep_observation_trigger(requested_at="2026-07-12T00:15:00Z")

        self.assertEqual(
            trigger["work_id"],
            "work-sweep-sweep-observations-task-2026-07-12T00:15:00Z",
        )
        self.assertEqual(trigger["materialization_id"], "__system__")
        self.assertEqual(trigger["step"], "sweep_observations")
        self.assertEqual(trigger["requested_at"], "2026-07-12T00:15:00Z")
        self.assertEqual(trigger["caused_by"], {"kind": "scheduler", "id": "sweep-observations-task"})
        self.assertEqual(
            trigger["dedupe_key"],
            "sweep:sweep-observations-task:2026-07-12T00:15:00Z",
        )

    def test_build_sweep_observation_trigger_allows_custom_task_id(self) -> None:
        trigger = build_sweep_observation_trigger(
            requested_at="2026-07-12T00:15:00Z",
            task_id="sweep-observations-startup-task",
        )

        self.assertEqual(
            trigger["work_id"],
            "work-sweep-sweep-observations-startup-task-2026-07-12T00:15:00Z",
        )
        self.assertEqual(
            trigger["caused_by"],
            {"kind": "scheduler", "id": "sweep-observations-startup-task"},
        )
        self.assertEqual(
            trigger["dedupe_key"],
            "sweep:sweep-observations-startup-task:2026-07-12T00:15:00Z",
        )

    def test_sweep_output_returns_single_routed_dict(self) -> None:
        outputs = list(
            handle_observation_work_for_output(
                build_sweep_observation_trigger(requested_at="2026-07-12T00:15:00Z"),
                self.table,
                FakeKasprAppClient(),
                observed_at="2026-07-12T00:15:00Z",
            )
        )

        self.assertEqual(len(outputs), 1)
        self.assertEqual(outputs[0]["route"], "work")
        self.assertEqual(outputs[0]["value"]["step"], "observe_runtime")

    def test_sweep_output_reapplies_runtime_when_rendered_spec_hash_changes(self) -> None:
        self.table = FakeTable()
        materialization = build_materialization()
        save_materialization(self.table, materialization)
        save_materialization_table(self.table, build_table_record(build_materialization()))
        save_materialization_status(
            self.table,
            build_status(
                desired_phase="running",
                observed_phase="running",
                last_heartbeat="2026-07-12T00:14:30Z",
                ready_status="True",
                runtime_health_level="healthy",
            ),
        )
        save_runtime_binding(
            self.table,
            RuntimeBinding(
                materialization_id="mat-1",
                runtime_mode="dedicated",
                kaspr_app_name="kaspr-materializer-orders",
                namespace="midstream",
                managed_agent_names=["writer", "committer"],
                template_version="v1",
                last_applied_spec_hash="stale-hash",
                last_apply_operation_id="op-1",
                updated_at="2026-07-12T00:00:00Z",
            ),
        )

        outputs = list(
            handle_observation_work_for_output(
                build_sweep_observation_trigger(requested_at="2026-07-12T00:15:00Z"),
                self.table,
                FakeKasprAppClient(),
                observed_at="2026-07-12T00:15:00Z",
            )
        )

        self.assertEqual(len(outputs), 1)
        self.assertEqual(outputs[0]["route"], "work")
        self.assertEqual(outputs[0]["value"]["step"], "apply_runtime")
        self.assertEqual(outputs[0]["value"]["caused_by"], {"kind": "scheduler", "id": "reconcile-runtime-drift"})

    def test_sweep_reapplies_only_a_materialization_with_a_table(self) -> None:
        self.table = FakeTable()
        materialization = build_materialization()
        save_materialization(self.table, materialization)
        # Created before 1.0: no table record, so nothing a reapply could render.
        save_materialization_status(
            self.table,
            build_status(
                desired_phase="running",
                observed_phase="running",
                last_heartbeat="2026-07-12T00:14:30Z",
                ready_status="True",
                runtime_health_level="healthy",
            ),
        )
        save_runtime_binding(
            self.table,
            RuntimeBinding(
                materialization_id="mat-1",
                runtime_mode="dedicated",
                kaspr_app_name="kaspr-materializer-orders",
                namespace="midstream",
                managed_agent_names=["writer", "committer"],
                template_version="v1",
                last_applied_spec_hash="stale-hash",
                last_apply_operation_id="op-1",
                updated_at="2026-07-12T00:00:00Z",
            ),
        )

        outputs = list(
            handle_observation_work_for_output(
                build_sweep_observation_trigger(requested_at="2026-07-12T00:15:00Z"),
                self.table,
                FakeKasprAppClient(),
                observed_at="2026-07-12T00:15:00Z",
            )
        )

        self.assertNotIn("apply_runtime", [output["value"].get("step") for output in outputs])

    def test_sweep_prioritizes_reapply_ahead_of_deleted_runtime_observation(self) -> None:
        self.table = FakeTable()
        save_materialization(
            self.table,
            Materialization(
                materialization_id="mat-0",
                name="older-materialization",
                tables=(TableSpec(name="orders", source_topic="orders", dataset="shop", dataset_id="ds-000000000001"),),
                destination_uri="abfs://lakehouse/orders",
                runtime_mode="dedicated",
                resolved_runtime_name="kaspr-materializer-older",
                namespace="midstream",
                desired_phase="deleting",
                created_at="2026-07-12T00:00:00Z",
                updated_at="2026-07-12T00:00:00Z",
                created_by="alice",
            ),
        )
        save_materialization_status(
            self.table,
            MaterializationStatus(
                materialization_id="mat-0",
                desired_phase="deleting",
                observed_phase="deleted",
                phase_reason="resource_missing_after_delete",
                last_transition_time="2026-07-12T00:00:00Z",
                last_resource_observation_time="2026-07-12T00:00:00Z",
                observation_source="hybrid",
                runtime_health=RuntimeHealth(level="unknown"),
                resource_observation=ResourceObservation(
                    namespace="midstream",
                    kaspr_app_name="kaspr-materializer-older",
                    resource_exists=False,
                    managed_agent_states={},
                ),
            ),
        )
        save_runtime_binding(
            self.table,
            RuntimeBinding(
                materialization_id="mat-0",
                runtime_mode="dedicated",
                kaspr_app_name="kaspr-materializer-older",
                namespace="midstream",
                managed_agent_names=["writer", "committer"],
                template_version="v1",
                last_applied_spec_hash="old-hash",
                last_apply_operation_id="op-0",
                updated_at="2026-07-12T00:00:00Z",
            ),
        )

        materialization = build_materialization()
        save_materialization(self.table, materialization)
        save_materialization_table(self.table, build_table_record(build_materialization()))
        save_materialization_status(
            self.table,
            build_status(
                desired_phase="running",
                observed_phase="running",
                last_heartbeat="2026-07-12T00:14:30Z",
                ready_status="True",
                runtime_health_level="healthy",
            ),
        )
        save_runtime_binding(
            self.table,
            RuntimeBinding(
                materialization_id="mat-1",
                runtime_mode="dedicated",
                kaspr_app_name="kaspr-materializer-orders",
                namespace="midstream",
                managed_agent_names=["writer", "committer"],
                template_version="v1",
                last_applied_spec_hash="stale-hash",
                last_apply_operation_id="op-1",
                updated_at="2026-07-12T00:00:00Z",
            ),
        )

        outputs = list(
            handle_observation_work_for_output(
                build_sweep_observation_trigger(requested_at="2026-07-12T00:15:00Z"),
                self.table,
                FakeKasprAppClient(),
                observed_at="2026-07-12T00:15:00Z",
            )
        )

        self.assertEqual(len(outputs), 2)
        self.assertEqual(outputs[0]["route"], "work")
        self.assertEqual(outputs[0]["value"]["materialization_id"], "mat-1")
        self.assertEqual(outputs[0]["value"]["step"], "apply_runtime")
        self.assertEqual(outputs[1]["value"]["materialization_id"], "mat-0")
        self.assertEqual(outputs[1]["value"]["step"], "observe_runtime")



def observe_work(*, kaspr_app_name: str = "kaspr-mat-1") -> dict:
    return MaterializationWorkEvent(
        work_id="work-observe-mat-1",
        materialization_id="mat-1",
        step="observe_runtime",
        requested_at="2026-07-12T00:05:00Z",
        caused_by=EventCause(kind="scheduler", id="observe:mat-1"),
        dedupe_key="observe:mat-1:1",
        target_runtime=TargetRuntime(namespace="midstream", kaspr_app_name=kaspr_app_name),
        observation_policy=ObservationPolicy(
            attempt=1,
            max_attempts=4,
            scheduled_for="2026-07-12T00:05:15Z",
            mode="one_shot",
        ),
    ).to_dict()


def finish_within(seconds: float, function):
    """Run `function` on a thread and return its result, failing if it is still running after `seconds`."""
    outcome: dict = {}

    def target() -> None:
        try:
            outcome["result"] = function()
        except BaseException as exc:  # noqa: BLE001 - re-raised on the test's thread
            outcome["error"] = exc

    thread = threading.Thread(target=target, daemon=True)
    thread.start()
    thread.join(seconds)
    if thread.is_alive():
        raise AssertionError(f"still waiting after {seconds}s: the wait has no deadline")
    if "error" in outcome:
        raise outcome["error"]
    return outcome["result"]


class AwaitableObserver:
    """A Kaspr client whose read is awaited, as the real client's `observe_async` is.

    The read gives up the event loop first; given `hold`, it waits until the test opens it.
    """

    def __init__(self, client, *, hold: bool = False) -> None:
        self.client = client
        self.hold = hold
        self.awaited = 0
        self.entered: asyncio.Event | None = None
        self.opened: asyncio.Event | None = None

    def observe(self, *, namespace: str, name: str) -> KasprAppObservationInput:
        return self.client.observe(namespace=namespace, name=name)

    async def observe_async(self, *, namespace: str, name: str) -> KasprAppObservationInput:
        self.awaited += 1
        if self.hold:
            self.entered.set()
            await self.opened.wait()
        else:
            await asyncio.sleep(0)
        return self.client.observe(namespace=namespace, name=name)


class SlowKubernetesApi:
    """The one CustomObjectsApi call an observation makes, answered after `delay` seconds or never."""

    def __init__(self, resource: dict, *, delay: float = 0.0, hang: bool = False) -> None:
        self.resource = resource
        self.delay = delay
        self.hang = hang
        self.threads: set[str] = set()
        self.cancelled = threading.Event()

    async def get_namespaced_custom_object(self, **kwargs) -> dict:
        self.threads.add(threading.current_thread().name)
        if not self.hang:
            await asyncio.sleep(self.delay)
            return self.resource
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            self.cancelled.set()
            raise


READY_APP = {
    "namespace": "midstream",
    "kaspr_app_name": "kaspr-mat-1",
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


class AsyncObservationHandlerTests(unittest.TestCase):
    """The resource-observation agent's entry point: the KasprApp read off the event loop."""

    def setUp(self) -> None:
        patcher = patch(
            "midstream.runtime.templates.installed_package_source",
            return_value=PackageSource(kind="git", reference="pkg-rev-1", requirement="git+https://github.com/vmirz/midstream.git@pkg-rev-1#egg=midstream-platform&subdirectory=python"),
        )
        patcher.start()
        self.addCleanup(patcher.stop)
        self.table = FakeTable()
        save_materialization(self.table, build_materialization())
        save_materialization_status(self.table, build_status(observed_phase="deploying"))
        save_materialization_table(self.table, build_table_record(build_materialization()))
        save_runtime_binding(
            self.table,
            RuntimeBinding(
                materialization_id="mat-1",
                runtime_mode="dedicated",
                kaspr_app_name="kaspr-mat-1",
                namespace="midstream",
                managed_agent_names=["writer"],
                template_version="v1",
                last_applied_spec_hash="hash",
                last_apply_operation_id="op-1",
                updated_at="2026-07-12T00:00:00Z",
            ),
        )

    def run_both(self, value: dict, observation: dict | None) -> list[dict]:
        """Run `value` through the sync handler and the async one, each on a copy of the table."""
        sync_table, async_table = FakeTable(), FakeTable()
        sync_table.values = copy.deepcopy(self.table.values)
        async_table.values = copy.deepcopy(self.table.values)
        sync_outputs = list(
            handle_observation_work_for_output(
                value,
                sync_table,
                FakeKasprAppClient(observation),
                observed_at="2026-07-12T00:06:00Z",
            )
        )
        client = AwaitableObserver(FakeKasprAppClient(observation))
        async_outputs = asyncio.run(
            handle_observation_work_for_output_async(
                value,
                async_table,
                client,
                observed_at="2026-07-12T00:06:00Z",
            )
        )

        self.assertIsInstance(async_outputs, types.GeneratorType, "Kaspr sends anything else as one value")
        async_outputs = list(async_outputs)
        self.assertEqual(async_outputs, sync_outputs)
        self.assertEqual(async_table.values, sync_table.values)
        if value["step"] == "observe_runtime":
            self.assertEqual(client.awaited, 1)
        return async_outputs

    def test_a_confirmed_delete_queues_the_kafka_cleanup_as_the_sync_handler_does(self) -> None:
        save_materialization(self.table, build_materialization(desired_phase="deleting"))
        save_materialization_status(self.table, build_status(desired_phase="deleting", observed_phase="deleting"))

        outputs = self.run_both(observe_work(), None)

        self.assertEqual([output["value"]["step"] for output in outputs], ["cleanup_runtime_kafka"])

    def test_a_runtime_still_rolling_out_is_observed_again_as_the_sync_handler_does(self) -> None:
        outputs = self.run_both(
            observe_work(),
            {**READY_APP, "ready_condition": None, "metadata_generation": 2, "rollout_in_progress": True},
        )

        self.assertEqual([output["route"] for output in outputs], ["scheduler"])

    def test_a_ready_but_silent_runtime_is_observed_again_as_the_sync_handler_does(self) -> None:
        outputs = self.run_both(observe_work(), READY_APP)

        self.assertEqual([output["route"] for output in outputs], ["scheduler"])

    def test_a_sweep_matches_the_sync_handler_and_never_gives_up_the_loop(self) -> None:
        trigger = build_sweep_observation_trigger(requested_at="2026-07-12T00:06:00Z")
        outputs = self.run_both(trigger, None)
        # The binding's spec hash is stale, so the sweep re-applies rather than observes.
        self.assertEqual([output["value"]["step"] for output in outputs], ["apply_runtime"])

        coroutine = handle_observation_work_for_output_async(
            trigger,
            self.table,
            FakeKasprAppClient(),
            observed_at="2026-07-12T00:06:00Z",
        )
        # Runs the coroutine to its first suspension; a sweep reads only the table, so there is none.
        with self.assertRaises(StopIteration) as finished:
            coroutine.send(None)
        self.assertEqual(list(finished.exception.value), outputs)

    def test_the_read_leaves_the_event_loop_running(self) -> None:
        client = KubernetesKasprAppClient()
        self.addCleanup(client._runner.close)
        api = SlowKubernetesApi(
            {
                "status": {
                    "conditions": [{"type": "Ready", "status": "True", "lastTransitionTime": "2026-07-12T00:00:00Z"}],
                    "linkedResources": {"agents": [{"name": "writer", "state": "running"}]},
                }
            },
            delay=0.2,
        )
        client._api = api
        turns = 0

        async def scenario():
            nonlocal turns
            done = False

            async def tick() -> None:
                nonlocal turns
                while not done:
                    await asyncio.sleep(0.005)
                    turns += 1

            ticking = asyncio.ensure_future(tick())
            outputs = await handle_observation_work_for_output_async(
                observe_work(),
                self.table,
                client,
                observed_at="2026-07-12T00:06:00Z",
            )
            done = True
            await ticking
            return list(outputs)

        outputs = asyncio.run(scenario())

        self.assertEqual([output["route"] for output in outputs], ["scheduler"])
        self.assertEqual(api.threads, {"kaspr-app-client"})
        # The read takes 0.2 s on the client's loop: 40 turns of this one; a held loop would not turn.
        self.assertGreaterEqual(turns, 10)

    def test_a_heartbeat_written_during_the_read_is_kept(self) -> None:
        """The status is updated after the read, from the record as the runtime-status agent left it."""
        client = AwaitableObserver(FakeKasprAppClient(READY_APP), hold=True)

        async def scenario():
            client.entered, client.opened = asyncio.Event(), asyncio.Event()
            observing = asyncio.ensure_future(
                handle_observation_work_for_output_async(
                    observe_work(),
                    self.table,
                    client,
                    observed_at="2026-07-12T00:06:00Z",
                )
            )
            await asyncio.wait_for(client.entered.wait(), 5)
            apply_runtime_status_event(
                self.table,
                RuntimeStatusEvent(
                    event_id="evt-1",
                    materialization_id="mat-1",
                    runtime_app_ref="kaspr-mat-1",
                    member_id="member-0",
                    agent_name="writer",
                    phase="running",
                    health="healthy",
                    schema_versions={"orders": 1},
                    rows_written=7,
                    reported_at="2026-07-12T00:05:58Z",
                ),
            )
            client.opened.set()
            return list(await observing)

        asyncio.run(scenario())

        status = get_materialization_status(self.table, "mat-1")
        self.assertEqual(status.last_heartbeat, "2026-07-12T00:05:58Z")
        self.assertEqual(status.runtime_progress.rows_written, 7)
        self.assertEqual(status.last_resource_observation_time, "2026-07-12T00:06:00Z")
        self.assertEqual(status.observed_phase, "running")

    def test_a_read_that_times_out_changes_nothing(self) -> None:
        """The sweep observes the runtime again; an escaped error would crash the agent instead."""
        for awaited in (False, True):
            with self.subTest(awaited=awaited):
                client = KubernetesKasprAppClient(call_timeout_seconds=0.05)
                self.addCleanup(client._runner.close)
                api = SlowKubernetesApi({}, hang=True)
                client._api = api
                before = copy.deepcopy(self.table.values)

                with self.assertLogs("midstream.handlers.resource_observation", "WARNING") as logs:
                    if awaited:
                        outputs = finish_within(
                            5,
                            lambda: list(
                                asyncio.run(
                                    handle_observation_work_for_output_async(
                                        observe_work(),
                                        self.table,
                                        client,
                                        observed_at="2026-07-12T00:06:00Z",
                                    )
                                )
                            ),
                        )
                    else:
                        outputs = finish_within(
                            5,
                            lambda: list(
                                handle_observation_work_for_output(
                                    observe_work(),
                                    self.table,
                                    client,
                                    observed_at="2026-07-12T00:06:00Z",
                                )
                            ),
                        )

                self.assertEqual(outputs, [])
                self.assertEqual(self.table.values, before)
                self.assertIn("Kubernetes read of KasprApp midstream/kaspr-mat-1 timed out after 0.05s", logs.output[0])
                self.assertTrue(api.cancelled.wait(2), "the read was left running on the client's loop")

    def test_a_read_that_fails_otherwise_still_raises(self) -> None:
        class FailingClient:
            def observe(self, *, namespace: str, name: str):
                raise RuntimeError("403 forbidden")

        with self.assertRaisesRegex(RuntimeError, "403 forbidden"):
            list(handle_observation_work_for_output(observe_work(), self.table, FailingClient(), observed_at="t"))
        with self.assertRaisesRegex(RuntimeError, "403 forbidden"):
            asyncio.run(
                handle_observation_work_for_output_async(observe_work(), self.table, FailingClient(), observed_at="t")
            )



def setUpModule() -> None:
    # A deployment always names the namespace its runtimes run in (handbook 02 §2.3); these tests
    # reach the configuration without choosing one, so the environment names it for them.
    os.environ.setdefault("CONTROL_PLANE_DEFAULT_NAMESPACE", "midstream")
    control_plane_config.get_config.cache_clear()


if __name__ == "__main__":
    unittest.main()

def member(
    name: str,
    *,
    restarts: int = 0,
    reason: str | None = None,
    code: int | None = None,
    exited_at: str | None = None,
    waiting: str | None = None,
) -> RuntimeMemberState:
    return RuntimeMemberState(
        name=name,
        restarts=restarts,
        last_exit_reason=reason,
        last_exit_code=code,
        last_exited_at=exited_at,
        waiting_reason=waiting,
    )


NOW = "2026-07-12T01:00:00Z"
TEN_MINUTES_AGO = "2026-07-12T00:50:00Z"
AN_HOUR_AGO = "2026-07-12T00:00:00Z"


class RestartInstabilityTests(unittest.TestCase):
    def test_members_killed_for_memory_or_held_back_after_crashes_are_unstable(self) -> None:
        members = (
            member("app-0", restarts=58, reason="OOMKilled", code=137, exited_at=TEN_MINUTES_AGO),
            member("app-1", restarts=48, reason="OOMKilled", code=137, exited_at=TEN_MINUTES_AGO, waiting="CrashLoopBackOff"),
            member("app-2"),
        )
        instability = restart_instability(members, now=NOW)
        self.assertEqual((2, 3, "out of memory"), (instability.unstable, instability.members, instability.cause))
        self.assertEqual("2 of 3 members restarted in the last 30 minutes: out of memory.", instability.message)

    def test_one_out_of_memory_kill_is_enough_but_one_other_restart_is_not(self) -> None:
        self.assertIsNotNone(restart_instability((member("app-0", restarts=1, reason="OOMKilled", code=137, exited_at=TEN_MINUTES_AGO),), now=NOW))
        self.assertIsNone(restart_instability((member("app-0", restarts=1, reason="Error", code=1, exited_at=TEN_MINUTES_AGO),), now=NOW))
        frequent = restart_instability((member("app-0", restarts=3, reason="Error", code=137, exited_at=TEN_MINUTES_AGO),), now=NOW)
        self.assertEqual("The member restarted in the last 30 minutes: exited with code 137.", frequent.message)

    def test_restarts_older_than_the_window_are_forgotten(self) -> None:
        self.assertIsNone(restart_instability((member("app-0", restarts=58, reason="OOMKilled", exited_at=AN_HOUR_AGO),), now=NOW))

    def test_crash_looping_members_are_unstable_whenever_they_last_ran(self) -> None:
        looping = restart_instability(
            (member("app-0", restarts=9, reason="Error", code=1, exited_at=AN_HOUR_AGO, waiting="CrashLoopBackOff"), member("app-1")),
            now=NOW,
        )
        self.assertEqual("1 of 2 members is crash-looping: exited with code 1.", looping.message)
        self.assertEqual(1, looping.unstable)

    def test_pods_that_were_not_read_say_nothing(self) -> None:
        self.assertIsNone(restart_instability(None, now=NOW))
        self.assertIsNone(restart_instability((), now=NOW))

    def test_member_states_round_trip_on_the_observation(self) -> None:
        observation = ResourceObservation(
            namespace="midstream",
            kaspr_app_name="kaspr-mat-1",
            resource_exists=True,
            managed_agent_states={},
            members=(member("app-0", restarts=2, reason="OOMKilled", code=137, exited_at=TEN_MINUTES_AGO, waiting="CrashLoopBackOff"), member("app-1")),
        )
        self.assertEqual(observation, ResourceObservation.from_dict(observation.to_dict()))
        self.assertNotIn("members", replace_members(observation, None).to_dict())


def replace_members(observation: ResourceObservation, members) -> ResourceObservation:
    from dataclasses import replace

    return replace(observation, members=members)


def oom_looping_observation(*, exited_at: str = TEN_MINUTES_AGO) -> KasprAppObservationInput:
    return KasprAppObservationInput(
        namespace="midstream",
        kaspr_app_name="kaspr-mat-1",
        resource_exists=True,
        ready_condition=ResourceCondition(status="True", reason="Healthy", message="Ready", last_transition_time=AN_HOUR_AGO),
        managed_agent_states={"writer": "running"},
        members=(
            member("kaspr-mat-1-app-0", restarts=58, reason="OOMKilled", code=137, exited_at=exited_at),
            member("kaspr-mat-1-app-1", restarts=40, reason="OOMKilled", code=137, exited_at=exited_at),
            member("kaspr-mat-1-app-2", restarts=0),
        ),
    )


class RestartingRuntimeStatusTests(unittest.TestCase):
    def test_an_observation_of_members_killed_for_memory_degrades_a_runtime_that_reports_healthy(self) -> None:
        status = derive_status_from_resource_observation(
            build_status(observed_phase="running", last_heartbeat="2026-07-12T00:59:30Z", ready_status="True", runtime_health_level="healthy"),
            normalize_resource_observation(oom_looping_observation()),
            observed_at=NOW,
        )
        self.assertEqual(("degraded", "runtime_members_restarting"), (status.observed_phase, status.phase_reason))
        self.assertEqual("2 of 3 members restarted in the last 30 minutes: out of memory.", status.phase_message)
        self.assertEqual(("degraded", "members_restarting"), (status.runtime_health.level, status.runtime_health.reason))
        self.assertEqual(status.phase_message, status.runtime_health.message)
        self.assertEqual(3, len(status.resource_observation.members))

    def test_restarts_long_past_leave_a_healthy_runtime_running(self) -> None:
        status = derive_status_from_resource_observation(
            build_status(observed_phase="deploying", last_heartbeat="2026-07-12T00:59:30Z", ready_status="True", runtime_health_level="healthy"),
            normalize_resource_observation(oom_looping_observation(exited_at=AN_HOUR_AGO)),
            observed_at=NOW,
        )
        self.assertEqual("running", status.observed_phase)

    def test_a_stopping_runtime_is_judged_by_its_member_count_not_its_restarts(self) -> None:
        status = derive_status_from_resource_observation(
            build_status(desired_phase="stopped", observed_phase="running", ready_status="True"),
            normalize_resource_observation(oom_looping_observation()),
            observed_at=NOW,
        )
        self.assertNotEqual("degraded", status.observed_phase)


class RestartingRuntimeHeartbeatTests(unittest.TestCase):
    def setUp(self) -> None:
        self.table = FakeTable()
        save_materialization(self.table, build_materialization())
        restarting = derive_status_from_resource_observation(
            build_status(observed_phase="running", last_heartbeat="2026-07-12T00:59:30Z", ready_status="True", runtime_health_level="healthy"),
            normalize_resource_observation(oom_looping_observation()),
            observed_at=NOW,
        )
        save_materialization_status(self.table, restarting)

    def heartbeat(self, at: str):
        return apply_runtime_status_event(
            self.table,
            RuntimeStatusEvent(
                event_id=f"evt-{at}",
                materialization_id="mat-1",
                runtime_app_ref="kaspr-mat-1",
                member_id="kaspr-mat-1-app-2",
                agent_name="writer",
                phase="running",
                health="healthy",
                schema_versions={"orders": 1},
                rows_written=1,
                bytes_written=1,
                files_written=1,
                records_skipped=0,
                latest_offsets={"orders": {"0": 1}},
                reported_at=at,
            ),
        )

    def test_a_healthy_heartbeat_does_not_make_a_restarting_runtime_healthy(self) -> None:
        updated = self.heartbeat("2026-07-12T01:00:30Z")
        self.assertEqual(("degraded", "runtime_members_restarting"), (updated.observed_phase, updated.phase_reason))
        self.assertEqual("degraded", updated.runtime_health.level)
        self.assertEqual("2026-07-12T01:00:30Z", updated.last_heartbeat)

    def test_once_the_restarts_age_out_a_heartbeat_makes_it_running_again(self) -> None:
        updated = self.heartbeat("2026-07-12T01:21:00Z")
        self.assertEqual(("running", "healthy"), (updated.observed_phase, updated.runtime_health.level))
