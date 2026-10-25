"""Runtime autoscaling: more members for a runtime far behind its topics, its own count after."""

from __future__ import annotations

import os
from collections.abc import Iterable
from dataclasses import replace
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))

from table_records import build_table_record, table_specs  # noqa: E402

from materializer_control_plane.config import ControlPlaneConfig, get_config  # noqa: E402
from materializer_control_plane import config as control_plane_config  # noqa: E402
from materializer_control_plane.handlers.maintenance import handle_maintenance_work  # noqa: E402
from materializer_control_plane.handlers.resource_observation import (  # noqa: E402
    build_sweep_observation_trigger,
    handle_observation_work_for_output,
)
from materializer_control_plane.models import (  # noqa: E402
    LAG_POINTS_KEPT,
    ConsumerLag,
    EventCause,
    LagPoint,
    Materialization,
    MaterializationRequestEvent,
    MaterializationRequestSpec,
    MaterializationStatus,
    MaterializationWorkEvent,
    ResourceCondition,
    ResourceObservation,
    RuntimeAutoscale,
    RuntimeBinding,
    RuntimeHealth,
    TopicLag,
)
from materializer_control_plane.observation.reader import KasprAppObservationInput  # noqa: E402
from materializer_control_plane.runtime import (  # noqa: E402
    build_dedicated_runtime_template_input,
    render_dedicated_runtime_resources,
    spec_hash,
)
from materializer_control_plane.services import process_request_event  # noqa: E402
from materializer_control_plane.services.autoscaling import (  # noqa: E402
    CALM_POINTS,
    AutoscalePolicy,
    autoscale_after_lag,
    plan_autoscale,
    scaled_materialization,
)
from materializer_control_plane.services.retention import purge_materialization_records  # noqa: E402
from materializer_control_plane.storage import (  # noqa: E402
    get_materialization,
    get_runtime_autoscale,
    save_materialization,
    save_materialization_status,
    save_materialization_table,
    save_runtime_autoscale,
    save_runtime_binding,
)

POLICY = AutoscalePolicy(enabled=True, max_replicas=16, target_seconds=1800, min_lag=100_000, cooldown_seconds=300)


def at(minute: int) -> str:
    hours, minutes = divmod(minute, 60)
    return f"2026-10-25T{12 + hours:02d}:{minutes:02d}:00Z"


def materialization(*, replicas: int = 1, autoscaling: bool = True, desired_phase: str = "running") -> Materialization:
    return Materialization(
        materialization_id="mat-1",
        name="orders",
        tables=table_specs("orders"),
        destination_uri="gs://lakehouse/warehouse",
        resolved_runtime_name="kaspr-mat-1",
        namespace="midstream",
        desired_phase=desired_phase,
        created_at=at(0),
        updated_at=at(0),
        replicas=replicas,
        autoscaling=autoscaling,
    )


def measure(subject: Materialization, lags: Iterable[tuple[int, int]], *, record: RuntimeAutoscale | None = None, policy=POLICY, capacity: int | None = 32, healthy: bool = True):
    """Each (minute, lag) measured in turn; the plans, last one last."""
    plans = []
    for minute, lag in lags:
        plan = plan_autoscale(subject, record, lag=lag, measured_at=at(minute), capacity=capacity, healthy=healthy, policy=policy)
        record = plan.autoscale
        plans.append(plan)
    return plans


class AutoscaleDecisionTests(unittest.TestCase):
    def test_a_runtime_far_behind_gets_the_members_its_pace_says_it_needs(self) -> None:
        # 50,000 records a minute drained from 4.9 M: 98 minutes alone, three times the target.
        first, second, third = measure(materialization(), [(0, 5_000_000), (1, 4_950_000), (2, 4_900_000)])
        self.assertEqual((False, False), (first.changed, second.changed), "not before three measurements show its pace")
        self.assertTrue(third.changed)
        self.assertEqual(4, third.autoscale.replicas)
        self.assertEqual(5880, third.autoscale.eta_seconds)
        self.assertEqual("4,900,000 records behind, about 1 h 38 min to catch up with 1 member", third.autoscale.reason)
        self.assertEqual(at(2), third.autoscale.changed_at)

    def test_never_more_than_the_maximum_or_the_members_the_partitions_keep_busy(self) -> None:
        slow = [(0, 90_000_000), (1, 89_990_000), (2, 89_980_000)]
        self.assertEqual(16, measure(materialization(), slow)[-1].autoscale.replicas)
        self.assertEqual(3, measure(materialization(), slow, capacity=3)[-1].autoscale.replicas)
        self.assertEqual(16, measure(materialization(), slow, capacity=None)[-1].autoscale.replicas)

    def test_a_runtime_that_does_not_drain_doubles(self) -> None:
        plan = measure(materialization(replicas=2), [(0, 900_000), (1, 900_000), (2, 950_000)])[-1]
        self.assertEqual(4, plan.autoscale.replicas)
        self.assertIsNone(plan.autoscale.eta_seconds)
        self.assertEqual("950,000 records behind and not catching up with 2 members", plan.autoscale.reason)

    def test_nothing_for_a_runtime_on_time_under_the_floor_or_unhealthy(self) -> None:
        on_time = measure(materialization(), [(0, 1_000_000), (1, 400_000), (2, 200_000)])[-1]
        self.assertFalse(on_time.changed, "200,000 at 300,000 a minute is under a minute away")
        under = measure(materialization(), [(0, 99_000), (1, 99_000), (2, 99_000)])[-1]
        self.assertFalse(under.changed)
        unhealthy = measure(materialization(), [(0, 5_000_000), (1, 4_950_000), (2, 4_900_000)], healthy=False)[-1]
        self.assertFalse(unhealthy.changed, "members added to a restarting runtime only restart too")

    def test_no_second_step_within_the_cooldown_and_a_fresh_pace_first(self) -> None:
        plans = measure(materialization(), [(0, 50_000_000), (1, 49_990_000), (2, 49_980_000), (3, 49_970_000), (4, 49_960_000), (5, 49_950_000), (8, 49_900_000)])
        self.assertEqual([2], [index for index, plan in enumerate(plans) if plan.changed][:1])
        self.assertEqual(16, plans[2].autoscale.replicas)
        self.assertTrue(all(not plan.changed for plan in plans[3:]), "at the maximum already")
        stepwise = measure(materialization(), [(0, 5_000_000), (1, 4_950_000), (2, 4_900_000), (3, 4_880_000), (4, 4_860_000), (5, 4_840_000), (8, 4_780_000)])
        self.assertEqual(4, stepwise[2].autoscale.replicas)
        self.assertFalse(any(plan.changed for plan in stepwise[3:6]), "three minutes after a change: still cooling down")
        self.assertTrue(stepwise[6].changed, "five minutes on, its new pace still too slow: more members")
        self.assertGreater(stepwise[6].autoscale.replicas, 4)

    def test_caught_up_for_three_measurements_it_goes_back_to_its_own_count(self) -> None:
        scaled = RuntimeAutoscale(materialization_id="mat-1", replicas=8, reason="far behind", changed_at=at(0))
        plans = measure(materialization(replicas=2), [(10, 120_000), (11, 90_000), (12, 80_000), (13, 70_000)], record=scaled)
        self.assertEqual([False, False, False, True], [plan.changed for plan in plans])
        self.assertEqual(CALM_POINTS, 3)
        last = plans[-1].autoscale
        self.assertEqual((None, None, at(13)), (last.replicas, last.reason, last.changed_at))

    def test_turned_off_stopped_or_disabled_it_goes_back_at_once(self) -> None:
        scaled = RuntimeAutoscale(materialization_id="mat-1", replicas=8, reason="far behind", changed_at=at(0))
        for subject, policy in (
            (materialization(autoscaling=False), POLICY),
            (materialization(desired_phase="stopped"), POLICY),
            (materialization(), replace(POLICY, enabled=False)),
        ):
            plan = measure(subject, [(1, 9_000_000)], record=scaled, policy=policy)[-1]
            self.assertTrue(plan.changed)
            self.assertIsNone(plan.autoscale.replicas)

    def test_a_count_raised_past_the_scaled_one_is_no_scaling(self) -> None:
        scaled = RuntimeAutoscale(materialization_id="mat-1", replicas=4, changed_at=at(0))
        plan = measure(materialization(replicas=8), [(10, 50_000)], record=scaled)[-1]
        self.assertFalse(plan.changed)
        self.assertIsNone(plan.autoscale.replicas)

    def test_it_keeps_the_last_quarter_hour_of_measurements(self) -> None:
        plans = measure(materialization(), [(minute, 50_000) for minute in range(30)])
        self.assertEqual(LAG_POINTS_KEPT, len(plans[-1].autoscale.points))
        self.assertEqual(at(29), plans[-1].autoscale.points[-1].at)

    def test_the_record_round_trips(self) -> None:
        record = RuntimeAutoscale(
            materialization_id="mat-1",
            replicas=6,
            reason="far behind",
            changed_at=at(2),
            eta_seconds=1200,
            points=(LagPoint(at=at(1), lag=10), LagPoint(at=at(2), lag=5)),
            updated_at=at(2),
        )
        self.assertEqual(record, RuntimeAutoscale.from_dict(record.to_dict()))


class ScaledRenderTests(unittest.TestCase):
    def setUp(self) -> None:
        patcher = patch("materializer_control_plane.runtime.templates.get_installed_package_revision", return_value="pkg-rev-1")
        patcher.start()
        self.addCleanup(patcher.stop)

    def replicas(self, subject: Materialization) -> int:
        rendered = render_dedicated_runtime_resources(build_dedicated_runtime_template_input(subject, table_records=[build_table_record(subject)]))
        return rendered["kaspr_app"]["spec"]["replicas"] if "kaspr_app" in rendered else next(
            resource["spec"]["replicas"] for resource in rendered.values() if isinstance(resource, dict) and resource.get("kind") == "KasprApp"
        )

    def test_the_runtime_is_rendered_at_the_count_autoscaling_gave_it(self) -> None:
        scaled = RuntimeAutoscale(materialization_id="mat-1", replicas=6)
        self.assertEqual(6, self.replicas(scaled_materialization(materialization(replicas=2), scaled)))
        self.assertEqual(2, self.replicas(scaled_materialization(materialization(replicas=2, autoscaling=False), scaled)))
        self.assertEqual(8, self.replicas(scaled_materialization(materialization(replicas=8), scaled)))
        self.assertEqual(2, self.replicas(scaled_materialization(materialization(replicas=2), None)))
        self.assertEqual(2, materialization(replicas=2).replicas, "the materialization's own count stays")


class FakeTable:
    def __init__(self) -> None:
        self.values: dict[str, dict] = {}

    def get(self, key: str) -> dict | None:
        return self.values.get(key)

    def __setitem__(self, key: str, value: dict) -> None:
        self.values[key] = value

    def __delitem__(self, key: str) -> None:
        if key not in self.values:
            raise KeyError(key)
        del self.values[key]

    def prefix_scan(self, prefix: str):
        for key in sorted(self.values):
            if key.startswith(prefix):
                yield key, self.values[key]


def status(*, observed_phase: str = "running", health: str = "healthy") -> MaterializationStatus:
    return MaterializationStatus(
        materialization_id="mat-1",
        desired_phase="running",
        observed_phase=observed_phase,
        phase_reason="runtime_ready",
        last_transition_time=at(0),
        observation_source="hybrid",
        runtime_health=RuntimeHealth(level=health),
        last_heartbeat=at(0),
        resource_observation=ResourceObservation(
            namespace="midstream",
            kaspr_app_name="kaspr-mat-1",
            resource_exists=True,
            managed_agent_states={"writer": "running"},
            ready_condition=ResourceCondition(status="True", reason="Ready", message=None, last_transition_time=at(0)),
        ),
    )


def lag_event(minute: int, lag: int, *, partitions: int = 8) -> MaterializationWorkEvent:
    return MaterializationWorkEvent(
        work_id=f"work-record-consumer-lag-mat-1-{at(minute)}",
        materialization_id="mat-1",
        step="record_consumer_lag",
        requested_at=at(minute),
        caused_by=EventCause(kind="scheduler", id="refresh-consumer-lag"),
        dedupe_key=f"record_consumer_lag:mat-1:{at(minute)}",
        consumer_lag=ConsumerLag(
            total_lag=lag,
            topics={"orders": TopicLag(total_lag=lag, partitions=partitions)},
            measured_at=at(minute),
        ),
    )


class AutoscaleAfterLagTests(unittest.TestCase):
    def setUp(self) -> None:
        self.table = FakeTable()
        self.config = ControlPlaneConfig()
        save_materialization(self.table, materialization())
        save_materialization_status(self.table, status())

    def record(self, minute: int, lag: int) -> list[dict]:
        return list(handle_maintenance_work(lag_event(minute, lag).to_dict(), self.table, config=self.config))

    def test_each_measurement_is_kept_and_a_change_is_applied_in_the_materializations_shard(self) -> None:
        self.assertEqual([], self.record(0, 5_000_000))
        self.assertEqual([], self.record(1, 4_950_000))
        outputs = self.record(2, 4_900_000)
        self.assertEqual(1, len(outputs))
        self.assertEqual(("work", "materialization:mat-1"), (outputs[0]["route"], outputs[0]["key"]))
        work = outputs[0]["value"]
        self.assertEqual(("apply_runtime", {"kind": "scheduler", "id": "runtime-autoscaling"}), (work["step"], work["caused_by"]))
        self.assertEqual({"namespace": "midstream", "kaspr_app_name": "kaspr-mat-1"}, {k: work["target_runtime"][k] for k in ("namespace", "kaspr_app_name")})
        record = get_runtime_autoscale(self.table, "mat-1")
        self.assertEqual((4, 3), (record.replicas, len(record.points)))

    def test_partitions_bound_the_members(self) -> None:
        for minute, lag in ((0, 90_000_000), (1, 89_990_000)):
            self.record(minute, lag)
        self.assertEqual([], autoscale_after_lag(self.table, lag_event(1, 89_990_000), config=self.config), "a measurement delivered again counts once")
        self.assertEqual(2, len(get_runtime_autoscale(self.table, "mat-1").points))
        work = autoscale_after_lag(self.table, lag_event(2, 89_980_000, partitions=8), config=self.config)
        self.assertEqual(1, len(work))
        self.assertEqual(8, get_runtime_autoscale(self.table, "mat-1").replicas, "eight partitions keep eight members busy")

    def test_a_degraded_runtime_is_not_given_more_members(self) -> None:
        save_materialization_status(self.table, status(observed_phase="degraded", health="degraded"))
        for minute, lag in ((0, 5_000_000), (1, 4_950_000), (2, 4_900_000)):
            self.assertEqual([], self.record(minute, lag))

    def test_nothing_is_kept_for_a_materialization_that_turned_autoscaling_off(self) -> None:
        save_materialization(self.table, materialization(autoscaling=False))
        self.record(0, 5_000_000)
        self.assertIsNone(get_runtime_autoscale(self.table, "mat-1"))

    def test_the_deployments_switch_takes_a_scaled_runtime_back(self) -> None:
        save_runtime_autoscale(self.table, RuntimeAutoscale(materialization_id="mat-1", replicas=6, changed_at=at(0)))
        work = autoscale_after_lag(self.table, lag_event(10, 9_000_000), config=replace(self.config, runtime_autoscaling_enabled=False))
        self.assertEqual(["apply_runtime"], [event.step for event in work])
        self.assertIsNone(get_runtime_autoscale(self.table, "mat-1").replicas)

    def test_purging_a_materialization_forgets_its_autoscaling(self) -> None:
        save_runtime_autoscale(self.table, RuntimeAutoscale(materialization_id="mat-1", replicas=6))
        purge_materialization_records(self.table, "mat-1")
        self.assertIsNone(get_runtime_autoscale(self.table, "mat-1"))


class ScaledSweepTests(unittest.TestCase):
    def setUp(self) -> None:
        patcher = patch("materializer_control_plane.runtime.templates.get_installed_package_revision", return_value="pkg-rev-1")
        patcher.start()
        self.addCleanup(patcher.stop)
        self.table = FakeTable()
        self.subject = materialization(replicas=2)
        save_materialization(self.table, self.subject)
        save_materialization_table(self.table, build_table_record(self.subject))
        save_materialization_status(self.table, status())
        save_runtime_autoscale(self.table, RuntimeAutoscale(materialization_id="mat-1", replicas=6, changed_at=at(0)))

    def sweep(self, applied: Materialization) -> list[dict]:
        rendered = render_dedicated_runtime_resources(build_dedicated_runtime_template_input(applied, table_records=[build_table_record(applied)]))
        save_runtime_binding(
            self.table,
            RuntimeBinding(
                materialization_id="mat-1",
                runtime_mode="dedicated",
                kaspr_app_name="kaspr-mat-1",
                namespace="midstream",
                managed_agent_names=["writer", "committer"],
                template_version="v1",
                last_applied_spec_hash=spec_hash(rendered),
                last_apply_operation_id="op-1",
                updated_at=at(0),
            ),
        )

        class Client:
            def observe(self, *, namespace: str, name: str) -> KasprAppObservationInput:
                return KasprAppObservationInput(namespace=namespace, kaspr_app_name=name, resource_exists=True, managed_agent_states={})

        return [
            output
            for output in handle_observation_work_for_output(
                build_sweep_observation_trigger(requested_at=at(1)), self.table, Client(), observed_at=at(1)
            )
            if output["value"]["step"] == "apply_runtime"
        ]

    def test_a_scaled_runtime_is_no_drift_to_the_sweep(self) -> None:
        self.assertEqual([], self.sweep(replace(self.subject, replicas=6)))

    def test_a_runtime_not_yet_at_its_scaled_count_is_applied_again(self) -> None:
        self.assertEqual(1, len(self.sweep(self.subject)))


def request(action: str, *, autoscaling: bool | None = None, replicas: int | None = 2) -> MaterializationRequestEvent:
    return MaterializationRequestEvent(
        request_id=f"req-{action}-{autoscaling}",
        materialization_id="mat-1",
        action=action,
        submitted_at=at(30),
        submitted_by="admin@example.com",
        submitted_by_id="usr-000000000001",
        spec=MaterializationRequestSpec(
            name="orders",
            tables=table_specs("orders"),
            destination_uri="gs://lakehouse/warehouse",
            runtime_name="kaspr-mat-1",
            namespace="midstream",
            replicas=replicas,
            autoscaling=autoscaling,
        ),
        claim_request_id="idr-claim000001" if action == "create" else None,
    )


class AutoscaleRequestTests(unittest.TestCase):
    def setUp(self) -> None:
        self.table = FakeTable()
        self.config = ControlPlaneConfig()

    def test_a_create_has_autoscaling_unless_it_says_otherwise(self) -> None:
        process_request_event(self.table, request("create"), config=self.config)
        self.assertTrue(get_materialization(self.table, "mat-1").autoscaling)
        other = FakeTable()
        process_request_event(other, request("create", autoscaling=False), config=self.config)
        self.assertFalse(get_materialization(other, "mat-1").autoscaling)

    def existing(self, *, scaled: bool) -> None:
        save_materialization(self.table, materialization(replicas=2))
        save_materialization_status(self.table, status())
        if scaled:
            save_runtime_autoscale(self.table, RuntimeAutoscale(materialization_id="mat-1", replicas=6, changed_at=at(0)))

    def test_turning_it_off_while_scaled_takes_the_members_back_at_once(self) -> None:
        self.existing(scaled=True)
        result = process_request_event(self.table, request("update", autoscaling=False), config=self.config)
        self.assertEqual(["apply_runtime"], [event.step for event in result.work])
        self.assertFalse(get_materialization(self.table, "mat-1").autoscaling)
        self.assertIsNone(get_runtime_autoscale(self.table, "mat-1"))
        self.assertEqual(1, get_materialization(self.table, "mat-1").spec_version, "not part of the runtime's spec")

    def test_turning_it_off_or_on_otherwise_applies_nothing(self) -> None:
        self.existing(scaled=False)
        off = process_request_event(self.table, request("update", autoscaling=False), config=self.config)
        self.assertEqual([], [event.step for event in off.work])
        on = process_request_event(self.table, replace(request("update", autoscaling=True), request_id="req-on"), config=self.config)
        self.assertEqual([], [event.step for event in on.work])
        self.assertTrue(get_materialization(self.table, "mat-1").autoscaling)

    def test_an_update_from_before_autoscaling_leaves_it_as_it_is(self) -> None:
        self.existing(scaled=False)
        save_materialization(self.table, replace(materialization(replicas=2), autoscaling=False))
        process_request_event(self.table, request("update", autoscaling=None), config=self.config)
        self.assertFalse(get_materialization(self.table, "mat-1").autoscaling)

    def test_a_stop_forgets_the_scale_so_a_resume_starts_at_its_own_count(self) -> None:
        self.existing(scaled=True)
        process_request_event(self.table, request("stop"), config=self.config)
        self.assertIsNone(get_runtime_autoscale(self.table, "mat-1"))


def tearDownModule() -> None:
    # Renders read the deployment's configuration, which is cached: leave none behind for other modules.
    get_config.cache_clear()



def setUpModule() -> None:
    # A deployment always names the namespace its runtimes run in (handbook 02 §2.3); these tests
    # reach the configuration without choosing one, so the environment names it for them.
    os.environ.setdefault("MATERIALIZER_CONTROL_PLANE_DEFAULT_NAMESPACE", "midstream")
    control_plane_config.get_config.cache_clear()


if __name__ == "__main__":
    unittest.main()
