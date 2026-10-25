"""Version reporting across the control plane and its runtimes."""

from __future__ import annotations

import unittest

from materializer_control_plane.models.events import RuntimeStatusEvent
from materializer_control_plane.models.status import (
    MaterializationStatus,
    ResourceCondition,
    ResourceObservation,
    RuntimeHealth,
)
from materializer_control_plane.version import PACKAGE_VERSION, UNKNOWN_VERSION, package_version
from materializer_control_plane.web.system import version_view


def build_event(**overrides) -> RuntimeStatusEvent:
    base = {
        "event_id": "evt-1",
        "materialization_id": "mat-1",
        "runtime_app_ref": "kaspr-mat-1",
        "member_id": "kaspr-mat-1-app-0",
        "agent_name": "writer",
        "phase": "running",
        "health": "healthy",
        "reported_at": "2026-09-12T00:00:00Z",
    }
    return RuntimeStatusEvent(**{**base, **overrides})


class PackageVersionTests(unittest.TestCase):
    def test_version_is_resolved_from_distribution_metadata(self) -> None:
        """`pyproject.toml` stays the single source of truth for the shipped version."""
        self.assertEqual(package_version(), PACKAGE_VERSION)
        self.assertNotEqual(PACKAGE_VERSION, UNKNOWN_VERSION)

    def test_control_plane_reports_its_component_and_version(self) -> None:
        self.assertEqual(
            version_view(),
            {"component": "control-plane", "version": PACKAGE_VERSION},
        )


class RuntimeVersionReportingTests(unittest.TestCase):
    def test_status_event_round_trips_the_runtime_version(self) -> None:
        event = build_event(runtime_version="1.4.2")
        self.assertEqual(RuntimeStatusEvent.from_dict(event.to_dict()).runtime_version, "1.4.2")

    def test_status_event_without_a_version_is_accepted(self) -> None:
        """A runtime on an older build predates the field and must still report status."""
        event = RuntimeStatusEvent.from_dict(build_event().to_dict())
        self.assertIsNone(event.runtime_version)

    def test_status_record_round_trips_the_runtime_version(self) -> None:
        status = MaterializationStatus(
            materialization_id="mat-1",
            desired_phase="running",
            observed_phase="running",
            phase_reason="processing",
            last_transition_time="2026-09-12T00:00:00Z",
            observation_source="hybrid",
            runtime_health=RuntimeHealth(level="healthy"),
            resource_observation=ResourceObservation(
                namespace="midstream",
                kaspr_app_name="kaspr-mat-1",
                resource_exists=True,
                managed_agent_states=[],
            ),
            runtime_version="1.4.2",
        )
        restored = MaterializationStatus.from_dict(status.to_dict())
        self.assertEqual(restored.runtime_version, "1.4.2")
        self.assertIsNone(
            MaterializationStatus.from_dict(
                {k: v for k, v in status.to_dict().items() if k != "runtime_version"}
            ).runtime_version,
            "a status written before this field existed must still load",
        )


class DeletedMaterializationStaysDeletedTests(unittest.TestCase):
    """A deleted materialization must not read as alive anywhere."""

    def _materialization(self, desired_phase: str):
        from materializer_control_plane.models import Materialization, TableSpec

        return Materialization(
            materialization_id="mat-1",
            name="orders",
            tables=(TableSpec(name="orders", source_topic="orders", dataset="shop", dataset_id="ds-000000000001"),),
            destination_uri="gs://lakehouse/orders",
            runtime_mode="dedicated",
            resolved_runtime_name="kaspr-mat-1",
            namespace="midstream",
            desired_phase=desired_phase,
            created_at="2026-09-12T00:00:00Z",
            updated_at="2026-09-12T03:27:00Z",
            created_by="vmirzoyan",
        )

    def _status(self, observed_phase: str, phase_reason: str) -> MaterializationStatus:
        return MaterializationStatus(
            materialization_id="mat-1",
            desired_phase="running",
            observed_phase=observed_phase,
            phase_reason=phase_reason,
            last_transition_time="2026-09-12T00:00:00Z",
            observation_source="hybrid",
            runtime_health=RuntimeHealth(level="healthy"),
            resource_observation=ResourceObservation(
                namespace="midstream",
                kaspr_app_name="kaspr-mat-1",
                resource_exists=True,
                managed_agent_states={},
                # A ready runtime is what lets a healthy status event set `running`; without it the
                # resurrection path in apply_runtime_status_event is never reached.
                ready_condition=ResourceCondition(
                    status="True",
                    reason="Ready",
                    message=None,
                    last_transition_time="2026-09-12T00:00:00Z",
                ),
            ),
        )

    def test_stale_running_status_reads_as_deleted(self) -> None:
        from materializer_control_plane.storage.materializations import observed_phase_for

        phase, reason = observed_phase_for(
            self._materialization("deleted"), self._status("running", "processing")
        )
        self.assertEqual(phase, "deleted")
        self.assertEqual(reason, "runtime_removed")

    def test_a_live_materialization_keeps_its_reported_status(self) -> None:
        from materializer_control_plane.storage.materializations import observed_phase_for

        phase, reason = observed_phase_for(
            self._materialization("running"), self._status("running", "processing")
        )
        self.assertEqual((phase, reason), ("running", "processing"))

    def test_runtime_status_cannot_revive_a_deleting_materialization(self) -> None:
        """Late or replayed status events are the mechanism that resurrected them."""
        from materializer_control_plane.services.observation import apply_runtime_status_event
        from materializer_control_plane.storage import (
            save_materialization,
            save_materialization_status,
        )
        from test_storage import FakeTable

        table = FakeTable()
        save_materialization(table, self._materialization("deleted"))
        save_materialization_status(
            table, self._status("deleted", "runtime_and_kafka_resources_deleted")
        )

        result = apply_runtime_status_event(
            table, build_event(phase="running", health="healthy", runtime_version="0.1.0")
        )

        self.assertEqual(result.observed_phase, "deleted")
        self.assertEqual(result.phase_reason, "runtime_and_kafka_resources_deleted")
