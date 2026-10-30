from __future__ import annotations

from collections.abc import Iterable
from dataclasses import replace
import types
import unittest
from unittest.mock import patch

from midstream.protection.policy import ProtectionPolicy  # noqa: E402
from midstream.config import ControlPlaneConfig
from midstream.handlers import handle_request_event, handle_request_event_for_output
from midstream.models import (
    Materialization,
    MaterializationRequestEvent,
    MaterializationRequestSpec,
    MaterializationStatus,
    ResourceObservation,
    RuntimeHealth,
)
from midstream.models import RequestedReprotect, TopicChangeRequest
from midstream.services import IntakeResult, process_request_event
from midstream.services.workflow import is_live_drop
from midstream.storage import (
    get_materialization,
    get_materialization_status,
    get_materialization_table,
    list_materialization_tables,
    save_materialization,
    save_materialization_status,
    save_materialization_table,
)
from midstream.storage.identity import identity_routing_key
from midstream.storage.keys import materialization_routing_key
from table_records import build_table_record, table_spec


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


ORDERS = table_spec("orders", "orders")
PAYMENTS = table_spec("payments", "payments", dataset="finance", dataset_id="ds-000000000002")
REFUNDS = table_spec("refunds", "refunds", dataset="finance", dataset_id="ds-000000000002")


def build_materialization(materialization_id: str, *, desired_phase: str) -> Materialization:
    return Materialization(
        materialization_id=materialization_id,
        name="Orders",
        tables=(ORDERS,),
        destination_uri="gs://lakehouse/orders",
        runtime_mode="dedicated",
        resolved_runtime_name="kaspr-mat-1",
        namespace="midstream",
        desired_phase=desired_phase,
        created_at="2026-07-12T00:00:00Z",
        updated_at="2026-07-12T00:00:00Z",
        created_by="alice",
    )


def build_status(materialization_id: str, *, desired_phase: str, observed_phase: str) -> MaterializationStatus:
    return MaterializationStatus(
        materialization_id=materialization_id,
        desired_phase=desired_phase,
        observed_phase=observed_phase,
        phase_reason=observed_phase,
        last_transition_time="2026-07-12T00:01:00Z",
        observation_source="hybrid",
        runtime_health=RuntimeHealth(level="unknown"),
        resource_observation=ResourceObservation(
            namespace="midstream",
            kaspr_app_name="kaspr-mat-1",
            resource_exists=False,
            managed_agent_states={},
        ),
    )


def build_request_event(materialization_id: str, *, action: str, name: str | None = "Orders") -> MaterializationRequestEvent:
    return MaterializationRequestEvent(
        request_id=f"req-{action}",
        materialization_id=materialization_id,
        action=action,
        submitted_at="2026-07-12T00:02:00Z",
        submitted_by="alice@example.com",
        submitted_by_id="usr-000000000001",
        spec=MaterializationRequestSpec(
            name=name,
            tables=(ORDERS, PAYMENTS),
            destination_uri="gs://lakehouse/orders",
            runtime_mode="dedicated",
            runtime_name="kaspr-mat-1",
            namespace="midstream",
        ),
        claim_request_id="idr-claim000001" if action == "create" else None,
    )


def steps(result: IntakeResult) -> list[str]:
    return [event.step for event in result.work]


class RequestIntakeServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.config = ControlPlaneConfig()
        self.table = FakeTable()

    def test_create_saves_the_materialization_and_claims_its_tables_names(self) -> None:
        result = process_request_event(self.table, build_request_event("mat-1", action="create"), config=self.config)

        materialization = get_materialization(self.table, "mat-1")
        status = get_materialization_status(self.table, "mat-1")
        # Nothing goes to the workflow yet: the directory's answer to the claim does.
        self.assertEqual(result.work, ())
        (claim,) = result.identity
        self.assertEqual("claim_tables", claim.action)
        self.assertEqual("idr-claim000001", claim.request_id)
        self.assertEqual(("usr-000000000001", "alice@example.com"), (claim.actor_id, claim.actor_email))
        self.assertEqual("create", claim.payload["purpose"])
        self.assertEqual("req-create", claim.payload["materialization_request_id"])
        self.assertEqual(
            [
                {"dataset_id": "ds-000000000001", "dataset": "shop", "name": "orders", "source_topic": "orders"},
                {"dataset_id": "ds-000000000002", "dataset": "finance", "name": "payments", "source_topic": "payments"},
            ],
            claim.payload["tables"],
        )
        self.assertEqual(materialization.desired_phase, "running")
        self.assertEqual((ORDERS, PAYMENTS), materialization.tables)
        self.assertEqual("Orders", materialization.name)
        self.assertEqual((status.observed_phase, status.phase_reason), ("requested", "claiming_tables"))
        self.assertIn("shop.orders, finance.payments", status.phase_message)

    def test_a_create_without_a_label_is_labelled_with_its_id(self) -> None:
        process_request_event(self.table, build_request_event("mat-1", action="create", name=None), config=self.config)

        self.assertEqual("mat-1", get_materialization(self.table, "mat-1").name)

    def test_the_claim_is_routed_to_the_directory_on_its_one_key(self) -> None:
        outputs = list(handle_request_event_for_output(build_request_event("mat-1", action="create").to_dict(), self.table, config=self.config))

        self.assertEqual(["identity"], [output["route"] for output in outputs])
        self.assertEqual(identity_routing_key(), outputs[0]["key"])
        self.assertEqual("claim_tables", outputs[0]["value"]["action"])

    def test_the_output_handler_persists_before_returning_a_generator(self) -> None:
        outputs = handle_request_event_for_output(build_request_event("mat-1", action="create").to_dict(), self.table, config=self.config)

        # Kaspr sends each value a generator yields, and a list as one value.
        self.assertIsInstance(outputs, types.GeneratorType)
        self.assertIsNotNone(get_materialization(self.table, "mat-1"))

    def test_a_create_delivered_again_after_its_claim_was_answered_hands_nothing_on(self) -> None:
        process_request_event(self.table, build_request_event("mat-1", action="create"), config=self.config)
        save_materialization_status(self.table, build_status("mat-1", desired_phase="running", observed_phase="discovering"))

        result = process_request_event(self.table, build_request_event("mat-1", action="create"), config=self.config)

        self.assertEqual(IntakeResult(), result)

    def test_a_create_delivered_again_before_the_answer_claims_again(self) -> None:
        first = process_request_event(self.table, build_request_event("mat-1", action="create"), config=self.config)
        again = process_request_event(self.table, build_request_event("mat-1", action="create"), config=self.config)

        # The same request: the directory answers it with the outcome it stored.
        self.assertEqual(first.identity, again.identity)

    def test_create_request_persists_requested_runtime_scale(self) -> None:
        request_event = build_request_event("mat-1", action="create")
        scaled_request = replace(request_event, spec=replace(request_event.spec, storage_size="8Gi", replicas=16))

        process_request_event(self.table, scaled_request, config=self.config)

        materialization = get_materialization(self.table, "mat-1")
        self.assertEqual(materialization.storage_size, "8Gi")
        self.assertEqual(materialization.replicas, 16)

    def test_create_request_persists_skip_tombstones(self) -> None:
        request_event = build_request_event("mat-1", action="create")

        process_request_event(self.table, replace(request_event, spec=replace(request_event.spec, skip_tombstones=True)), config=self.config)

        self.assertTrue(get_materialization(self.table, "mat-1").skip_tombstones)

    def test_a_4_1_create_that_asked_for_no_live_tail_is_created_like_any_other(self) -> None:
        # Every materialization has a live tail now; a create accepted before 4.3 still applies.
        emitted = build_request_event("mat-1", action="create").to_dict()
        emitted["spec"]["live_tail"] = False

        process_request_event(self.table, MaterializationRequestEvent.from_dict(emitted), config=self.config)

        self.assertNotIn("live_tail", get_materialization(self.table, "mat-1").to_dict())

    def test_create_request_defaults_runtime_scale_when_unspecified(self) -> None:
        process_request_event(self.table, build_request_event("mat-1", action="create"), config=self.config)

        materialization = get_materialization(self.table, "mat-1")
        self.assertEqual(materialization.storage_size, "64Mi")
        self.assertEqual(materialization.replicas, 1)

    def test_legacy_pause_request_still_routes_to_apply_runtime(self) -> None:
        """No endpoint requests a pause any more, but one already on the topic must still decode."""
        save_materialization(self.table, build_materialization("mat-1", desired_phase="paused"))
        save_materialization_status(self.table, build_status("mat-1", desired_phase="paused", observed_phase="running"))

        result = process_request_event(self.table, build_request_event("mat-1", action="pause"), config=self.config, work_id="work-pause")

        self.assertEqual(["apply_runtime"], steps(result))
        self.assertEqual((), result.identity)

    def test_stop_request_persists_the_stopped_phase_and_routes_to_apply_runtime(self) -> None:
        save_materialization(self.table, replace(build_materialization("mat-1", desired_phase="running"), replicas=4))
        save_materialization_status(self.table, build_status("mat-1", desired_phase="running", observed_phase="running"))

        result = process_request_event(self.table, build_request_event("mat-1", action="stop"), config=self.config, work_id="work-stop")

        materialization = get_materialization(self.table, "mat-1")
        self.assertEqual(["apply_runtime"], steps(result))
        self.assertEqual("work-stop", result.work[0].work_id)
        self.assertEqual(materialization.desired_phase, "stopped")
        # Stopping changes the phase only; the scale it resumes to is kept.
        self.assertEqual(materialization.replicas, 4)
        self.assertEqual(get_materialization_status(self.table, "mat-1").desired_phase, "stopped")

    def _update_request(self, replicas: int) -> MaterializationRequestEvent:
        request_event = build_request_event("mat-1", action="update")
        return replace(request_event, spec=replace(request_event.spec, replicas=replicas))

    def test_update_request_applies_the_replica_count_and_keeps_the_phase(self) -> None:
        for desired_phase in ("running", "stopped"):
            with self.subTest(desired_phase=desired_phase):
                save_materialization(self.table, replace(build_materialization("mat-1", desired_phase=desired_phase), replicas=4))
                save_materialization_status(self.table, build_status("mat-1", desired_phase=desired_phase, observed_phase=desired_phase))

                result = process_request_event(self.table, self._update_request(8), config=self.config, work_id="work-update")

                materialization = get_materialization(self.table, "mat-1")
                self.assertEqual(["apply_runtime"], steps(result))
                self.assertEqual(materialization.replicas, 8)
                self.assertEqual(materialization.spec_version, 2)
                self.assertEqual(materialization.desired_phase, desired_phase)
                self.assertEqual(get_materialization_status(self.table, "mat-1").desired_phase, desired_phase)

    def test_a_new_label_applies_at_once_and_touches_nothing_else(self) -> None:
        save_materialization(self.table, replace(build_materialization("mat-1", desired_phase="running"), replicas=4))
        save_materialization_status(self.table, build_status("mat-1", desired_phase="running", observed_phase="running"))
        request_event = replace(self._update_request(4), label="Sales orders (EU)")

        result = process_request_event(self.table, request_event, config=self.config)

        materialization = get_materialization(self.table, "mat-1")
        self.assertEqual("Sales orders (EU)", materialization.name)
        # The runtime knows nothing of the label: nothing is applied, and the spec has not changed.
        self.assertEqual(IntakeResult(), result)
        self.assertEqual(1, materialization.spec_version)

    def test_a_4_1_update_that_switched_the_live_tail_changes_nothing(self) -> None:
        save_materialization(self.table, replace(build_materialization("mat-1", desired_phase="running"), replicas=4))
        save_materialization_status(self.table, build_status("mat-1", desired_phase="running", observed_phase="running"))
        before = get_materialization(self.table, "mat-1")
        emitted = self._update_request(4).to_dict()
        emitted["live_tail"] = False
        emitted["spec"]["live_tail"] = False

        result = process_request_event(self.table, MaterializationRequestEvent.from_dict(emitted), config=self.config)

        self.assertEqual(IntakeResult(), result)
        after = get_materialization(self.table, "mat-1")
        # Stamped as updated, and nothing else: no new spec, nothing re-applied, no live tail switch.
        self.assertEqual(before, replace(after, updated_at=before.updated_at))
        self.assertNotIn("live_tail", after.to_dict())

    def test_an_update_without_a_label_keeps_the_label(self) -> None:
        save_materialization(self.table, replace(build_materialization("mat-1", desired_phase="running"), replicas=4))
        request_event = self._update_request(8)
        # A request carries the spec as the view read it, label included; only `label` changes it.
        request_event = replace(request_event, spec=replace(request_event.spec, name="stale"))

        process_request_event(self.table, request_event, config=self.config)

        self.assertEqual("Orders", get_materialization(self.table, "mat-1").name)

    def test_an_accepted_update_applies_even_when_the_maximum_was_lowered_since(self) -> None:
        save_materialization(self.table, replace(build_materialization("mat-1", desired_phase="running"), replicas=4))

        process_request_event(self.table, self._update_request(8), config=replace(self.config, max_runtime_replicas=6))

        self.assertEqual(get_materialization(self.table, "mat-1").replicas, 8)

    def test_redelivered_update_request_does_not_count_its_change_twice(self) -> None:
        save_materialization(self.table, replace(build_materialization("mat-1", desired_phase="running"), replicas=4))

        for _ in range(2):
            process_request_event(self.table, self._update_request(8), config=self.config)

        materialization = get_materialization(self.table, "mat-1")
        self.assertEqual(materialization.replicas, 8)
        self.assertEqual(materialization.spec_version, 2)

    def test_update_request_rejects_a_materialization_being_deleted(self) -> None:
        save_materialization(self.table, replace(build_materialization("mat-1", desired_phase="deleting"), replicas=4))

        with self.assertRaisesRegex(ValueError, "deleting"):
            process_request_event(self.table, self._update_request(8), config=self.config)

        self.assertEqual(get_materialization(self.table, "mat-1").replicas, 4)

    def test_delete_deletes_the_runtime_and_frees_every_name_it_holds(self) -> None:
        save_materialization(self.table, build_materialization("mat-1", desired_phase="running"))
        save_materialization_status(self.table, build_status("mat-1", desired_phase="running", observed_phase="running"))

        result = process_request_event(self.table, build_request_event("mat-1", action="delete"), config=self.config)

        self.assertEqual(["delete_runtime"], steps(result))
        (release,) = result.identity
        self.assertEqual("release_tables", release.action)
        self.assertEqual({"materialization_id": "mat-1", "reason": "deleted", "all": True}, release.payload)
        self.assertEqual("usr-000000000001", release.actor_id)
        self.assertEqual("deleting", get_materialization(self.table, "mat-1").desired_phase)
        outputs = result.outputs()
        self.assertEqual(["identity", "work"], [output["route"] for output in outputs])
        self.assertEqual(materialization_routing_key("mat-1"), outputs[1]["key"])

    def test_request_intake_handler_parses_dict(self) -> None:
        result = handle_request_event(build_request_event("mat-1", action="create").to_dict(), self.table, config=self.config)

        self.assertEqual(["claim_tables"], [request.action for request in result.identity])

    def test_process_request_event_emits_protocol_log_when_enabled(self) -> None:
        config = ControlPlaneConfig(protocol_logging_enabled=True)

        with patch("midstream.services.request_intake.LOGGER") as logger:
            process_request_event(self.table, build_request_event("mat-1", action="create"), config=config)

        self.assertTrue(logger.info.called)
        logged = "".join(str(arg) for call in logger.info.call_args_list for arg in call.args)
        self.assertIn("control_plane_request_intake_completed", logged)
        self.assertIn("claim_tables", logged)


class ProtectionIntakeTests(unittest.TestCase):
    """A create stores its resolved policy at revision 1; an edit replaces it from the revision it read,
    and the runtime is applied - which delivers it - with its spec unchanged (design §5.8)."""

    POLICY = ProtectionPolicy({"email": "hash", "secret": "redact"})

    def setUp(self) -> None:
        self.config = ControlPlaneConfig()
        self.table = FakeTable()

    def _running(self, **changes) -> None:
        materialization = replace(build_materialization("mat-1", desired_phase="running"), replicas=4, **changes)
        save_materialization(self.table, materialization)
        save_materialization_status(self.table, build_status("mat-1", desired_phase="running", observed_phase="running"))

    def _edit(self, policy: ProtectionPolicy, *, base: int, apply_rules: bool = False, request_id: str = "req-update") -> MaterializationRequestEvent:
        event = build_request_event("mat-1", action="update")
        return replace(
            event,
            request_id=request_id,
            submitted_by="owner@example.com",
            spec=replace(event.spec, replicas=4, protection=policy, protection_base_revision=base, apply_protection_rules=apply_rules),
        )

    def test_a_create_stores_its_policy_at_revision_one(self) -> None:
        event = build_request_event("mat-1", action="create")
        event = replace(event, spec=replace(event.spec, protection=self.POLICY))
        process_request_event(self.table, event, config=self.config)
        materialization = get_materialization(self.table, "mat-1")
        self.assertEqual((self.POLICY, 1, "alice@example.com"), (materialization.protection, materialization.protection_revision, materialization.protection_changed_by))

    def test_a_create_from_before_data_protection_protects_nothing(self) -> None:
        process_request_event(self.table, build_request_event("mat-1", action="create"), config=self.config)
        materialization = get_materialization(self.table, "mat-1")
        self.assertTrue(materialization.protection.empty)
        self.assertEqual(0, materialization.protection_revision)

    def test_an_edit_raises_the_revision_and_applies_the_runtime_without_a_new_spec(self) -> None:
        self._running(protection=self.POLICY, protection_revision=2)
        result = process_request_event(self.table, self._edit(ProtectionPolicy({"phone": "partial"}), base=2), config=self.config, work_id="work-update")
        materialization = get_materialization(self.table, "mat-1")
        self.assertEqual(["apply_runtime"], steps(result))
        self.assertEqual((ProtectionPolicy({"phone": "partial"}), 3), (materialization.protection, materialization.protection_revision))
        self.assertEqual("owner@example.com", materialization.protection_changed_by)
        self.assertIsNone(materialization.protection_reason)
        self.assertEqual(1, materialization.spec_version)

    def test_an_edit_delivered_again_changes_nothing(self) -> None:
        self._running(protection=self.POLICY, protection_revision=2)
        edit = self._edit(ProtectionPolicy({"phone": "partial"}), base=2)
        process_request_event(self.table, edit, config=self.config)
        again = process_request_event(self.table, edit, config=self.config)
        self.assertEqual([], steps(again))
        self.assertEqual(3, get_materialization(self.table, "mat-1").protection_revision)

    def test_an_edit_overtaken_by_another_is_refused_and_says_so(self) -> None:
        self._running(protection=self.POLICY, protection_revision=2)
        process_request_event(self.table, self._edit(ProtectionPolicy({"phone": "partial"}), base=2, request_id="req-a"), config=self.config)
        with self.assertLogs("midstream.services.request_intake", level="WARNING"):
            result = process_request_event(self.table, self._edit(ProtectionPolicy({}), base=2, request_id="req-b"), config=self.config)
        self.assertEqual([], steps(result))
        materialization = get_materialization(self.table, "mat-1")
        self.assertEqual((ProtectionPolicy({"phone": "partial"}), 3), (materialization.protection, materialization.protection_revision))
        self.assertIn("another change of protection", get_materialization_status(self.table, "mat-1").phase_message)

    def test_applying_the_rules_records_the_same_policy_again(self) -> None:
        self._running(protection=self.POLICY, protection_revision=2)
        result = process_request_event(self.table, self._edit(self.POLICY, base=2, apply_rules=True), config=self.config)
        materialization = get_materialization(self.table, "mat-1")
        self.assertEqual(["apply_runtime"], steps(result))
        self.assertEqual((self.POLICY, 3, "apply_rules"), (materialization.protection, materialization.protection_revision, materialization.protection_reason))

    def test_an_update_without_protection_leaves_it_alone(self) -> None:
        self._running(protection=self.POLICY, protection_revision=2)
        process_request_event(self.table, replace(self._edit(self.POLICY, base=2), spec=replace(self._edit(self.POLICY, base=2).spec, protection=None, replicas=8)), config=self.config)
        materialization = get_materialization(self.table, "mat-1")
        self.assertEqual((self.POLICY, 2, 8), (materialization.protection, materialization.protection_revision, materialization.replicas))


class ReprotectIntakeTests(unittest.TestCase):
    """A reprotect - asked alone, or with an edit - goes on the records of the tables it names, where
    every maintenance tick carries it until the maintenance app has run it (handbook 22 §22.14)."""

    POLICY = ProtectionPolicy({"email": "hash"})

    def setUp(self) -> None:
        self.config = ControlPlaneConfig()
        self.table = FakeTable()
        materialization = replace(
            build_materialization("mat-1", desired_phase="running"),
            retired_tables=(PAYMENTS,),
            protection=self.POLICY,
            protection_revision=2,
            replicas=4,
        )
        save_materialization(self.table, materialization)
        save_materialization_status(self.table, build_status("mat-1", desired_phase="running", observed_phase="running"))
        save_materialization_table(self.table, build_table_record(materialization, table="orders"))
        save_materialization_table(self.table, build_table_record(materialization, table="payments", state="retired"))

    def _reprotect(self, *, tables: tuple[str, ...] | None = None, purge: bool = False, request_id: str = "req-reprotect") -> MaterializationRequestEvent:
        return replace(
            build_request_event("mat-1", action="reprotect"),
            request_id=request_id,
            reprotect=RequestedReprotect(tables=tables, purge=purge),
        )

    def _asked(self, name: str):
        return get_materialization_table(self.table, "mat-1", name).reprotect

    def test_a_reprotect_goes_on_the_record_of_every_table_kept(self) -> None:
        result = process_request_event(self.table, self._reprotect(purge=True), config=self.config, work_id="work-reprotect")

        self.assertEqual([], steps(result))
        orders = self._asked("orders")
        self.assertEqual(("req-reprotect", "alice@example.com", 2, True), (orders.request_id, orders.requested_by, orders.revision, orders.purge))
        # A kept table, which no runtime writes, waits for no revision.
        self.assertEqual(0, self._asked("payments").revision)
        # The materialization itself is left as it was.
        materialization = get_materialization(self.table, "mat-1")
        self.assertEqual((2, 1, "running"), (materialization.protection_revision, materialization.spec_version, materialization.desired_phase))

    def test_a_reprotect_may_name_one_table_and_a_newer_one_replaces_it(self) -> None:
        process_request_event(self.table, self._reprotect(tables=("payments",)), config=self.config)
        self.assertIsNone(self._asked("orders"))
        process_request_event(self.table, self._reprotect(tables=("payments",), purge=True, request_id="req-again"), config=self.config)
        self.assertEqual(("req-again", True), (self._asked("payments").request_id, self._asked("payments").purge))

    def test_a_reprotect_of_a_materialization_being_deleted_is_dropped(self) -> None:
        save_materialization(self.table, replace(get_materialization(self.table, "mat-1"), desired_phase="deleting"))
        process_request_event(self.table, self._reprotect(), config=self.config)
        self.assertEqual((None, None), (self._asked("orders"), self._asked("payments")))

    def test_an_edit_that_asks_for_a_reprotect_waits_for_its_own_revision(self) -> None:
        event = build_request_event("mat-1", action="update")
        edit = replace(
            event,
            submitted_by="owner@example.com",
            spec=replace(event.spec, replicas=4, protection=ProtectionPolicy({"email": "partial"}), protection_base_revision=2),
            reprotect=RequestedReprotect(purge=True),
        )
        process_request_event(self.table, edit, config=self.config)
        self.assertEqual(3, get_materialization(self.table, "mat-1").protection_revision)
        self.assertEqual((3, True, "owner@example.com"), (self._asked("orders").revision, self._asked("orders").purge, self._asked("orders").requested_by))

    def test_an_edit_refused_as_overtaken_asks_for_no_reprotect(self) -> None:
        event = build_request_event("mat-1", action="update")
        stale = replace(
            event,
            spec=replace(event.spec, replicas=4, protection=ProtectionPolicy({}), protection_base_revision=1),
            reprotect=RequestedReprotect(),
        )
        with self.assertLogs("midstream.services.request_intake", level="WARNING"):
            process_request_event(self.table, stale, config=self.config)
        self.assertEqual((None, None), (self._asked("orders"), self._asked("payments")))

    def test_the_request_event_round_trips_its_reprotect(self) -> None:
        event = self._reprotect(tables=("orders",), purge=True)
        self.assertEqual(event, MaterializationRequestEvent.from_dict(event.to_dict()))
        self.assertNotIn("reprotect", build_request_event("mat-1", action="update").to_dict())


class TopicChangeIntakeTests(unittest.TestCase):
    """An update that changes the topics: intake plans it again against the records as they are.

    A change that adds tables waits for their names; one that adds none applies at once."""

    def setUp(self) -> None:
        self.config = ControlPlaneConfig()
        self.table = FakeTable()
        materialization = replace(build_materialization("mat-1", desired_phase="running"), tables=(ORDERS, PAYMENTS))
        save_materialization(self.table, materialization)
        for name in materialization.table_names:
            save_materialization_table(self.table, build_table_record(materialization, table=name))
        save_materialization_status(self.table, build_status("mat-1", desired_phase="running", observed_phase="running"))

    def _change(self, topics, actions=None, *, request_id: str = "req-topics", new=(), replicas: int | None = None) -> IntakeResult:
        request_event = build_request_event("mat-1", action="update")
        request_event = replace(
            request_event,
            request_id=request_id,
            spec=replace(request_event.spec, replicas=replicas),
            topic_change=TopicChangeRequest(source_topics=tuple(topics), table_actions=dict(actions or {}), new_tables=tuple(new)),
            claim_request_id=f"idr-{request_id}",
        )
        return process_request_event(self.table, request_event, config=self.config)

    def _states(self) -> dict[str, str]:
        return {record.name: record.state for record in list_materialization_tables(self.table, "mat-1")}

    def test_adding_a_topic_waits_for_its_tables_name(self) -> None:
        result = self._change(["orders", "payments", "refunds"], new=(REFUNDS,))

        materialization = get_materialization(self.table, "mat-1")
        # Nothing changes until the name is claimed.
        self.assertEqual(materialization.table_names, ("orders", "payments"))
        self.assertEqual(materialization.spec_version, 1)
        pending = materialization.pending_topic_change
        self.assertEqual(("req-topics", "idr-req-topics"), (pending.request_id, pending.claim_request_id))
        self.assertEqual((REFUNDS,), pending.plan.added)
        self.assertEqual((), result.work)
        (claim,) = result.identity
        self.assertEqual(("claim_tables", "idr-req-topics", "update"), (claim.action, claim.request_id, claim.payload["purpose"]))
        self.assertEqual(["refunds"], [table["name"] for table in claim.payload["tables"]])
        self.assertEqual([], claim.payload["release"])
        self.assertEqual(
            "Claiming the table names finance.refunds in their datasets",
            get_materialization_status(self.table, "mat-1").phase_message,
        )

    def test_a_change_that_adds_and_drops_frees_the_dropped_names_with_the_claim(self) -> None:
        result = self._change(["orders", "refunds"], {"payments": "drop"}, new=(REFUNDS,))

        (claim,) = result.identity
        self.assertEqual(["refunds"], [table["name"] for table in claim.payload["tables"]])
        self.assertEqual([{"dataset_id": "ds-000000000002", "dataset": "finance", "name": "payments"}], claim.payload["release"])
        # Nothing is dropped before the claim is answered.
        self.assertEqual(self._states(), {"orders": "active", "payments": "active"})

    def test_a_new_replica_count_applies_while_a_change_of_topics_waits(self) -> None:
        result = self._change(["orders", "payments", "refunds"], new=(REFUNDS,), replicas=3)

        materialization = get_materialization(self.table, "mat-1")
        self.assertEqual(["apply_runtime"], steps(result))
        self.assertEqual(["claim_tables"], [request.action for request in result.identity])
        self.assertEqual((3, 2), (materialization.replicas, materialization.spec_version))

    def test_only_one_change_of_topics_waits_at_a_time(self) -> None:
        self._change(["orders", "payments", "refunds"], new=(REFUNDS,))

        result = self._change(["orders"], {"payments": "drop"}, request_id="req-second")

        self.assertEqual(IntakeResult(), result)
        self.assertEqual(self._states(), {"orders": "active", "payments": "active"})
        message = get_materialization_status(self.table, "mat-1").phase_message
        self.assertTrue(message.startswith("The change of topics was not applied:"), message)
        self.assertIn("still waiting", message)

    def test_a_waiting_change_delivered_again_claims_again(self) -> None:
        first = self._change(["orders", "payments", "refunds"], new=(REFUNDS,))
        again = self._change(["orders", "payments", "refunds"], new=(REFUNDS,))

        self.assertEqual(first.identity, again.identity)
        self.assertEqual("req-topics", get_materialization(self.table, "mat-1").pending_topic_change.request_id)

    def test_removing_a_topic_marks_its_table_and_reapplies_the_runtime(self) -> None:
        for action, state, retired in (("drop", "dropping", ()), ("keep", "retired", ("payments",))):
            with self.subTest(action=action):
                self.setUp()

                result = self._change(["orders"], {"payments": action})

                materialization = get_materialization(self.table, "mat-1")
                self.assertEqual(["apply_runtime"], steps(result))
                self.assertEqual(materialization.table_names, ("orders",))
                self.assertEqual(tuple(spec.name for spec in materialization.retired_tables), retired)
                self.assertEqual(self._states(), {"orders": "active", "payments": state})
                # A dropped table leaves the catalog now, and frees its name; a kept one keeps it.
                releases = [request for request in result.identity if request.action == "release_tables"]
                self.assertEqual(1 if action == "drop" else 0, len(releases))
                if releases:
                    self.assertEqual(
                        {"materialization_id": "mat-1", "reason": "dropped", "tables": [{"dataset_id": "ds-000000000002", "dataset": "finance", "name": "payments"}]},
                        releases[0].payload,
                    )

    def test_dropping_a_kept_table_drops_it_at_once_and_frees_its_name(self) -> None:
        self._change(["orders"], {"payments": "keep"})

        result = self._change(["orders"], {"payments": "drop"}, request_id="req-drop")

        (work_event,) = result.work
        self.assertEqual(work_event.step, "drop_table")
        self.assertTrue(is_live_drop(work_event))
        self.assertEqual(work_event.caused_by.id, "req-drop")
        self.assertEqual(["release_tables"], [request.action for request in result.identity])
        self.assertEqual(get_materialization(self.table, "mat-1").retired_tables, ())
        self.assertEqual(self._states()["payments"], "dropping")

    def test_adding_a_kept_tables_topic_back_writes_it_again_under_its_name(self) -> None:
        self._change(["orders"], {"payments": "keep"})

        result = self._change(["orders", "payments"], request_id="req-resume")

        materialization = get_materialization(self.table, "mat-1")
        # It keeps its name, so nothing is claimed.
        self.assertEqual(["apply_runtime"], steps(result))
        self.assertEqual((), result.identity)
        self.assertEqual(materialization.tables, (ORDERS, PAYMENTS))
        self.assertEqual(materialization.retired_tables, ())
        self.assertEqual(self._states()["payments"], "active")
        self.assertEqual(materialization.spec_version, 3)

    def test_a_redelivered_change_is_applied_once(self) -> None:
        for _ in range(2):
            self._change(["orders"], {"payments": "drop"})

        materialization = get_materialization(self.table, "mat-1")
        self.assertEqual(materialization.table_names, ("orders",))
        self.assertEqual(materialization.spec_version, 2)
        self.assertEqual(self._states()["payments"], "dropping")

    def test_a_change_planned_against_older_records_is_refused_at_intake(self) -> None:
        """Two updates raced: the second removes a topic the first left, and says nothing of its table."""
        self._change(["orders"], {"payments": "keep"})

        result = self._change(["refunds"], request_id="req-late", new=(REFUNDS,))

        self.assertEqual(IntakeResult(), result)
        self.assertEqual(get_materialization(self.table, "mat-1").table_names, ("orders",))
        self.assertEqual(get_materialization_table(self.table, "mat-1", "orders").state, "active")
        message = get_materialization_status(self.table, "mat-1").phase_message
        self.assertTrue(message.startswith("The change of topics was not applied:"), message)
        self.assertIn("orders", message)


if __name__ == "__main__":
    unittest.main()
