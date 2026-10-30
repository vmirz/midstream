"""The table-events agent keeps the control plane's records in step with what runtimes did (spec §9.2)."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import replace
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from midstream.handlers import handle_runtime_status, handle_table_event  # noqa: E402
from midstream.models import (  # noqa: E402
    Materialization,
    MaterializationStatus,
    ResourceCondition,
    ResourceObservation,
    RuntimeHealth,
)
from midstream.storage import (  # noqa: E402
    get_materialization,
    get_materialization_schema,
    get_materialization_status,
    get_materialization_table,
    list_schema_versions,
    save_materialization,
    save_materialization_status,
    save_materialization_table,
)
from midstream.models.table import ReprotectRequest  # noqa: E402
from table_records import build_table_record, table_specs  # noqa: E402

MATERIALIZATION_ID = "mat-1"


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


def build_materialization(**overrides) -> Materialization:
    values = dict(
        materialization_id=MATERIALIZATION_ID,
        namespace="midstream",
        name="shop",
        tables=table_specs("orders"),
        destination_uri="gs://lake",
        resolved_runtime_name="kaspr-mat-1",
        desired_phase="running",
        created_at="2026-10-18T00:00:00Z",
        updated_at="2026-10-18T00:00:00Z",
    )
    values.update(overrides)
    return Materialization(**values)


def build_status() -> MaterializationStatus:
    return MaterializationStatus(
        materialization_id=MATERIALIZATION_ID,
        desired_phase="running",
        observed_phase="running",
        phase_reason="processing",
        last_transition_time="2026-10-18T00:00:00Z",
        observation_source="hybrid",
        runtime_health=RuntimeHealth(level="healthy"),
        resource_observation=ResourceObservation(
            namespace="midstream",
            kaspr_app_name="kaspr-mat-1",
            resource_exists=True,
            managed_agent_states={"kaspr-mat-1-writer": "running"},
            ready_condition=ResourceCondition(status="True", reason="Healthy", message=None, last_transition_time=None),
        ),
    )


def event(
    kind: str,
    payload: dict,
    *,
    event_id: str | None = None,
    at: str = "2026-10-18T10:00:00.000Z",
    table: str = "orders",
) -> dict:
    return {
        "event_id": event_id or f"evt-{kind}-{table}-{at}",
        "kind": kind,
        "materialization_id": MATERIALIZATION_ID,
        "table": table,
        "occurred_at": at,
        "runtime_app_ref": "kaspr-mat-1",
        "member_id": "member-0",
        "payload": payload,
    }


def heartbeat(at: str) -> dict:
    return {
        "event_id": f"evt-status-{at}",
        "materialization_id": MATERIALIZATION_ID,
        "runtime_app_ref": "kaspr-mat-1",
        "member_id": "member-0",
        "agent_name": "writer",
        "phase": "running",
        "health": "healthy",
        "phase_reason": "processing",
        "phase_message": "Runtime is actively processing source records.",
        "latest_offsets": {"orders": {"0": 10}},
        "reported_at": at,
    }


def schema_changed(schema_id: int, *, at: str = "2026-10-18T10:00:00.000Z", table: str = "orders") -> dict:
    return event(
        "schema_changed",
        {
            "schema_id": schema_id,
            "schema": {"type": "struct", "schema-id": schema_id, "fields": []},
            "fork_map": {"fields": {}},
            "fields": [{"field_id": 10 + schema_id, "path": [f"f{schema_id}"], "name": f"f{schema_id}", "type": "string", "kind": "value"}],
            "field_count": 1,
            "changes": [{"kind": "added", "path": [f"f{schema_id}"]}],
            "metadata_location": f"gs://lake/t/metadata/0000{schema_id}-a.metadata.json",
            "last_column_id": 10 + schema_id,
            "rejected_paths": [],
        },
        at=at,
        table=table,
    )


class TableEventTests(unittest.TestCase):
    def setUp(self) -> None:
        self.table = FakeTable()
        save_materialization(self.table, build_materialization())
        save_materialization_status(self.table, build_status())
        save_materialization_table(self.table, build_table_record(build_materialization(), sequence_number=1, current_snapshot_id=1))

    def record(self, name: str = "orders"):
        return get_materialization_table(self.table, MATERIALIZATION_ID, name)

    def schema_changed(self, schema_id: int, *, at: str = "2026-10-18T10:00:00.000Z", table: str = "orders") -> dict:
        return schema_changed(schema_id, at=at, table=table)

    def test_a_schema_change_updates_the_projection_its_archive_and_the_materialization(self) -> None:
        self.assertTrue(handle_table_event(self.schema_changed(1), self.table))

        projection = get_materialization_schema(self.table, MATERIALIZATION_ID, "orders")
        self.assertEqual((1, "2026-10-18T10:00:00.000Z"), (projection.schema_id, projection.since))
        self.assertEqual([1], [item.schema_id for item in list_schema_versions(self.table, MATERIALIZATION_ID, "orders")])
        self.assertEqual(1, self.record().schema_id)
        self.assertEqual("gs://lake/t/metadata/00001-a.metadata.json", self.record().metadata_location)

    def test_a_late_schema_change_is_archived_without_winning(self) -> None:
        handle_table_event(self.schema_changed(2), self.table)
        # Archiving it is a change, though the projection keeps the newer schema.
        self.assertTrue(handle_table_event(self.schema_changed(1, at="2026-10-18T09:00:00.000Z"), self.table))

        self.assertEqual(2, get_materialization_schema(self.table, MATERIALIZATION_ID, "orders").schema_id)
        self.assertEqual([2, 1], [item.schema_id for item in list_schema_versions(self.table, MATERIALIZATION_ID, "orders")])

    def test_a_schema_change_delivered_again_changes_nothing(self) -> None:
        self.assertTrue(handle_table_event(self.schema_changed(1), self.table))
        self.assertFalse(handle_table_event(self.schema_changed(1), self.table))
        # A late one delivered again is old news too.
        self.assertTrue(handle_table_event(self.schema_changed(0, at="2026-10-18T09:00:00.000Z"), self.table))
        self.assertFalse(handle_table_event(self.schema_changed(0, at="2026-10-18T09:00:00.000Z"), self.table))

        projection = get_materialization_schema(self.table, MATERIALIZATION_ID, "orders")
        self.assertEqual((1, "2026-10-18T10:00:00.000Z"), (projection.schema_id, projection.since))
        self.assertEqual([1, 0], [item.schema_id for item in list_schema_versions(self.table, MATERIALIZATION_ID, "orders")])
        self.assertEqual(1, self.record().schema_id)

    def test_a_schema_change_a_commit_got_ahead_of_still_becomes_the_projection(self) -> None:
        # The commit naming schema 1 was applied first: the record has it, the projection does not.
        save_materialization_table(self.table, replace(self.record(), schema_id=1))

        self.assertTrue(handle_table_event(self.schema_changed(1), self.table))
        self.assertEqual(1, get_materialization_schema(self.table, MATERIALIZATION_ID, "orders").schema_id)
        self.assertEqual([1], [item.schema_id for item in list_schema_versions(self.table, MATERIALIZATION_ID, "orders")])

    def protected(self, schema_id: int, revision: int, *, at: str = "2026-10-18T11:00:00.000Z") -> dict:
        """A `schema_changed` carrying the table's protection, as the committer sends it."""
        value = self.schema_changed(schema_id, at=at)
        field_id = 10 + schema_id
        value["payload"]["protection"] = {
            "version": 1,
            "revision": revision,
            "policy": {"kinds": {"email": "hash"}},
            "key_id": "abcd1234",
            "fields": {str(field_id): {"path": [f"f{schema_id}"], "kind": "email", "mask": "hash" if revision < 3 else "partial", "since": schema_id}},
            "history": [{"revision": item, "policy": {"kinds": {"email": "hash"}}} for item in range(1, revision + 1)],
        }
        value["payload"]["changes"] = [] if revision > 1 else value["payload"]["changes"]
        return value

    def test_a_recorded_policy_at_the_same_schema_replaces_the_projection(self) -> None:
        self.assertTrue(handle_table_event(self.protected(1, 1), self.table))
        self.assertTrue(handle_table_event(self.protected(1, 3, at="2026-10-18T12:00:00.000Z"), self.table))

        projection = get_materialization_schema(self.table, MATERIALIZATION_ID, "orders")
        self.assertEqual((1, 3), projection.position)
        self.assertEqual({"kind": "email", "mask": "partial", "since_schema_id": 1}, projection.fields[0]["protection"])
        (archived,) = list_schema_versions(self.table, MATERIALIZATION_ID, "orders")
        self.assertEqual(3, archived.protection_revision)
        self.assertEqual("gs://lake/t/metadata/00001-a.metadata.json", self.record().metadata_location)

    def test_an_older_protection_delivered_late_never_wins(self) -> None:
        handle_table_event(self.protected(1, 3, at="2026-10-18T12:00:00.000Z"), self.table)
        self.assertFalse(handle_table_event(self.protected(1, 1), self.table))
        projection = get_materialization_schema(self.table, MATERIALIZATION_ID, "orders")
        self.assertEqual((1, 3), projection.position)
        self.assertEqual(3, list_schema_versions(self.table, MATERIALIZATION_ID, "orders")[0].protection_revision)

    def test_a_table_without_protection_has_no_annotations(self) -> None:
        handle_table_event(self.schema_changed(1), self.table)
        projection = get_materialization_schema(self.table, MATERIALIZATION_ID, "orders")
        self.assertEqual({}, projection.protection)
        self.assertNotIn("protection", projection.fields[0])

    def test_a_commit_updates_the_table_record(self) -> None:
        handle_table_event(
            event(
                "table_committed",
                {
                    "snapshot_id": 77,
                    "sequence_number": 2,
                    "committed_at": "2026-10-18T10:00:00.000Z",
                    "files_added": 3,
                    "records_added": 30,
                    "totals": {"records": 30, "files": 3, "bytes": 9000},
                    "offsets": {"0": 29},
                    "valid_through": "2026-10-18T09:59:00.000Z",
                    "duration_ms": 420,
                    "metadata_location": "gs://lake/t/metadata/00002-b.metadata.json",
                    "schema_id": 0,
                    "snapshots": 2,
                },
            ),
            self.table,
        )

        record = self.record()
        self.assertEqual((77, 2, 2), (record.current_snapshot_id, record.sequence_number, record.snapshots))
        self.assertEqual({"records": 30, "files": 3, "bytes": 9000}, record.totals)
        self.assertEqual({"0": 29}, record.offsets)
        self.assertEqual("2026-10-18T09:59:00.000Z", record.valid_through)
        self.assertEqual(420, record.last_commit_duration_ms)

    def test_a_commit_older_than_the_record_changes_nothing(self) -> None:
        save_materialization_table(self.table, replace(self.record(), sequence_number=5))

        self.assertFalse(handle_table_event(event("table_committed", {"snapshot_id": 3, "sequence_number": 4}), self.table))
        self.assertEqual(1, self.record().current_snapshot_id)

    def test_maintenance_runs_are_kept_once_each_newest_first(self) -> None:
        compaction = event(
            "maintenance_completed",
            {
                "job": "compaction",
                "outcome": "succeeded",
                "partition": "2026-10-17",
                "counts": {"inputs": 8, "outputs": 1},
                "totals": {"records": 30, "files": 1, "bytes": 8000},
                "sequence_number": 2,
                "snapshot_id": 7,
                "metadata_location": "gs://lake/t/metadata/00002-c.metadata.json",
            },
            event_id="evt-m-1",
        )
        handle_table_event(compaction, self.table)
        handle_table_event(compaction, self.table)
        handle_table_event(event("maintenance_completed", {"job": "expire_snapshots", "outcome": "skipped"}, event_id="evt-m-2"), self.table)

        record = self.record()
        self.assertEqual(["expire_snapshots", "compaction"], [run.job for run in record.maintenance])
        self.assertEqual("2026-10-17", record.maintenance[1].partition)
        self.assertEqual({"records": 30, "files": 1, "bytes": 8000}, record.totals)
        self.assertEqual((2, 7, "gs://lake/t/metadata/00002-c.metadata.json"), (record.sequence_number, record.current_snapshot_id, record.metadata_location))

    def test_a_compaction_report_a_later_commit_overtook_leaves_the_table_as_that_commit_left_it(self) -> None:
        """The maintenance app and the runtime report apart: the runtime's next commit may arrive first."""
        handle_table_event(event("table_committed", {"snapshot_id": 9, "sequence_number": 3, "totals": {"records": 33, "files": 2, "bytes": 9000}}), self.table)

        handle_table_event(
            event(
                "maintenance_completed",
                {"job": "compaction", "outcome": "succeeded", "sequence_number": 2, "snapshot_id": 7, "totals": {"records": 30, "files": 1, "bytes": 8000}, "metadata_location": "gs://lake/t/metadata/00002-c.metadata.json"},
                event_id="evt-m-late",
            ),
            self.table,
        )

        record = self.record()
        self.assertEqual("compaction", record.maintenance[0].job)
        self.assertEqual((3, 9, {"records": 33, "files": 2, "bytes": 9000}), (record.sequence_number, record.current_snapshot_id, record.totals))

    def test_a_started_job_shows_on_the_record_until_its_completion(self) -> None:
        started = event(
            "maintenance_started",
            {"job": "compaction", "started_at": "2026-10-18T10:00:00.000Z", "partition": "2026-10-17", "attempt": 1},
            event_id="evt-s-1",
        )
        self.assertTrue(handle_table_event(started, self.table))
        running = self.record().maintenance_job
        self.assertEqual(("compaction", "2026-10-17", "member-0"), (running.job, running.partition, running.member_id))
        # Another job's completion leaves it running.
        handle_table_event(event("maintenance_completed", {"job": "expire_snapshots", "outcome": "skipped"}, event_id="evt-m-x"), self.table)
        self.assertIsNotNone(self.record().maintenance_job)

        done = {"job": "compaction", "outcome": "abandoned", "started_at": "2026-10-18T10:00:00.000Z"}
        handle_table_event(event("maintenance_completed", done, event_id="evt-m-3", at="2026-10-18T10:05:00.000Z"), self.table)

        self.assertIsNone(self.record().maintenance_job)
        self.assertEqual("2026-10-18T10:00:00.000Z", self.record().maintenance[0].started_at)
        # Delivered again after its completion, the start does not bring the job back.
        self.assertFalse(handle_table_event(started, self.table))
        self.assertIsNone(self.record().maintenance_job)

    def test_a_table_being_dropped_takes_only_the_end_of_its_running_job(self) -> None:
        handle_table_event(event("maintenance_started", {"job": "orphan_cleanup", "started_at": "2026-10-18T10:00:00.000Z"}, event_id="evt-s-2"), self.table)
        save_materialization_table(self.table, replace(self.record(), state="dropping"))

        cancelled = {"job": "orphan_cleanup", "outcome": "cancelled", "started_at": "2026-10-18T10:00:00.000Z"}
        self.assertTrue(handle_table_event(event("maintenance_completed", cancelled, event_id="evt-m-4"), self.table))

        record = self.record()
        self.assertIsNone(record.maintenance_job)
        self.assertEqual((), record.maintenance)

    def test_a_table_error_degrades_the_status_until_the_next_commit(self) -> None:
        handle_table_event(event("table_error", {"reason": "commit_failed", "message": "Commits are failing: 503"}), self.table)

        status = get_materialization_status(self.table, MATERIALIZATION_ID)
        self.assertEqual(("degraded", "commit_failed"), (status.observed_phase, status.phase_reason))
        self.assertEqual("Table orders: Commits are failing: 503", status.phase_message)
        self.assertEqual("commit_failed", self.record().last_error.reason)
        self.assertEqual("Commits are failing: 503", self.record().last_error.message)

        # Heartbeats keep saying the members are fine; the table error holds.
        status = handle_runtime_status(heartbeat("2026-10-18T10:00:30Z"), self.table)
        self.assertEqual(("degraded", "commit_failed"), (status.observed_phase, status.phase_reason))

        handle_table_event(event("table_committed", {"snapshot_id": 9, "sequence_number": 3}), self.table)
        self.assertIsNone(self.record().last_error)
        self.assertEqual("running", get_materialization_status(self.table, MATERIALIZATION_ID).observed_phase)
        status = handle_runtime_status(heartbeat("2026-10-18T10:01:00Z"), self.table)
        self.assertEqual(("running", "processing"), (status.observed_phase, status.phase_reason))

    def test_a_compaction_that_succeeds_clears_its_own_failure(self) -> None:
        handle_table_event(event("table_error", {"reason": "compaction_failed", "message": "gave up"}), self.table)

        handle_table_event(event("maintenance_completed", {"job": "compaction", "outcome": "succeeded"}, event_id="evt-ok"), self.table)

        self.assertIsNone(self.record().last_error)
        self.assertNotEqual("degraded", get_materialization_status(self.table, MATERIALIZATION_ID).observed_phase)

    def test_an_expiry_moves_the_records_metadata_forward_at_the_same_snapshot(self) -> None:
        handle_table_event(
            event("table_committed", {"snapshot_id": 9, "sequence_number": 3, "metadata_location": "gs://lake/t/metadata/00005-a.metadata.json", "snapshots": 12}),
            self.table,
        )
        expiry = {
            "job": "expire_snapshots",
            "outcome": "succeeded",
            "counts": {"expired": 11, "remaining": 1},
            "metadata_location": "gs://lake/t/metadata/00006-b.metadata.json",
            "sequence_number": 3,
            "snapshots": 1,
        }
        handle_table_event(event("maintenance_completed", expiry, event_id="evt-x1"), self.table)
        record = self.record()
        self.assertEqual(("gs://lake/t/metadata/00006-b.metadata.json", 1, 9, 3), (record.metadata_location, record.snapshots, record.current_snapshot_id, record.sequence_number))

        # An older one arriving late changes nothing.
        handle_table_event(event("maintenance_completed", {**expiry, "metadata_location": "gs://lake/t/metadata/00004-c.metadata.json", "snapshots": 30}, event_id="evt-x2"), self.table)
        self.assertEqual(("gs://lake/t/metadata/00006-b.metadata.json", 1), (self.record().metadata_location, self.record().snapshots))

    def test_a_plaintext_count_is_kept_unless_an_older_one_arrives_later(self) -> None:
        counted = {"revision": 2, "counted_at": "2026-10-18T10:05:00Z", "snapshot_id": 7, "files": 3, "rows": 30, "bytes": 900, "blocked_files": 1, "blocked": ["ip: no string member"]}
        handle_table_event(event("plaintext_counted", counted, at="2026-10-18T10:05:00.000Z"), self.table)
        report = self.record().plaintext
        self.assertEqual((2, 7, 3, 30, 900, 1, ("ip: no string member",)), (report.revision, report.snapshot_id, report.files, report.rows, report.bytes, report.blocked_files, report.blocked))

        handle_table_event(event("plaintext_counted", {**counted, "counted_at": "2026-10-18T10:01:00Z", "files": 9}, at="2026-10-18T10:06:00.000Z"), self.table)
        self.assertEqual(3, self.record().plaintext.files)
        handle_table_event(event("plaintext_counted", {**counted, "counted_at": "2026-10-18T10:07:00Z", "files": 0, "rows": 0}, at="2026-10-18T10:07:00.000Z"), self.table)
        self.assertEqual((0, 0), (self.record().plaintext.files, self.record().plaintext.rows))

    def test_a_reprotect_done_clears_its_own_request_and_failure(self) -> None:
        asked = ReprotectRequest(request_id="req-r", requested_at="2026-10-18T09:00:00Z", requested_by="owner@example.com", revision=2, purge=True)
        save_materialization_table(self.table, replace(self.record(), reprotect=asked))

        # A run not done yet, and one done for an older request, leave it asked.
        handle_table_event(event("maintenance_completed", {"job": "reprotect", "outcome": "failed", "request_id": "req-r", "done": False}, event_id="evt-r1"), self.table)
        handle_table_event(event("maintenance_completed", {"job": "reprotect", "outcome": "succeeded", "request_id": "req-old", "done": True}, event_id="evt-r2"), self.table)
        self.assertEqual(asked, self.record().reprotect)

        handle_table_event(event("table_error", {"reason": "reprotect_failed", "message": "gave up"}), self.table)
        handle_table_event(
            event(
                "maintenance_completed",
                {"job": "reprotect", "outcome": "succeeded", "request_id": "req-r", "done": True, "snapshot_id": 8, "sequence_number": 2, "metadata_location": "gs://lake/t/metadata/00002-r.metadata.json"},
                event_id="evt-r3",
            ),
            self.table,
        )
        record = self.record()
        self.assertIsNone(record.reprotect)
        self.assertIsNone(record.last_error)
        self.assertEqual((8, 2), (record.current_snapshot_id, record.sequence_number))
        self.assertEqual(["reprotect", "reprotect", "reprotect"], [run.job for run in record.maintenance])

    def test_a_materialization_being_deleted_takes_only_the_end_of_a_running_job(self) -> None:
        """Its drop waits for the record to say nothing runs on the table: the job's end says so."""
        started = {"job": "compaction", "started_at": "2026-10-18T10:00:00.000Z", "partition": "2026-08"}
        handle_table_event(event("maintenance_started", started, event_id="evt-s-3"), self.table)
        save_materialization(self.table, build_materialization(desired_phase="deleting"))

        cancelled = {"job": "compaction", "outcome": "cancelled", "started_at": "2026-10-18T10:00:00.000Z"}
        self.assertTrue(handle_table_event(event("maintenance_completed", cancelled, event_id="evt-m-5"), self.table))

        record = self.record()
        self.assertIsNone(record.maintenance_job)
        self.assertEqual((), record.maintenance, "no history kept for a table on its way out")
        # Anything else about it is still dropped.
        again = {"job": "compaction", "started_at": "2026-10-18T10:10:00.000Z"}
        self.assertFalse(handle_table_event(event("maintenance_started", again, event_id="evt-s-4"), self.table))
        self.assertIsNone(self.record().maintenance_job)

    def test_events_for_a_materialization_being_deleted_or_without_a_table_are_dropped(self) -> None:
        save_materialization(self.table, build_materialization(desired_phase="deleting"))
        self.assertFalse(handle_table_event(self.schema_changed(1), self.table))

        save_materialization(self.table, build_materialization())
        del self.table[f"materialization-table:{MATERIALIZATION_ID}:orders"]
        self.assertFalse(handle_table_event(self.schema_changed(1), self.table))
        self.assertIsNone(get_materialization_schema(self.table, MATERIALIZATION_ID, "orders"))

    def test_events_for_a_table_being_dropped_or_dropped_are_dropped(self) -> None:
        for state in ("dropping", "dropped"):
            with self.subTest(state=state):
                save_materialization_table(self.table, replace(self.record(), state=state))

                self.assertFalse(handle_table_event(self.schema_changed(1), self.table))
                self.assertFalse(handle_table_event(event("table_committed", {"snapshot_id": 9, "sequence_number": 3}), self.table))
                self.assertIsNone(get_materialization_schema(self.table, MATERIALIZATION_ID, "orders"))
                self.assertEqual(1, self.record().sequence_number)

    def test_a_kept_tables_error_does_not_hold_the_status(self) -> None:
        """Kept read-only, the table is written by nothing: its last error is history, not health."""
        handle_table_event(event("table_error", {"reason": "commit_failed", "message": "Commits are failing: 503"}), self.table)
        save_materialization_table(self.table, replace(self.record(), state="retired"))

        status = handle_runtime_status(heartbeat("2026-10-18T10:00:30Z"), self.table)

        self.assertNotEqual("degraded", status.observed_phase)
        self.assertEqual("commit_failed", self.record().last_error.reason)

    def test_an_event_that_names_no_table_is_dropped(self) -> None:
        # From a runtime that predates multi-topic materializations.
        unnamed = {key: value for key, value in self.schema_changed(1).items() if key != "table"}

        with self.assertLogs("midstream.handlers.table_events", level="WARNING"):
            self.assertFalse(handle_table_event(unnamed, self.table))
        self.assertIsNone(get_materialization_schema(self.table, MATERIALIZATION_ID, "orders"))


class MultiTableEventTests(unittest.TestCase):
    """A materialization of two topics: each table's events touch that table alone."""

    def setUp(self) -> None:
        self.table = FakeTable()
        materialization = build_materialization(tables=table_specs("orders", "payments"))
        save_materialization(self.table, materialization)
        save_materialization_status(self.table, build_status())
        for name in ("orders", "payments"):
            save_materialization_table(
                self.table, build_table_record(materialization, table=name, sequence_number=1, current_snapshot_id=1)
            )

    def record(self, name: str):
        return get_materialization_table(self.table, MATERIALIZATION_ID, name)

    def status(self):
        return get_materialization_status(self.table, MATERIALIZATION_ID)

    def test_a_schema_change_touches_only_its_own_table(self) -> None:
        change = schema_changed(1, table="payments")

        self.assertTrue(handle_table_event(change, self.table))

        self.assertEqual(1, get_materialization_schema(self.table, MATERIALIZATION_ID, "payments").schema_id)
        self.assertEqual("payments", get_materialization_schema(self.table, MATERIALIZATION_ID, "payments").table)
        self.assertIsNone(get_materialization_schema(self.table, MATERIALIZATION_ID, "orders"))
        self.assertEqual((0, 1), (self.record("orders").schema_id, self.record("payments").schema_id))

    def test_a_commit_touches_only_its_own_table(self) -> None:
        handle_table_event(
            event("table_committed", {"snapshot_id": 5, "sequence_number": 2, "totals": {"records": 7, "files": 1, "bytes": 10}}, table="payments"),
            self.table,
        )

        self.assertEqual({"records": 7, "files": 1, "bytes": 10}, self.record("payments").totals)
        self.assertEqual({"records": 0, "files": 0, "bytes": 0}, self.record("orders").totals)
        self.assertEqual(1, self.record("orders").current_snapshot_id)

    def test_an_event_for_a_table_the_materialization_does_not_have_is_dropped(self) -> None:
        self.assertFalse(handle_table_event(event("table_committed", {"snapshot_id": 5, "sequence_number": 2}, table="customers"), self.table))

    def test_the_status_stays_degraded_while_any_table_has_an_error(self) -> None:
        handle_table_event(event("table_error", {"reason": "commit_failed", "message": "orders failing"}), self.table)
        handle_table_event(
            event("table_error", {"reason": "compaction_failed", "message": "payments gave up"}, table="payments", at="2026-10-18T10:00:01.000Z"),
            self.table,
        )
        self.assertEqual(("degraded", "compaction_failed"), (self.status().observed_phase, self.status().phase_reason))
        self.assertEqual("Table payments: payments gave up", self.status().phase_message)

        # Orders commits again: its error clears, payments' holds.
        handle_table_event(event("table_committed", {"snapshot_id": 9, "sequence_number": 3}, at="2026-10-18T10:00:02.000Z"), self.table)
        self.assertIsNone(self.record("orders").last_error)
        self.assertEqual(("degraded", "compaction_failed"), (self.status().observed_phase, self.status().phase_reason))

        handle_table_event(
            event("maintenance_completed", {"job": "compaction", "outcome": "succeeded"}, table="payments", event_id="evt-ok"),
            self.table,
        )
        self.assertIsNone(self.record("payments").last_error)
        self.assertEqual("running", self.status().observed_phase)

    def test_clearing_one_tables_error_shows_the_next_ones(self) -> None:
        handle_table_event(event("table_error", {"reason": "commit_failed", "message": "payments failing"}, table="payments"), self.table)
        handle_table_event(
            event("table_error", {"reason": "commit_failed", "message": "orders failing"}, at="2026-10-18T10:00:01.000Z"),
            self.table,
        )
        self.assertEqual("Table orders: orders failing", self.status().phase_message)

        handle_table_event(event("table_committed", {"snapshot_id": 9, "sequence_number": 3}, at="2026-10-18T10:00:02.000Z"), self.table)

        status = self.status()
        self.assertEqual(("degraded", "commit_failed"), (status.observed_phase, status.phase_reason))
        self.assertEqual("Table payments: payments failing", status.phase_message)


if __name__ == "__main__":
    unittest.main()
