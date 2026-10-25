"""What the control plane tells the maintenance app about each table, every minute."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import replace
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from table_records import build_table_record, table_specs  # noqa: E402

from midstream.config import ControlPlaneConfig  # noqa: E402
from midstream.handlers.maintenance import (  # noqa: E402
    build_table_maintenance_trigger,
    handle_maintenance_work,
)
from midstream.models import (  # noqa: E402
    MaintenanceMessage,
    Materialization,
    RunningMaintenance,
    SettingOverride,
    SettingsDocument,
)
from midstream.models.table import MaintenanceRun  # noqa: E402
from midstream.runtime_app.config import RuntimeAppConfig  # noqa: E402
from midstream.services.table_maintenance import (  # noqa: E402
    MAINTENANCE_SETTING_FIELDS,
    build_table_maintenance_messages,
    last_runs,
)
from midstream.storage import save_materialization, save_materialization_table  # noqa: E402
from midstream.storage.settings import save_settings_document  # noqa: E402


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


def build_materialization(materialization_id: str, *topics: str, desired_phase: str = "running") -> Materialization:
    return Materialization(
        materialization_id=materialization_id,
        namespace="midstream",
        name=f"shop_{materialization_id[-1]}",
        tables=table_specs(*topics),
        destination_uri="gs://lake",
        desired_phase=desired_phase,
        created_at="2026-10-19T00:00:00Z",
        updated_at="2026-10-19T00:00:00Z",
    )


ISSUED_AT = "2026-10-19T12:00:00Z"


class TableMaintenanceScheduleTests(unittest.TestCase):
    def setUp(self) -> None:
        self.table = FakeTable()

    def add(self, materialization: Materialization, **states: str) -> None:
        save_materialization(self.table, materialization)
        for spec in materialization.all_tables:
            record = build_table_record(materialization, table=spec.name, state=states.get(spec.name, "active"))
            save_materialization_table(self.table, record)

    def messages(self, runtime_config: RuntimeAppConfig | None = None) -> dict[str, MaintenanceMessage]:
        found = build_table_maintenance_messages(self.table, runtime_config=runtime_config or RuntimeAppConfig(), issued_at=ISSUED_AT)
        return {f"{message.target.materialization_id}:{message.target.table}": message for message in found}

    def test_every_written_and_kept_table_is_ticked_stopped_materializations_included(self) -> None:
        running = build_materialization("mat-1", "orders", "payments")
        kept = replace(running, tables=running.tables[:1], retired_tables=running.tables[1:])
        self.add(kept, payments="retired")
        self.add(build_materialization("mat-2", "refunds", desired_phase="stopped"))

        messages = self.messages(RuntimeAppConfig(compaction_interval_minutes=7))

        self.assertEqual({"mat-1:orders", "mat-1:payments", "mat-2:refunds"}, set(messages))
        self.assertEqual({"maintenance_tick"}, {message.kind for message in messages.values()})
        payments = messages["mat-1:payments"]
        # The target names the table in its dataset, its Iceberg namespace.
        self.assertEqual(("retired", "shop", "maintenance:mat-1:payments"), (payments.target.state, payments.target.namespace, payments.key))
        self.assertTrue(payments.target.location.endswith("/payments"))
        self.assertEqual(set(MAINTENANCE_SETTING_FIELDS), set(payments.settings))
        self.assertEqual(7, payments.settings["compaction_interval_minutes"])
        self.assertEqual(ISSUED_AT, payments.issued_at)

    def test_a_table_on_its_way_out_is_cancelled_and_a_dropped_one_forgotten(self) -> None:
        self.add(build_materialization("mat-1", "orders", "payments", "refunds"), payments="dropping", refunds="dropped")
        self.add(build_materialization("mat-2", "audit", desired_phase="deleting"))
        self.add(build_materialization("mat-3", "gone", desired_phase="deleted"))

        messages = self.messages()

        self.assertEqual(
            {"mat-1:orders": "maintenance_tick", "mat-1:payments": "maintenance_cancel", "mat-2:audit": "maintenance_cancel"},
            {key: message.kind for key, message in messages.items()},
        )
        self.assertEqual("table_dropping", messages["mat-1:payments"].reason)
        self.assertEqual("materialization_deleting", messages["mat-2:audit"].reason)

    def test_a_tick_says_when_each_job_last_ran_and_what_runs(self) -> None:
        materialization = build_materialization("mat-1", "orders")
        save_materialization(self.table, materialization)
        running = RunningMaintenance(job="compaction", started_at="2026-10-19T11:58:00Z", member_id="maintenance-0")
        record = build_table_record(
            materialization,
            maintenance=(
                MaintenanceRun(job="compaction", completed_at="2026-10-19T11:00:00Z"),
                MaintenanceRun(job="orphan_cleanup", completed_at="2026-10-19T03:00:00Z", outcome="failed"),
                MaintenanceRun(job="compaction", completed_at="2026-10-19T10:00:00Z"),
            ),
            maintenance_job=running,
        )
        save_materialization_table(self.table, record)

        (tick,) = self.messages().values()

        self.assertEqual({"compaction": "2026-10-19T11:00:00Z", "orphan_cleanup": "2026-10-19T03:00:00Z"}, tick.last_runs)
        self.assertEqual(running, tick.running)
        self.assertEqual(tick.last_runs, last_runs(record))

    def test_the_scheduled_step_routes_every_message_to_the_maintenance_app(self) -> None:
        self.add(build_materialization("mat-1", "orders", "payments"))
        settings = FakeTable()
        save_settings_document(
            settings,
            SettingsDocument(scope="runtime", revision=1, values={"orphan_min_age_hours": SettingOverride(value=48, updated_at=ISSUED_AT, updated_by="ops")}),
            updated_at=ISSUED_AT,
        )

        before = dict(self.table.values)

        outputs = list(
            handle_maintenance_work(
                build_table_maintenance_trigger(requested_at=ISSUED_AT),
                self.table,
                settings_table=settings,
                config=ControlPlaneConfig(),
            )
        )

        # The tail app is named every table too, on its own route (`test_live_tail_scheduler.py`).
        maintenance = [output for output in outputs if output["route"] == "maintenance"]
        self.assertEqual({"maintenance", "tail"}, {output["route"] for output in outputs})
        self.assertEqual(["maintenance:mat-1:orders", "maintenance:mat-1:payments"], [output["key"] for output in maintenance])
        message = MaintenanceMessage.from_dict(maintenance[0]["value"])
        # The runtimes' overrides apply: the app sees the settings the Settings page shows.
        self.assertEqual(48, message.settings["orphan_min_age_hours"])
        self.assertEqual(ISSUED_AT, message.issued_at)
        # Nothing is written: the step only reads, from whichever shard it runs in.
        self.assertEqual(before, self.table.values)


class MaintenanceMessageTests(unittest.TestCase):
    def test_messages_round_trip_and_refuse_what_is_not_one(self) -> None:
        materialization = build_materialization("mat-1", "orders")
        from midstream.services.table_maintenance import maintenance_target

        target = maintenance_target(build_table_record(materialization))
        message = MaintenanceMessage(
            kind="maintenance_request",
            target=target,
            issued_at=ISSUED_AT,
            settings={"compaction_enabled": True},
            job="compaction",
            request_id="mnt-1",
            requested_by="ops",
        )

        self.assertEqual(message, MaintenanceMessage.from_dict(message.to_dict()))
        with self.assertRaisesRegex(ValueError, "job must be one of"):
            MaintenanceMessage(kind="maintenance_request", target=target, issued_at=ISSUED_AT, job="vacuum")
        with self.assertRaisesRegex(ValueError, "kind must be one of"):
            MaintenanceMessage(kind="maintenance_now", target=target, issued_at=ISSUED_AT)


if __name__ == "__main__":
    unittest.main()
