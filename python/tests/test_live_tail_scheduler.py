"""What the control plane tells the tail app about each table, every minute (handbook 19)."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import replace
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from table_records import build_table_record, table_specs  # noqa: E402

from materializer_control_plane.config import ControlPlaneConfig  # noqa: E402
from materializer_control_plane.handlers.maintenance import build_table_maintenance_trigger, handle_maintenance_work  # noqa: E402
from materializer_control_plane.models import (  # noqa: E402
    LiveTailHints,
    LiveTailMessage,
    LiveTailTarget,
    Materialization,
    SettingOverride,
    SettingsDocument,
    live_tail_routing_key,
)
from materializer_control_plane.services.live_tail import (  # noqa: E402
    LIVE_TAIL_SETTING_FIELDS,
    build_live_tail_messages,
    live_tail_output,
)
from materializer_control_plane.storage import save_materialization, save_materialization_table  # noqa: E402
from materializer_control_plane.storage.settings import save_settings_document  # noqa: E402

ISSUED_AT = "2026-10-23T12:00:00Z"


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


def build_materialization(materialization_id: str, *topics: str, desired_phase: str = "running", skip_tombstones: bool = False) -> Materialization:
    return Materialization(
        materialization_id=materialization_id,
        name=f"shop_{materialization_id[-1]}",
        tables=table_specs(*topics),
        destination_uri="gs://lake",
        desired_phase=desired_phase,
        created_at="2026-10-23T00:00:00Z",
        updated_at="2026-10-23T00:00:00Z",
        skip_tombstones=skip_tombstones,
    )


class LiveTailScheduleTests(unittest.TestCase):
    def setUp(self) -> None:
        self.table = FakeTable()

    def add(self, materialization: Materialization, **states: str) -> None:
        save_materialization(self.table, materialization)
        for spec in materialization.all_tables:
            record = build_table_record(
                materialization,
                table=spec.name,
                state=states.get(spec.name, "active"),
                offsets={"0": 41, "1": -1},
                schema_id=3,
            )
            save_materialization_table(self.table, record)

    def messages(self, config: ControlPlaneConfig | None = None) -> dict[str, LiveTailMessage]:
        found = build_live_tail_messages(self.table, config=config or ControlPlaneConfig(), issued_at=ISSUED_AT)
        return {f"{message.target.materialization_id}:{message.target.table}": message for message in found}

    def test_every_written_table_of_a_running_materialization_is_ticked(self) -> None:
        self.add(build_materialization("mat-1", "orders", "payments", skip_tombstones=True))

        messages = self.messages(ControlPlaneConfig(live_tail_max_rows_per_table=5000, live_tail_idle_seconds=0))

        self.assertEqual({"mat-1:orders": "tail_tick", "mat-1:payments": "tail_tick"}, {key: message.kind for key, message in messages.items()})
        orders = messages["mat-1:orders"]
        self.assertEqual("tail:mat-1:orders", orders.key)
        self.assertEqual(live_tail_routing_key("mat-1", "orders"), orders.key)
        target = orders.target
        self.assertEqual(("shop", "ds-000000000001", "orders", "orders"), (target.dataset, target.dataset_id, target.table, target.source_topic))
        self.assertTrue(target.location.endswith("/mat-1/orders"))
        self.assertEqual(("day", "6b1c0000-0000-0000-0000-000000000000", True), (target.partition_granularity, target.table_uuid, target.skip_tombstones))
        self.assertEqual(set(LIVE_TAIL_SETTING_FIELDS), set(orders.settings))
        self.assertEqual((5000, 0), (orders.settings["live_tail_max_rows_per_table"], orders.settings["live_tail_idle_seconds"]))
        self.assertEqual(LiveTailHints(offsets={"0": 41, "1": -1}, schema_id=3, metadata_location=target.location + "/metadata/00000-6b1c0000.metadata.json"), orders.hints)
        self.assertEqual(ISSUED_AT, orders.issued_at)

    def test_every_other_table_in_the_catalog_is_cancelled_with_its_reason(self) -> None:
        running = build_materialization("mat-1", "orders", "payments", "refunds", "audit")
        kept = replace(running, tables=running.tables[:3], retired_tables=running.tables[3:])
        self.add(kept, payments="dropping", refunds="dropped", audit="retired")
        self.add(build_materialization("mat-2", "returns"))
        # Stored before 4.3 with its live tail switched off; every materialization has one now.
        self.table.values["materialization:mat-2"]["payload"]["live_tail"] = False
        self.add(build_materialization("mat-3", "invoices", desired_phase="stopped"))
        self.add(build_materialization("mat-4", "ledger", desired_phase="deleting"))
        self.add(build_materialization("mat-5", "old", desired_phase="deleted"))
        self.add(build_materialization("mat-6", "paused", desired_phase="paused"))

        messages = self.messages()

        self.assertEqual(
            {
                "mat-1:orders": ("tail_tick", None),
                "mat-1:payments": ("tail_cancel", "table_dropping"),
                "mat-1:audit": ("tail_cancel", "table_retired"),
                "mat-2:returns": ("tail_tick", None),
                "mat-3:invoices": ("tail_cancel", "materialization_stopped"),
                "mat-4:ledger": ("tail_cancel", "materialization_deleting"),
                "mat-5:old": ("tail_cancel", "materialization_deleting"),
                "mat-6:paused": ("tail_cancel", "materialization_stopped"),
            },
            {key: (message.kind, message.reason) for key, message in messages.items()},
        )
        # A cancel carries no settings or hints: the app stops, it does not tail.
        self.assertEqual(({}, None), (messages["mat-3:invoices"].settings, messages["mat-3:invoices"].hints))

    def test_nothing_is_sent_while_the_deployment_has_live_tail_off(self) -> None:
        self.add(build_materialization("mat-1", "orders"))

        self.assertEqual({}, self.messages(ControlPlaneConfig(live_tail_enabled=False)))

    def test_the_scheduled_step_routes_every_tail_message_keyed_by_its_table_and_writes_nothing(self) -> None:
        self.add(build_materialization("mat-1", "orders", "payments"))
        self.add(build_materialization("mat-2", "returns", desired_phase="stopped"))
        settings = FakeTable()
        save_settings_document(
            settings,
            SettingsDocument(
                scope="control-plane",
                revision=1,
                values={"live_tail_retention_seconds": SettingOverride(value=45, updated_at=ISSUED_AT, updated_by="ops")},
            ),
            updated_at=ISSUED_AT,
        )
        before = dict(self.table.values)

        outputs = list(
            handle_maintenance_work(build_table_maintenance_trigger(requested_at=ISSUED_AT), self.table, settings_table=settings)
        )

        tails = [output for output in outputs if output["route"] == "tail"]
        self.assertEqual(["tail:mat-1:orders", "tail:mat-1:payments", "tail:mat-2:returns"], [output["key"] for output in tails])
        ticks = [LiveTailMessage.from_dict(output["value"]) for output in tails]
        self.assertEqual(["tail_tick", "tail_tick", "tail_cancel"], [message.kind for message in ticks])
        # The control plane's overrides apply: the app sees the settings the Settings page shows.
        self.assertEqual(45, ticks[0].settings["live_tail_retention_seconds"])
        self.assertEqual(before, self.table.values)

    def test_a_table_dropped_and_created_again_is_ticked_with_its_new_uuid(self) -> None:
        materialization = build_materialization("mat-1", "orders")
        save_materialization(self.table, materialization)
        save_materialization_table(self.table, build_table_record(materialization, table_uuid="7d2e0000-0000-0000-0000-000000000000"))

        (tick,) = self.messages().values()

        self.assertEqual("7d2e0000-0000-0000-0000-000000000000", tick.target.table_uuid)


class LiveTailMessageTests(unittest.TestCase):
    def target(self) -> LiveTailTarget:
        return LiveTailTarget(
            materialization_id="mat-1",
            dataset="shop",
            dataset_id="ds-000000000001",
            table="orders",
            location="gs://lake/materializer/mat-1/orders",
            source_topic="orders",
            table_uuid="6b1c",
            skip_tombstones=True,
        )

    def test_messages_round_trip_and_route_by_table(self) -> None:
        tick = LiveTailMessage(
            kind="tail_tick",
            target=self.target(),
            issued_at=ISSUED_AT,
            settings={"live_tail_max_rows_per_table": 250000},
            hints=LiveTailHints(offsets={"0": 1041, "1": 998}, schema_id=3, metadata_location="gs://…/00042.metadata.json"),
        )
        cancel = LiveTailMessage(kind="tail_cancel", target=self.target(), issued_at=ISSUED_AT, reason="table_dropping")

        for message in (tick, cancel):
            self.assertEqual(message, LiveTailMessage.from_dict(message.to_dict()))
        self.assertEqual({"route": "tail", "key": "tail:mat-1:orders", "value": tick.to_dict()}, live_tail_output(tick))
        self.assertNotIn("hints", cancel.to_dict())
        self.assertEqual("table_dropping", cancel.to_dict()["reason"])

    def test_what_is_not_a_message_is_refused(self) -> None:
        with self.assertRaisesRegex(ValueError, "kind must be one of"):
            LiveTailMessage(kind="tail_now", target=self.target(), issued_at=ISSUED_AT)
        with self.assertRaisesRegex(ValueError, "reason must be one of"):
            LiveTailMessage(kind="tail_cancel", target=self.target(), issued_at=ISSUED_AT, reason="bored")
        with self.assertRaisesRegex(ValueError, "issued_at is required"):
            LiveTailMessage(kind="tail_tick", target=self.target(), issued_at="")
        with self.assertRaisesRegex(ValueError, "dataset_id is required"):
            replace(self.target(), dataset_id="")
        with self.assertRaises(KeyError):
            LiveTailMessage.from_dict({"kind": "tail_tick", "issued_at": ISSUED_AT})

    def test_hints_read_offsets_by_partition_as_text(self) -> None:
        hints = LiveTailHints.from_dict({"offsets": {0: "7", "1": 9}, "schema_id": "2"})
        self.assertEqual(({"0": 7, "1": 9}, 2, None), (hints.offsets, hints.schema_id, hints.metadata_location))
        self.assertEqual(LiveTailHints(), LiveTailHints.from_dict(None))


if __name__ == "__main__":
    unittest.main()
