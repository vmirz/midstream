from __future__ import annotations

from dataclasses import replace
import unittest

from materializer_control_plane.models import Materialization, TableSpec, TopicChangeRequest
from materializer_control_plane.services.topic_changes import TopicChangeError, describe_topic_change, plan_topic_change
from materializer_control_plane.table_names import table_name_for_topic
from table_records import build_table_record, table_spec, table_specs


def build_materialization(*topics: str, retired: tuple[TableSpec, ...] = ()) -> Materialization:
    return Materialization(
        materialization_id="mat-1",
        name="shop",
        tables=table_specs(*topics),
        retired_tables=retired,
        destination_uri="gs://lake/warehouse",
        runtime_mode="dedicated",
        resolved_runtime_name="kaspr-mat-1",
        namespace="kafka-sql",
        desired_phase="running",
        created_at="2026-07-12T00:00:00Z",
        updated_at="2026-07-12T00:00:00Z",
    )


def records_of(materialization: Materialization, **states: str):
    """A record for every table of `materialization`, kept ones included; `states` by table name."""
    written = [build_table_record(materialization, table=spec.name) for spec in materialization.tables]
    kept = [
        build_table_record(replace(materialization, tables=(spec,), retired_tables=()), table=spec.name, state="retired")
        for spec in materialization.retired_tables
    ]
    return [replace(record, state=states.get(record.name, record.state)) for record in written + kept]


def plan(materialization: Materialization, topics, actions=None, records=None, maximum: int = 10, new=None):
    """Plan a change of `topics`. `new` gives the tables of topics new to the materialization; a new
    topic not in it gets a table named after it in the default dataset, as the console prefills."""
    existing = {spec.source_topic for spec in materialization.all_tables}
    chosen = dict(new or {})
    new_tables = tuple(
        chosen.get(topic.strip()) or table_spec(table_name_for_topic(topic.strip()), topic.strip())
        for topic in topics
        if topic.strip() and topic.strip() not in existing
    )
    return plan_topic_change(
        materialization,
        records_of(materialization) if records is None else records,
        TopicChangeRequest(source_topics=tuple(topics), table_actions=dict(actions or {}), new_tables=tuple(dict.fromkeys(new_tables))),
        maximum=maximum,
    )


def names(specs) -> list[str]:
    return [spec.name for spec in specs]


class TopicChangePlanTests(unittest.TestCase):
    def test_an_added_topic_gets_the_table_the_request_chose(self) -> None:
        shipments = table_spec("shipments", "mtm.shipments.v1", dataset="logistics", dataset_id="ds-000000000002")

        result = plan(build_materialization("orders", "payments"), ["orders", "payments", "mtm.shipments.v1"], new={"mtm.shipments.v1": shipments})

        self.assertEqual(names(result.tables), ["orders", "payments", "shipments"])
        self.assertEqual(result.added, (shipments,))
        self.assertEqual("logistics.shipments", result.added[0].qualified_name)
        self.assertEqual((result.removed, result.dropped, result.retired_tables), ((), (), ()))
        self.assertTrue(result.changes_runtime)

    def test_a_new_topic_needs_a_table_chosen_for_it(self) -> None:
        materialization = build_materialization("orders")

        with self.assertRaises(TopicChangeError) as error:
            plan_topic_change(
                materialization,
                records_of(materialization),
                TopicChangeRequest(source_topics=("orders", "payments")),
                maximum=10,
            )

        self.assertIn("choose the dataset and name of topic payments", str(error.exception))

    def test_topics_already_read_keep_their_tables_in_the_new_order(self) -> None:
        materialization = build_materialization("orders", "payments")

        result = plan(materialization, ["payments", "orders"])

        self.assertEqual(names(result.tables), ["payments", "orders"])
        self.assertFalse(result.changes_anything)

    def test_a_table_keeps_its_dataset_and_name(self) -> None:
        materialization = build_materialization("orders")
        moved = table_spec("orders", "orders", dataset="finance", dataset_id="ds-000000000002")

        with self.assertRaises(TopicChangeError) as error:
            plan_topic_change(
                materialization,
                records_of(materialization),
                TopicChangeRequest(source_topics=("orders",), new_tables=(moved,)),
                maximum=10,
            )

        self.assertIn("never change", str(error.exception))
        # Repeating it as it is changes nothing.
        same = plan_topic_change(
            materialization,
            records_of(materialization),
            TopicChangeRequest(source_topics=("orders",), new_tables=materialization.tables),
            maximum=10,
        )
        self.assertFalse(same.changes_anything)

    def test_a_removed_topics_table_is_kept_or_dropped_as_asked(self) -> None:
        materialization = build_materialization("orders", "payments", "refunds")

        result = plan(materialization, ["orders"], {"payments": "keep", "refunds": "drop"})

        self.assertEqual(names(result.tables), ["orders"])
        self.assertEqual(names(result.removed), ["payments", "refunds"])
        self.assertEqual(names(result.retired), ["payments"])
        self.assertEqual(names(result.retired_tables), ["payments"])
        self.assertEqual(names(result.dropped), ["refunds"])
        self.assertTrue(result.changes_runtime)
        self.assertEqual("Changing topics: keeping shop.payments read-only; dropping shop.refunds", describe_topic_change(result))

    def test_removing_a_topic_needs_a_decision_about_its_table(self) -> None:
        with self.assertRaises(TopicChangeError) as error:
            plan(build_materialization("orders", "payments"), ["orders"])

        self.assertEqual(error.exception.field_name, "removed_tables")
        self.assertIn("payments", str(error.exception))

    def test_adding_a_kept_tables_topic_again_writes_that_table_again(self) -> None:
        kept = table_spec("payments", "payments", dataset="finance", dataset_id="ds-000000000002")
        materialization = build_materialization("orders", retired=(kept,))

        result = plan(materialization, ["orders", "payments"])

        self.assertEqual(result.tables, (materialization.tables[0], kept))
        self.assertEqual(result.resumed, (kept,))
        self.assertEqual((result.added, result.retired_tables), ((), ()))
        self.assertTrue(result.changes_runtime)

    def test_a_kept_table_can_be_dropped_without_changing_any_topic(self) -> None:
        kept = table_spec("payments", "payments")
        materialization = build_materialization("orders", retired=(kept,))

        result = plan(materialization, ["orders"], {"payments": "drop"})

        self.assertEqual(result.dropped, (kept,))
        self.assertEqual(result.retired_tables, ())
        self.assertFalse(result.changes_runtime)
        self.assertTrue(result.changes_anything)

    def test_a_kept_table_cannot_be_dropped_while_its_topic_is_added_back(self) -> None:
        kept = table_spec("payments", "payments")

        with self.assertRaises(TopicChangeError) as error:
            plan(build_materialization("orders", retired=(kept,)), ["orders", "payments"], {"payments": "drop"})

        self.assertEqual(error.exception.field_name, "removed_tables")

    def test_a_table_still_written_cannot_be_kept_or_dropped(self) -> None:
        with self.assertRaises(TopicChangeError) as error:
            plan(build_materialization("orders", "payments"), ["orders", "payments"], {"orders": "drop"})

        self.assertEqual(error.exception.field_name, "removed_tables")
        self.assertIn("still reads", str(error.exception))

    def test_an_unknown_table_is_refused(self) -> None:
        with self.assertRaises(TopicChangeError) as error:
            plan(build_materialization("orders", "payments"), ["orders"], {"payments": "drop", "customers": "drop"})

        self.assertEqual(error.exception.field_name, "removed_tables")
        self.assertIn("customers", str(error.exception))

    def test_a_new_table_never_takes_a_name_in_use(self) -> None:
        """Written and kept tables, tables being dropped and dropped ones whose files stay all hold
        their names in the materialization - in whatever dataset - since each is a directory of it."""
        kept = table_spec("orders_v2", "orders-v2")
        materialization = build_materialization("orders", retired=(kept,))
        gone = build_table_record(
            replace(materialization, tables=(table_spec("orders_v3", "orders-v3"),), retired_tables=()),
            table="orders_v3",
            state="dropped",
        )
        records = [*records_of(materialization), gone]

        for taken in ("orders", "orders_v2", "orders_v3", "ORDERS_V3"):
            with self.subTest(taken=taken), self.assertRaises(TopicChangeError) as error:
                plan(
                    materialization,
                    ["orders", "orders.v4"],
                    records=records,
                    new={"orders.v4": table_spec(taken.lower(), "orders.v4", dataset="elsewhere", dataset_id="ds-000000000009")},
                )
            self.assertIn("already has a table named", str(error.exception))
        result = plan(materialization, ["orders", "orders.v4"], records=records, new={"orders.v4": table_spec("orders_v4", "orders.v4")})
        self.assertEqual(names(result.added), ["orders_v4"])

    def test_two_new_tables_cannot_share_a_name(self) -> None:
        with self.assertRaises(TopicChangeError) as error:
            plan(
                build_materialization("orders"),
                ["orders", "a", "b"],
                new={"a": table_spec("same", "a"), "b": table_spec("same", "b", dataset="other", dataset_id="ds-000000000003")},
            )

        self.assertIn("same", str(error.exception))

    def test_a_redelivered_request_ignores_the_tables_it_already_dropped(self) -> None:
        """Applied once, the request finds what it dropped being dropped: the second plan changes nothing."""
        materialization = build_materialization("orders")
        dropping = build_table_record(build_materialization("orders", "payments"), table="payments", state="dropping")

        result = plan(materialization, ["orders"], {"payments": "drop"}, records=[*records_of(materialization), dropping])

        self.assertFalse(result.changes_anything)

    def test_topics_are_validated_as_on_create(self) -> None:
        materialization = build_materialization("orders")
        for topics in (["orders", "bad topic"], ["orders", "b", "c"]):
            with self.subTest(topics=topics):
                with self.assertRaises(TopicChangeError) as error:
                    plan(materialization, topics, maximum=2, new={"bad topic": table_spec("bad", "bad topic")})
                self.assertEqual(error.exception.field_name, "tables")

    def test_a_repeated_topic_counts_once(self) -> None:
        result = plan(build_materialization("orders"), ["orders", " payments ", "payments", ""])

        self.assertEqual(names(result.tables), ["orders", "payments"])

    def test_a_plan_round_trips_for_a_pending_change(self) -> None:
        from materializer_control_plane.models import PendingTopicChange, TopicChangePlan

        result = plan(build_materialization("orders", "payments"), ["orders", "refunds"], {"payments": "drop"})
        pending = PendingTopicChange(request_id="req-1", claim_request_id="idr-1", submitted_at="2026-07-12T00:00:00Z", plan=result)

        self.assertEqual(TopicChangePlan.from_dict(result.to_dict()), result)
        self.assertEqual(PendingTopicChange.from_dict(pending.to_dict()), pending)
        self.assertEqual(names(pending.added), ["refunds"])


if __name__ == "__main__":
    unittest.main()
