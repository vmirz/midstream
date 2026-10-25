"""The names SQL reads tables by, `<dataset>.<table>`, and a materialization's label.

A table's name defaults to one derived from its topic, which the console derives too, to prefill
it while a materialization is created; its `web/shared/tableNames.test.ts` checks these same vectors.
"""

from __future__ import annotations

import unittest

from materializer_control_plane.table_names import (
    TABLE_NAME_MAX_LENGTH,
    InvalidNameError,
    InvalidSourceTopicsError,
    dataset_name_problem,
    normalize_dataset_name,
    normalize_materialization_label,
    normalize_source_topics,
    normalize_table_name,
    table_name_for_topic,
    table_name_problem,
    table_names_for_topics,
)

#: (topic, table name) — mirrored in `web/shared/tableNames.test.ts`.
TABLE_NAME_VECTORS = (
    ("orders", "orders"),
    ("Orders.V2", "orders_v2"),
    ("orders-v2", "orders_v2"),
    ("orders__v2", "orders_v2"),
    ("materializer-loadtest-orders", "materializer_loadtest_orders"),
    ("2026-orders", "t_2026_orders"),
    ("order", "order_topic"),
    ("SELECT", "select_topic"),
    ("iceberg", "iceberg_topic"),
    ("__consumer_offsets", "consumer_offsets"),
    ("---", "topic"),
    ("_", "topic"),
    ("x" * 80, "x" * 63),
    ("a" * 62 + "-b", "a" * 62),
)


class TableNameTests(unittest.TestCase):
    def test_a_topic_becomes_a_name_sql_reads_unquoted(self) -> None:
        for topic, expected in TABLE_NAME_VECTORS:
            with self.subTest(topic=topic):
                name = table_name_for_topic(topic)
                self.assertEqual(expected, name)
                self.assertLessEqual(len(name), TABLE_NAME_MAX_LENGTH)
                # Every derived name is one a table may be given.
                self.assertEqual(name, normalize_table_name(name))

    def test_topics_deriving_the_same_name_take_suffixes_in_their_order(self) -> None:
        self.assertEqual(("orders_v2", "orders_v2_2", "orders_v2_3"), table_names_for_topics(["orders.v2", "orders-v2", "ORDERS_V2"]))
        self.assertEqual(("orders_2", "orders", "orders_2_2"), table_names_for_topics(["orders_2", "orders", "orders-2"]))
        self.assertEqual(("x" * 63, "x" * 61 + "_2"), table_names_for_topics(["x" * 80, "x" * 81]))
        self.assertEqual((), table_names_for_topics([]))

    def test_names_depend_only_on_the_topics_and_their_order(self) -> None:
        topics = ["payments", "orders", "orders.v2"]
        self.assertEqual(table_names_for_topics(topics), table_names_for_topics(list(topics)))
        self.assertEqual(("orders", "payments"), table_names_for_topics(["orders", "payments"]))


class DatasetAndTableNameTests(unittest.TestCase):
    """A dataset is the namespace of its tables in SQL: `<dataset>.<table>`, lowercase only."""

    def test_a_name_is_a_lowercase_letter_then_lowercase_letters_digits_and_underscores(self) -> None:
        for name in ("sales", "s", "finance_2026", "a" * 63):
            with self.subTest(name=name):
                self.assertEqual(name, normalize_dataset_name(name))
                self.assertEqual(name, normalize_table_name(name))
        self.assertEqual("sales", normalize_dataset_name("  sales "))
        for value in (None, "", "  ", 7, "2026_sales", "_sales", "sales-eu", "sales.eu", "sales eu", "ö", "a" * 64):
            with self.subTest(value=value):
                with self.assertRaises(InvalidNameError):
                    normalize_dataset_name(value)
                with self.assertRaises(InvalidNameError):
                    normalize_table_name(value)

    def test_names_are_lowercase_only_and_say_so(self) -> None:
        self.assertEqual('dataset names are lowercase only: "sales", not "Sales"', dataset_name_problem("Sales"))
        self.assertEqual('table names are lowercase only: "orders_v2", not "Orders_V2"', table_name_problem("Orders_V2"))

    def test_reserved_sql_words_cannot_name_either(self) -> None:
        for name in ("order", "select", "iceberg", "lake", "catalog", "copy"):
            with self.subTest(name=name):
                self.assertIn("reserved SQL word", dataset_name_problem(name))
                self.assertIn("reserved SQL word", table_name_problem(name))

    def test_duckdbs_own_catalogs_and_schemas_cannot_name_a_dataset(self) -> None:
        for name in ("main", "memory", "temp", "temporary", "information_schema", "pg_catalog"):
            with self.subTest(name=name), self.assertRaisesRegex(InvalidNameError, "DuckDB gives its own"):
                normalize_dataset_name(name)

    def test_a_table_may_still_take_one_inside_its_dataset(self) -> None:
        self.assertEqual("memory", table_name_for_topic("memory"))
        self.assertEqual("main", normalize_table_name("main"))

    def test_the_field_a_problem_is_about_is_named(self) -> None:
        with self.assertRaises(InvalidNameError) as raised:
            normalize_table_name("Orders", field_name="tables.0.name")
        self.assertEqual("tables.0.name", raised.exception.field_name)


class MaterializationLabelTests(unittest.TestCase):
    """A materialization's name is a label: free text, of no meaning in SQL or storage."""

    def test_a_label_is_free_text_trimmed_with_runs_of_space_as_one(self) -> None:
        self.assertEqual("Sales orders (EU)", normalize_materialization_label("  Sales   orders (EU) "))
        self.assertEqual("order", normalize_materialization_label("order"))
        self.assertEqual("x" * 128, normalize_materialization_label("x" * 128))

    def test_no_label_is_none_so_the_id_stands_in(self) -> None:
        for value in (None, "", "   "):
            with self.subTest(value=value):
                self.assertIsNone(normalize_materialization_label(value))

    def test_what_a_label_cannot_be(self) -> None:
        for value in ("x" * 129, "line\nbreak", "tab\there", 7, ["a"]):
            with self.subTest(value=value), self.assertRaises(InvalidNameError) as raised:
                normalize_materialization_label(value)
            self.assertEqual("name", raised.exception.field_name)


class SourceTopicTests(unittest.TestCase):
    def test_topics_are_kept_in_order_without_space_or_repeats(self) -> None:
        self.assertEqual(("orders", "payments"), normalize_source_topics([" orders", "payments ", "orders", ""], maximum=5))
        self.assertEqual(("orders", "payments"), normalize_source_topics("orders, payments,, orders", maximum=5))
        self.assertEqual(("a.b_c-D",), normalize_source_topics(["a.b_c-D"], maximum=1))

    def test_what_kafka_would_refuse_is_refused(self) -> None:
        for value in (None, [], "", " , ", [None], [7], {"orders": 1}, ["orders topic"], ["."], [".."], ["ö"], ["x" * 250]):
            with self.subTest(value=value), self.assertRaises(InvalidSourceTopicsError) as raised:
                normalize_source_topics(value, maximum=5)
            self.assertEqual("source_topics", raised.exception.field_name)
        self.assertEqual(("x" * 249,), normalize_source_topics(["x" * 249], maximum=1))

    def test_a_materialization_reads_at_most_the_maximum(self) -> None:
        with self.assertRaisesRegex(InvalidSourceTopicsError, "at most 2 topics; 3 were given"):
            normalize_source_topics(["a", "b", "c"], maximum=2)
        # Repeats do not count.
        self.assertEqual(("a", "b"), normalize_source_topics(["a", "b", "a"], maximum=2))


if __name__ == "__main__":
    unittest.main()
