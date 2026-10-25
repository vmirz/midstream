"""Conform (records onto a schema), drift detection, and the API projection agree on what fits."""

from __future__ import annotations

import json
import unittest

from midstream.lakehouse.schema.conform import Conformer, unmapped_text
from midstream.lakehouse.schema.forkmap import ForkMap
from midstream.lakehouse.schema.index import build_index
from midstream.lakehouse.schema.infer import infer
from midstream.lakehouse.schema.policy import SchemaLimits, evolve
from midstream.lakehouse.schema.render import logical_name, render_schema
from midstream.lakehouse.tables import build_schema
from midstream.runtime_app.schema.drift import DriftDetector, RequestDeduper


def schema_of(*records, limits=None):
    evolution = evolve(build_schema(), ForkMap(), [((), infer(record)) for record in records], limits=limits, schema_id=0)
    return evolution


def evolved(evolution, *records, limits=None):
    return evolve(evolution.schema, evolution.fork_map, [((), infer(r)) for r in records], limits=limits, last_column_id=evolution.last_column_id)


class ConformTests(unittest.TestCase):
    def test_values_land_in_their_fields(self) -> None:
        evolution = schema_of({"order": "A", "total": 1.5, "customer": {"email": "e"}, "items": [{"sku": "s", "qty": 1}], "tags": ["x"]})
        row, unmapped = Conformer(evolution.schema, evolution.fork_map).conform(
            {"order": "B", "total": 2, "customer": {"email": "f"}, "items": [{"sku": "t", "qty": 2}, None], "tags": []}
        )
        self.assertIsNone(unmapped)
        self.assertEqual(row["order"], "B")
        self.assertEqual(row["total"], 2.0)
        self.assertIsInstance(row["total"], float)
        self.assertEqual(row["customer"], {"email": "f"})
        self.assertEqual(row["items"], [{"sku": "t", "qty": 2}, None])
        self.assertEqual(row["tags"], [])

    def test_scalars_render_as_text_into_a_string_field(self) -> None:
        evolution = schema_of({"code": "x"})
        conformer = Conformer(evolution.schema, evolution.fork_map)
        self.assertEqual(conformer.conform({"code": 5})[0]["code"], "5")
        self.assertEqual(conformer.conform({"code": True})[0]["code"], "true")
        self.assertEqual(conformer.conform({"code": 2.5})[0]["code"], "2.5")

    def test_each_value_goes_to_the_fork_of_its_type(self) -> None:
        evolution = evolved(schema_of({"total": 1}), {"total": 1.5})
        conformer = Conformer(evolution.schema, evolution.fork_map)
        self.assertEqual(conformer.conform({"total": 7})[0], {"total": 7})
        self.assertEqual(conformer.conform({"total": 7.5})[0], {"total__v2": 7.5})

    def test_unknown_and_unplaceable_values_go_to_unmapped(self) -> None:
        evolution = schema_of({"total": 1, "customer": {"email": "e"}})
        row, unmapped = Conformer(evolution.schema, evolution.fork_map).conform(
            {"total": "many", "customer": {"email": "e", "phone": {"n": 1}}, "new": [1], "empty": [], "gone": None}
        )
        self.assertEqual(row, {"customer": {"email": "e"}})
        self.assertEqual(unmapped, {"total": "many", "customer.phone": {"n": 1}, "new": [1]})
        self.assertEqual(json.loads(unmapped_text(unmapped)), unmapped)

    def test_list_elements_that_do_not_fit_unmap_the_whole_list(self) -> None:
        evolution = schema_of({"scores": [1]})
        row, unmapped = Conformer(evolution.schema, evolution.fork_map).conform({"scores": [1, "x"]})
        self.assertEqual(row, {})
        self.assertEqual(unmapped, {"scores": [1, "x"]})

    def test_unknown_keys_inside_list_elements_keep_their_index(self) -> None:
        evolution = schema_of({"items": [{"sku": "a"}]})
        row, unmapped = Conformer(evolution.schema, evolution.fork_map).conform({"items": [{"sku": "a"}, {"sku": "b", "tax": 1}]})
        self.assertEqual(row["items"], [{"sku": "a"}, {"sku": "b"}])
        self.assertEqual(unmapped, {"items[1].tax": 1})

    def test_collapsed_family_takes_everything_as_json_text(self) -> None:
        limits = SchemaLimits(max_forks_per_field=1)
        evolution = evolved(evolved(schema_of({"v": 1}, limits=limits), {"v": "s"}, limits=limits), {"v": {"a": 1}}, limits=limits)
        conformer = Conformer(evolution.schema, evolution.fork_map)
        self.assertEqual(conformer.conform({"v": {"a": 1}})[0], {"v__json": '{"a":1}'})
        self.assertEqual(conformer.conform({"v": 3})[0], {"v__json": "3"})

    def test_mixed_list_elements_are_json_text(self) -> None:
        evolution = schema_of({"mixed": [1, "a", {"b": 2}]})
        row, unmapped = Conformer(evolution.schema, evolution.fork_map).conform({"mixed": [2, "c", None, [1]]})
        self.assertIsNone(unmapped)
        self.assertEqual(row["mixed"], ["2", '"c"', None, "[1]"])

    def test_renamed_keys_and_non_object_records(self) -> None:
        evolution = schema_of({"ID": 1, "id": 2, "_kafka_offset": 3})
        conformer = Conformer(evolution.schema, evolution.fork_map)
        self.assertEqual(conformer.conform({"ID": 9, "id": 8, "_kafka_offset": 7})[0], {"ID": 9, "id__k2": 8, "_kafka_offset__payload": 7})
        self.assertEqual(conformer.conform([1, 2]), ({}, {"$": [1, 2]}))
        self.assertEqual(conformer.conform(None), ({}, None))


class DriftTests(unittest.TestCase):
    def detector(self, evolution):
        return DriftDetector(build_index(evolution.schema, evolution.fork_map))

    def test_a_fitting_record_has_no_drift(self) -> None:
        evolution = schema_of({"a": 1, "b": {"c": "x"}, "items": [{"s": 1}]})
        self.assertEqual(self.detector(evolution).observations({"a": 2, "b": {"c": 5}, "items": [{"s": 3}], "z": None}), [])

    def test_new_paths_and_forking_types_are_reported(self) -> None:
        evolution = schema_of({"a": 1, "b": {"c": "x"}, "items": [{"s": 1}]})
        found = self.detector(evolution).observations({"a": 1.5, "b": {"d": True}, "items": [{"s": 1, "t": "x"}, {"s": 2, "t": "y"}], "n": {"m": 1}})
        self.assertEqual(
            sorted(found, key=lambda item: item[0]),
            [
                (("a",), "double"),
                (("b", "d"), "boolean"),
                (("items", "[]", "t"), "string"),
                (("n",), {"type": "struct", "fields": {"m": "long"}}),
            ],
        )

    def test_drift_agrees_with_the_policy(self) -> None:
        """Applying what drift reports makes the record fit, and conform then unmaps nothing."""
        evolution = schema_of({"a": 1, "customer": {"phone": "x"}, "items": [{"category": ["c"]}]})
        record = {"a": 2.5, "customer": {"phone": {"number": "1"}}, "items": [{"category": {"name": "n"}}], "new": [{"x": 1}]}
        found = self.detector(evolution).observations(record)
        after = evolve(evolution.schema, evolution.fork_map, found, last_column_id=evolution.last_column_id)
        self.assertEqual(DriftDetector(build_index(after.schema, after.fork_map)).observations(record), [])
        self.assertIsNone(Conformer(after.schema, after.fork_map).conform(record)[1])

    def test_rejected_paths_are_not_reported_again(self) -> None:
        evolution = schema_of({"a": 1})
        detector = DriftDetector(build_index(evolution.schema, evolution.fork_map), rejected_paths=[("big",)])
        self.assertEqual(detector.observations({"big": {"x": 1}, "a": 1}), [])

    def test_deduper_admits_each_observation_once_per_interval(self) -> None:
        now = [100.0]
        deduper = RequestDeduper(interval_seconds=10, clock=lambda: now[0])
        observation = [(("a",), "double")]
        self.assertEqual(deduper.admit(observation), observation)
        self.assertEqual(deduper.admit(observation), [])
        now[0] = 111.0
        self.assertEqual(deduper.admit(observation), observation)


class RenderTests(unittest.TestCase):
    def test_projection_folds_forks_under_one_logical_name(self) -> None:
        base = schema_of({"total": 1, "customer": {"email": "e"}, "items": [{"sku": "s"}]})
        evolution = evolved(base, {"total": 2.5})
        rendered = render_schema(evolution.schema, evolution.fork_map, schemas=[base.schema, evolution.schema])
        by_path = {tuple(item["path"]): item for item in rendered["fields"]}
        self.assertEqual(by_path[("total",)]["name"], "total")
        self.assertEqual(by_path[("total",)]["state"], "superseded")
        self.assertEqual(by_path[("total__v2",)]["name"], "total")
        self.assertEqual(by_path[("total__v2",)]["fork_of"], by_path[("total",)]["field_id"])
        self.assertEqual(by_path[("total__v2",)]["since_schema_id"], evolution.schema.schema_id)
        self.assertEqual(by_path[("total",)]["since_schema_id"], 0)
        self.assertEqual(by_path[("items",)]["type"], "list<struct>")
        self.assertEqual(by_path[("items",)]["kind"], "container")
        self.assertEqual(by_path[("items", "[]", "sku")]["name"], "items[].sku")
        self.assertEqual(by_path[("customer", "email")]["name"], "customer.email")
        self.assertEqual(by_path[("_kafka_timestamp",)]["kind"], "metadata")
        self.assertEqual(rendered["field_count"], len([item for item in rendered["fields"] if item["kind"] != "metadata"]))

    def test_logical_name(self) -> None:
        self.assertEqual(logical_name(("a", "[]", "b", "[]")), "a[].b[]")


if __name__ == "__main__":
    unittest.main()
