"""The hands-free schema policy (spec §8.3), row by row, with its caps and naming rules."""

from __future__ import annotations

import json
import unittest

from materializer_control_plane.lakehouse.schema.forkmap import COLLAPSED, CURRENT, SUPERSEDED, ForkMap
from materializer_control_plane.lakehouse.schema.index import build_index
from materializer_control_plane.lakehouse.schema.infer import infer, union
from materializer_control_plane.lakehouse.schema.policy import (
    ADDED,
    COLLAPSED_CHANGE,
    FORKED,
    REJECTED,
    SchemaLimits,
    evolve,
)
from materializer_control_plane.lakehouse.tables import FIRST_PAYLOAD_FIELD_ID, build_schema


def start(*records, limits=None):
    """Schema 0 from records, as discovery builds it."""
    return evolve(build_schema(), ForkMap(), [((), infer(record)) for record in records], limits=limits, schema_id=0)


def then(evolution, *records, limits=None):
    return evolve(evolution.schema, evolution.fork_map, [((), infer(record)) for record in records], limits=limits, last_column_id=evolution.last_column_id)


def members(evolution, *path):
    """`[(physical name, kind, state)]` of the family at `path` (keys; `[]` for elements)."""
    index = build_index(evolution.schema, evolution.fork_map)
    struct = index.root
    family = None
    remaining = list(path)
    while remaining:
        key = remaining.pop(0)
        if key == "[]":
            continue
        family = struct.families[key]
        if not remaining:
            break
        nxt = remaining[0]
        if nxt == "[]":
            member = next(item for item in family.members if item.element is not None and not item.json)
            element = member.element
            remaining.pop(0)
            struct = element.struct
        else:
            member = next(item for item in family.members if item.struct is not None and not item.json)
            struct = member.struct
    return [(item.name, item.kind, item.state) for item in family.members]


class InferenceTests(unittest.TestCase):
    def test_scalars_objects_and_arrays(self) -> None:
        self.assertEqual(infer(True), "boolean")
        self.assertEqual(infer(3), "long")
        self.assertEqual(infer(2**64), "double")
        self.assertEqual(infer(1.5), "double")
        self.assertEqual(infer("x"), "string")
        self.assertEqual(infer(None), "unknown")
        self.assertEqual(infer({"a": 1}), {"type": "struct", "fields": {"a": "long"}})
        self.assertEqual(infer([]), {"type": "list", "element": "unknown"})
        self.assertEqual(infer([1, 2.5]), {"type": "list", "element": "double"})
        self.assertEqual(infer([1, "a"]), {"type": "list", "element": "json"})
        self.assertEqual(infer([{"a": 1}, {"b": "x"}]), {"type": "list", "element": {"type": "struct", "fields": {"a": "long", "b": "string"}}})

    def test_union_widens_numbers_and_merges_structures(self) -> None:
        self.assertEqual(union("long", "double"), "double")
        self.assertEqual(union("unknown", "string"), "string")
        self.assertEqual(union("string", "long"), "json")
        self.assertEqual(union({"type": "struct", "fields": {}}, "long"), "json")


class PolicyRowTests(unittest.TestCase):
    def test_nothing_then_typed_adds_a_nullable_field(self) -> None:
        evolution = start({"status": "open"})
        field = evolution.schema.find_field("status")
        self.assertFalse(field.required)
        self.assertGreaterEqual(field.field_id, FIRST_PAYLOAD_FIELD_ID)
        self.assertEqual([change.kind for change in evolution.changes], [ADDED])

    def test_nothing_then_null_defers(self) -> None:
        evolution = start({"note": None, "items": [], "meta": {}, "deep": {"a": None}})
        self.assertFalse(evolution.changed)
        self.assertEqual(len(evolution.schema.fields), 7)

    def test_long_then_long_changes_nothing(self) -> None:
        self.assertFalse(then(start({"n": 1}), {"n": 2}).changed)

    def test_long_then_double_forks(self) -> None:
        evolution = then(start({"total": 1}), {"total": 1.5})
        self.assertEqual(members(evolution, "total"), [("total", "long", SUPERSEDED), ("total__v2", "double", CURRENT)])
        self.assertEqual([change.kind for change in evolution.changes], [FORKED])

    def test_double_then_long_changes_nothing(self) -> None:
        self.assertFalse(then(start({"total": 1.5}), {"total": 2}).changed)

    def test_string_then_any_scalar_changes_nothing(self) -> None:
        base = start({"code": "A1"})
        for value in (5, 2.5, True):
            self.assertFalse(then(base, {"code": value}).changed, value)

    def test_scalar_then_struct_or_list_forks(self) -> None:
        evolution = then(start({"phone": "+1"}), {"phone": {"number": "555"}})
        self.assertEqual(members(evolution, "phone"), [("phone", "string", SUPERSEDED), ("phone__v2", "struct", CURRENT)])
        evolution = then(start({"tags": "a"}), {"tags": ["a", "b"]})
        self.assertEqual(members(evolution, "tags")[1][1], "list")

    def test_struct_then_list_forks(self) -> None:
        evolution = then(start({"category": {"name": "x"}}), {"category": ["x"]})
        self.assertEqual([kind for _, kind, _ in members(evolution, "category")], ["struct", "list"])

    def test_struct_with_new_keys_adds_inside_the_struct(self) -> None:
        evolution = then(start({"customer": {"email": "a"}}), {"customer": {"email": "b", "phone": "1"}})
        self.assertEqual([change.kind for change in evolution.changes], [ADDED])
        self.assertEqual(evolution.changes[0].path, ("customer", "phone"))
        self.assertIsNotNone(evolution.schema.find_field("customer.phone"))

    def test_list_recurse_into_struct_elements(self) -> None:
        evolution = then(start({"items": [{"sku": "A"}]}), {"items": [{"sku": "B", "tax": 1.5}]})
        self.assertEqual(evolution.changes[0].path, ("items", "[]", "tax"))
        self.assertEqual(evolution.changes[0].kind, ADDED)

    def test_list_element_conflict_forks_the_list_field(self) -> None:
        evolution = then(start({"scores": [1, 2]}), {"scores": [1.5]})
        self.assertEqual([kind for _, kind, _ in members(evolution, "scores")], ["list", "list"])
        self.assertEqual(evolution.changes[0].kind, FORKED)
        self.assertEqual(evolution.changes[0].type, "list<double>")

    def test_conflict_inside_list_element_struct_forks_that_field(self) -> None:
        evolution = then(start({"items": [{"category": ["a"]}]}), {"items": [{"category": {"name": "a"}}]})
        self.assertEqual(
            [kind for _, kind, _ in members(evolution, "items", "[]", "category")],
            ["list", "struct"],
        )

    def test_a_type_an_existing_fork_has_needs_no_change(self) -> None:
        forked = then(start({"total": 1}), {"total": 1.5})
        self.assertFalse(then(forked, {"total": 7}).changed)
        self.assertFalse(then(forked, {"total": 7.25}).changed)

    def test_too_many_forks_collapse_to_json(self) -> None:
        evolution = start({"v": 1})
        for value in (1.5, "s", {"a": 1}):
            evolution = then(evolution, {"v": value})
        # long, double, struct... string goes to... v1 long, v2 double, v3 string, v4 struct: three forks.
        self.assertEqual(sum(1 for _, _, state in members(evolution, "v") if state != COLLAPSED), 4)
        collapsed = then(evolution, {"v": [1]})
        self.assertEqual(collapsed.changes[0].kind, COLLAPSED_CHANGE)
        names = members(collapsed, "v")
        self.assertEqual(names[-1], ("v__json", "json", COLLAPSED))
        self.assertTrue(all(state == SUPERSEDED for _, _, state in names[:-1]))
        # Nothing beneath a collapsed field evolves any more.
        self.assertFalse(then(collapsed, {"v": {"b": 2}}).changed)

    def test_too_wide_struct_collapses_to_json(self) -> None:
        limits = SchemaLimits(max_struct_fields=3)
        evolution = start({"attrs": {"a": 1, "b": 2}}, limits=limits)
        evolution = then(evolution, {"attrs": {"a": 1, "c": 3}}, limits=limits)
        self.assertEqual(len(evolution.schema.find_field("attrs").field_type.fields), 3)
        collapsed = then(evolution, {"attrs": {"d": 4}}, limits=limits)
        self.assertEqual([change.kind for change in collapsed.changes], [COLLAPSED_CHANGE])
        self.assertEqual(collapsed.changes[0].reason, "max_struct_fields")
        self.assertEqual(members(collapsed, "attrs")[-1], ("attrs__json", "json", COLLAPSED))

    def test_a_new_struct_wider_than_the_cap_is_json_from_the_start(self) -> None:
        limits = SchemaLimits(max_struct_fields=2)
        evolution = start({"attrs": {"a": 1, "b": 2, "c": 3}}, limits=limits)
        self.assertEqual(members(evolution, "attrs"), [("attrs__json", "json", COLLAPSED)])

    def test_depth_beyond_the_limit_collapses_at_the_limit(self) -> None:
        limits = SchemaLimits(max_nesting_depth=2)
        evolution = start({"a": {"b": {"c": 1}}}, limits=limits)
        self.assertEqual(members(evolution, "a"), [("a", "struct", CURRENT)])
        self.assertEqual(members(evolution, "a", "b"), [("b__json", "json", COLLAPSED)])

    def test_field_limit_rejects_and_leaves_the_schema_alone(self) -> None:
        limits = SchemaLimits(max_schema_fields=10)
        evolution = start({"a": 1}, limits=limits)
        self.assertEqual(len(evolution.schema.fields), 8)  # 7 metadata + a (+2 header ids = 10)
        rejected = then(evolution, {"b": 2}, limits=limits)
        self.assertFalse(rejected.changed)
        self.assertEqual(rejected.rejected[0].reason, "schema_field_limit")
        self.assertIs(rejected.schema, evolution.schema)


class NamingTests(unittest.TestCase):
    def test_keys_kept_verbatim_with_exceptions(self) -> None:
        evolution = start({"": 1, "ID": 1, "id": 2, "_kafka_offset": 3, "a.b": 4, "with space": 5})
        names = [field.name for field in evolution.schema.fields][7:]
        self.assertEqual(names, ["_", "ID", "id__k2", "_kafka_offset__payload", "a_b", "with space"])
        index = build_index(evolution.schema, evolution.fork_map)
        self.assertEqual(set(index.root.families), {"", "ID", "id", "_kafka_offset", "a.b", "with space"})

    def test_a_fork_never_takes_a_users_field_name(self) -> None:
        evolution = then(start({"total": 1, "total__v2": "x"}), {"total": 1.5})
        self.assertEqual(members(evolution, "total"), [("total", "long", SUPERSEDED), ("total__v2__k2", "double", CURRENT)])
        self.assertEqual(members(evolution, "total__v2"), [("total__v2", "string", CURRENT)])


class EvolutionMechanicsTests(unittest.TestCase):
    def test_ids_continue_from_last_column_id_and_schema_ids_advance(self) -> None:
        base = start({"a": 1})
        later = evolve(base.schema, base.fork_map, [((), infer({"b": {"c": "x"}}))], last_column_id=50)
        self.assertEqual(later.schema.schema_id, base.schema.schema_id + 1)
        self.assertEqual(later.schema.find_field("b").field_id, 51)
        self.assertEqual(later.schema.find_field("b.c").field_id, 52)
        self.assertEqual(later.last_column_id, 52)

    def test_metadata_ids_are_fixed(self) -> None:
        evolution = start({"x": 1})
        self.assertEqual([field.field_id for field in evolution.schema.fields[:7]], [1, 2, 3, 4, 5, 6, 7])

    def test_observations_by_path(self) -> None:
        base = start({"customer": {"email": "a"}, "items": [{"sku": "A"}]})
        evolution = evolve(
            base.schema,
            base.fork_map,
            [(("customer", "phone"), {"type": "struct", "fields": {"number": "string"}}), (("items", "[]", "tax"), {"type": "double"})],
        )
        self.assertEqual({change.path for change in evolution.changes}, {("customer", "phone"), ("items", "[]", "tax")})

    def test_same_batch_long_and_double_create_a_double(self) -> None:
        evolution = start({"amount": 0}, {"amount": 2.5})
        self.assertEqual(members(evolution, "amount"), [("amount", "double", CURRENT)])

    def test_fork_map_round_trips(self) -> None:
        evolution = then(start({"total": 1, "ID": 1, "id": 1}), {"total": 1.5})
        restored = ForkMap.from_json(evolution.fork_map.to_json())
        self.assertEqual(restored, evolution.fork_map)
        payload = json.loads(evolution.fork_map.to_json())
        entries = {entry["path"][-1]: entry for entry in payload["fields"].values()}
        self.assertEqual(entries["total__v2"]["fork_of"], evolution.schema.find_field("total").field_id)
        self.assertEqual(entries["id__k2"]["key"], "id")

    def test_unchanged_evolution_returns_the_same_objects(self) -> None:
        base = start({"a": 1})
        again = then(base, {"a": 2})
        self.assertIs(again.schema, base.schema)
        self.assertIs(again.fork_map, base.fork_map)


if __name__ == "__main__":
    unittest.main()
