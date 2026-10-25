"""What a JSON value says about its type: observations (spec §8.2).

An observation is plain JSON so it can travel on a topic and be compared as text:

- a scalar observation is one of `"boolean"`, `"long"`, `"double"`, `"string"`, `"json"`, or
  `"unknown"` (a null, which carries no type);
- `{"type": "struct", "fields": {key: observation}}` for an object;
- `{"type": "list", "element": observation}` for an array, its element the union of its items'.

No timestamps, decimals or UUIDs are inferred from strings: a wrong guess would cost a fork.
"""

from __future__ import annotations

from typing import Any

BOOLEAN = "boolean"
LONG = "long"
DOUBLE = "double"
STRING = "string"
JSON = "json"
UNKNOWN = "unknown"

SCALARS = frozenset({BOOLEAN, LONG, DOUBLE, STRING, JSON})
NUMERIC = frozenset({LONG, DOUBLE})

_INT64_MIN = -(2**63)
_INT64_MAX = 2**63 - 1

Observation = Any


def infer(value: Any) -> Observation:
    """The observation for one JSON value."""
    if value is None:
        return UNKNOWN
    if isinstance(value, bool):
        return BOOLEAN
    if isinstance(value, int):
        return LONG if _INT64_MIN <= value <= _INT64_MAX else DOUBLE
    if isinstance(value, float):
        return DOUBLE
    if isinstance(value, str):
        return STRING
    if isinstance(value, dict):
        return {"type": "struct", "fields": {str(key): infer(item) for key, item in value.items()}}
    if isinstance(value, (list, tuple)):
        element: Observation = UNKNOWN
        for item in value:
            element = union(element, infer(item))
        return {"type": "list", "element": element}
    return JSON


def union(left: Observation, right: Observation) -> Observation:
    """The observation both values fit, as the elements of one array must (spec §8.2).

    Numbers widen to double; objects merge key by key; arrays merge their elements; anything else
    that disagrees - text and numbers, objects and scalars - can only be held as JSON text.
    """
    if left == UNKNOWN:
        return right
    if right == UNKNOWN:
        return left
    if left == right:
        return left
    if isinstance(left, str) and isinstance(right, str) and left in NUMERIC and right in NUMERIC:
        return DOUBLE
    if is_struct(left) and is_struct(right):
        fields = dict(left["fields"])
        for key, item in right["fields"].items():
            fields[key] = union(fields[key], item) if key in fields else item
        return {"type": "struct", "fields": fields}
    if is_list(left) and is_list(right):
        return {"type": "list", "element": union(left["element"], right["element"])}
    return JSON


def is_struct(observation: Observation) -> bool:
    return isinstance(observation, dict) and observation.get("type") == "struct"


def is_list(observation: Observation) -> bool:
    return isinstance(observation, dict) and observation.get("type") == "list"


def is_unknown(observation: Observation) -> bool:
    """Whether the observation carries no type at all: a null, or nothing but nulls and empties."""
    if observation == UNKNOWN or observation is None:
        return True
    if is_list(observation):
        return is_unknown(observation["element"])
    if is_struct(observation):
        return all(is_unknown(item) for item in observation["fields"].values())
    return False


def normalize(observation: Observation) -> Observation:
    """Accept the spelling `{"type": "double"}` for a scalar as well as `"double"`."""
    if isinstance(observation, dict):
        kind = observation.get("type")
        if kind == "struct":
            return {
                "type": "struct",
                "fields": {str(key): normalize(item) for key, item in (observation.get("fields") or {}).items()},
            }
        if kind == "list":
            return {"type": "list", "element": normalize(observation.get("element", UNKNOWN))}
        if kind in SCALARS or kind == UNKNOWN:
            return kind
        return JSON
    if isinstance(observation, str) and (observation in SCALARS or observation == UNKNOWN):
        return observation
    return JSON


def describe(observation: Observation) -> str:
    """A short text form for logs and messages: `struct<a: long>`, `list<string>`."""
    if is_struct(observation):
        inner = ", ".join(f"{key}: {describe(item)}" for key, item in observation["fields"].items())
        return f"struct<{inner}>"
    if is_list(observation):
        return f"list<{describe(observation['element'])}>"
    return str(observation)
