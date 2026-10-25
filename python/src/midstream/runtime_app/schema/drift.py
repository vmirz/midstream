"""Drift detection: what in a record the table's schema cannot take yet (spec §7.2).

Runs on the writer's event loop for every record, so it is a single walk of the record against an
index built once per schema id. It never holds the record: whatever it finds becomes a
`schema_change_request` for the committer, and the record is staged as it is. It agrees with the
seal's conform step on what fits where (the same family index and acceptance rules), so a value it
does not report is a value conform will place.
"""

from __future__ import annotations

from collections.abc import Iterable
import json
import time
from typing import Any

from midstream.lakehouse.schema.conform import NOT_TAKEN, scalar_value
from midstream.lakehouse.schema.forkmap import ELEMENT
from midstream.lakehouse.schema.index import ElementIndex, Family, SchemaIndex, StructIndex
from midstream.lakehouse.schema.infer import infer, is_unknown

Observation = tuple[tuple[str, ...], Any]

#: An observation already requested is requested again at most this often (spec §7.2).
REQUEST_DEDUPE_SECONDS = 10.0
#: Distinct observations remembered for deduplication before the oldest are forgotten.
DEDUPE_LIMIT = 10_000


class DriftDetector:
    """Finds a record's unplaceable values against one schema."""

    def __init__(self, index: SchemaIndex, *, rejected_paths: Iterable[tuple[str, ...]] = ()) -> None:
        self.index = index
        self.schema_id = index.schema_id
        self._rejected = frozenset(tuple(path) for path in rejected_paths)

    def observations(self, value: Any) -> list[Observation]:
        """`(logical path, observed type)` for each part of `value` the schema cannot take."""
        if not isinstance(value, dict):
            return []
        found: list[Observation] = []
        self._struct(self.index.root, value, (), found)
        if not found:
            return found
        unique: dict[str, Observation] = {}
        for path, observed in found:
            if self._is_rejected(path):
                continue
            unique.setdefault(observation_key(path, observed), (path, observed))
        return list(unique.values())

    def _is_rejected(self, path: tuple[str, ...]) -> bool:
        if not self._rejected:
            return False
        return any(path[: len(rejected)] == rejected for rejected in self._rejected)

    def _struct(self, struct: StructIndex, value: dict, path: tuple[str, ...], found: list[Observation]) -> None:
        families = struct.families
        for key, item in value.items():
            if item is None:
                continue
            family = families.get(key)
            item_path = path + (key,)
            if family is None:
                observed = infer(item)
                if not is_unknown(observed):
                    found.append((item_path, observed))
                continue
            if family.collapsed:
                continue
            if not self._family_takes(family, item, item_path, found):
                found.append((item_path, infer(item)))

    def _family_takes(self, family: Family, item: Any, path: tuple[str, ...], found: list[Observation]) -> bool:
        if isinstance(item, dict):
            for member in sorted(family.members, key=lambda member: member.fork_version, reverse=True):
                if member.struct is not None and not member.json:
                    self._struct(member.struct, item, path, found)
                    return True
            return False
        if isinstance(item, list):
            for member in sorted(family.members, key=lambda member: member.fork_version, reverse=True):
                if member.element is None or member.json:
                    continue
                nested: list[Observation] = []
                if self._list_takes(member.element, item, path, nested):
                    found.extend(nested)
                    return True
            return False
        for member in family.members:
            if member.struct is None and member.element is None and not member.json:
                if scalar_value(member.type, item) is not NOT_TAKEN:
                    return True
        return False

    def _list_takes(self, element: ElementIndex, items: list, path: tuple[str, ...], found: list[Observation]) -> bool:
        if element.json:
            return True
        element_path = path + (ELEMENT,)
        for item in items:
            if item is None:
                continue
            if element.struct is not None:
                if not isinstance(item, dict):
                    return False
                self._struct(element.struct, item, element_path, found)
            elif element.element is not None:
                if not isinstance(item, list) or not self._list_takes(element.element, item, element_path, found):
                    return False
            elif scalar_value(element.type, item) is NOT_TAKEN:
                return False
        return True


def observation_key(path: tuple[str, ...], observed: Any) -> str:
    return json.dumps([list(path), observed], sort_keys=True, separators=(",", ":"))


class RequestDeduper:
    """Lets each distinct observation through at most once per `REQUEST_DEDUPE_SECONDS`, per process."""

    def __init__(self, *, interval_seconds: float = REQUEST_DEDUPE_SECONDS, clock=time.monotonic) -> None:
        self.interval_seconds = interval_seconds
        self._clock = clock
        self._last: dict[str, float] = {}

    def admit(self, observations: list[Observation]) -> list[Observation]:
        if not observations:
            return observations
        now = self._clock()
        admitted: list[Observation] = []
        for path, observed in observations:
            key = observation_key(path, observed)
            last = self._last.get(key)
            if last is not None and now - last < self.interval_seconds:
                continue
            self._last[key] = now
            admitted.append((path, observed))
        if len(self._last) > DEDUPE_LIMIT:
            for key, _ in sorted(self._last.items(), key=lambda item: item[1])[: len(self._last) - DEDUPE_LIMIT]:
                del self._last[key]
        return admitted

    def reset(self) -> None:
        self._last.clear()
