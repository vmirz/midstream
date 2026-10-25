"""The fork map: which physical fields hold one logical field (spec §8.4).

A logical field is a record key at one place in the record. Usually it is one Iceberg field named
after the key. When the key's values change type in a way Iceberg cannot widen, the field gets
siblings - `total__v2`, `total__v3` - each of one type: together they are its *family*. When a
family has forked too often, or a struct grows too wide, it *collapses*: from then on its values
are written as JSON text to `<name>__json`. A key that cannot be a field name as it stands - empty,
a case-insensitive twin of a sibling, a metadata column's name - is *renamed*.

The map records exactly those fields, keyed by field id, and nothing else: a plain field named after
its key has no entry. Stored as the `materializer.fork-map` table property and alongside the schema
in the runtime's state table; the seal's conform step and the console's logical columns read it.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import json
from typing import Any

CURRENT = "current"
SUPERSEDED = "superseded"
COLLAPSED = "collapsed"
STATES = (CURRENT, SUPERSEDED, COLLAPSED)

#: Path segment standing for a list's element.
ELEMENT = "[]"


@dataclass(frozen=True, slots=True)
class ForkEntry:
    """One field that is not simply named after its key, or that belongs to a family."""

    field_id: int
    #: Physical names from the top of the record; `[]` for a list's element.
    path: tuple[str, ...]
    #: The record key this field holds values of, when it is not the field's own name.
    key: str | None = None
    #: Field id of the family's first member; None for that member itself.
    fork_of: int | None = None
    fork_version: int = 1
    state: str = CURRENT

    def __post_init__(self) -> None:
        if self.state not in STATES:
            raise ValueError(f"fork map state must be one of: {', '.join(STATES)}")
        if self.fork_version < 1:
            raise ValueError("fork_version must be >= 1")

    @property
    def is_json(self) -> bool:
        return self.state == COLLAPSED

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "path": list(self.path),
            "fork_of": self.fork_of,
            "fork_version": self.fork_version,
            "state": self.state,
        }
        if self.key is not None:
            payload["key"] = self.key
        return payload

    @classmethod
    def from_dict(cls, field_id: int, value: dict[str, Any]) -> "ForkEntry":
        fork_of = value.get("fork_of")
        return cls(
            field_id=int(field_id),
            path=tuple(value.get("path") or ()),
            key=value.get("key"),
            fork_of=None if fork_of is None else int(fork_of),
            fork_version=int(value.get("fork_version") or 1),
            state=str(value.get("state") or CURRENT),
        )


@dataclass(frozen=True, slots=True)
class ForkMap:
    entries: dict[int, ForkEntry] = field(default_factory=dict)

    def get(self, field_id: int) -> ForkEntry | None:
        return self.entries.get(field_id)

    def __len__(self) -> int:
        return len(self.entries)

    def with_entries(self, *updates: ForkEntry) -> "ForkMap":
        entries = dict(self.entries)
        for entry in updates:
            entries[entry.field_id] = entry
        return ForkMap(entries)

    def to_dict(self) -> dict[str, Any]:
        return {"fields": {str(field_id): self.entries[field_id].to_dict() for field_id in sorted(self.entries)}}

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":"))

    @classmethod
    def from_dict(cls, value: dict[str, Any] | None) -> "ForkMap":
        if not isinstance(value, dict):
            return cls()
        fields = value.get("fields") or {}
        return cls({int(field_id): ForkEntry.from_dict(int(field_id), item) for field_id, item in fields.items()})

    @classmethod
    def from_json(cls, value: str | None) -> "ForkMap":
        if not value:
            return cls()
        return cls.from_dict(json.loads(value))
