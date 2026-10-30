"""A materialization's protection policy, and what a table records of it (design §5.3, §6).

The policy is which kinds a materialization masks, each with its mask:

    {"kinds": {"email": "hash", "phone": "partial", "secret": "redact"}}

It is resolved from the deployment default when a materialization is created, recorded on the
materialization with a revision, delivered to its runtime at every apply, and recorded by the
runtime's committer in each table's `materializer.protection` property, which is what the writers,
the live tail and every reader of the table go by.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field, replace
import json
from typing import Any

from midstream.lakehouse.tables import PROPERTY_PROTECTION
from midstream.protection.catalog import KIND_IDS, KINDS_BY_ID, MASK_HASH, MASKS
#: The version of the property's shape.
PROTECTION_FORMAT_VERSION = 1

#: Policy keys reserved for later versions (design §15): per-field choices and per-table policies.
RESERVED_POLICY_KEYS = ("fields", "tables")


class PolicyError(ValueError):
    """A policy a request may not set; `field` names the offending part, as the API reports it."""

    def __init__(self, message: str, *, field: str) -> None:
        super().__init__(message)
        self.field = field


@dataclass(frozen=True, slots=True)
class ProtectionPolicy:
    """Which kinds are masked, each with its mask. `ProtectionPolicy()` protects nothing."""

    kinds: Mapping[str, str] = field(default_factory=dict)

    @property
    def empty(self) -> bool:
        return not self.kinds

    @property
    def hashes(self) -> bool:
        """Whether any kind is hashed, which needs the protection key."""
        return any(mask == MASK_HASH for mask in self.kinds.values())

    def mask_of(self, kind_id: str | None) -> str | None:
        return None if kind_id is None else self.kinds.get(kind_id)

    def to_dict(self) -> dict[str, Any]:
        return {"kinds": {kind_id: self.kinds[kind_id] for kind_id in _ordered(self.kinds)}}

    @classmethod
    def from_dict(cls, value: Any) -> "ProtectionPolicy":
        """A stored policy, lenient: kinds or masks this release does not know are left out, so a
        record written by a newer release never stops an older one."""
        if not isinstance(value, Mapping):
            return cls()
        kinds = value.get("kinds")
        if not isinstance(kinds, Mapping):
            return cls()
        resolved = {
            str(kind_id): str(mask)
            for kind_id, mask in kinds.items()
            if kind_id in KINDS_BY_ID and KINDS_BY_ID[kind_id].allows(str(mask))
        }
        return cls(kinds=resolved)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, ProtectionPolicy) and dict(self.kinds) == dict(other.kinds)

    def __hash__(self) -> int:
        return hash(tuple(sorted(self.kinds.items())))


NONE = ProtectionPolicy()


def _ordered(kinds: Iterable[str]) -> list[str]:
    """Kind ids in the catalog's order, unknown ones last."""
    known = [kind_id for kind_id in KIND_IDS if kind_id in kinds]
    return known + sorted(kind_id for kind_id in kinds if kind_id not in KINDS_BY_ID)


def parse_policy(value: Any, *, field_name: str = "protection", allow: Iterable[str] = ()) -> ProtectionPolicy:
    """The policy a request asks for, validated; `PolicyError` naming the field otherwise.

    `{"kinds": {kind: mask}}`, or `{"kinds": [kind, …]}` for each kind's default mask, so the stored
    policy always says what it does. `allow` names further keys the caller handles itself (an
    update's `apply_rules`).
    """
    if not isinstance(value, Mapping):
        raise PolicyError(f"{field_name} must be an object with kinds", field=field_name)
    for key in value:
        if key in RESERVED_POLICY_KEYS:
            raise PolicyError(f"{field_name}.{key} is not supported in this version", field=f"{field_name}.{key}")
        if key != "kinds" and key not in allow:
            raise PolicyError(f"{field_name} has an unknown key {key!r}", field=f"{field_name}.{key}")
    kinds = value.get("kinds", {})
    resolved: dict[str, str] = {}
    if isinstance(kinds, list):
        for index, kind_id in enumerate(kinds):
            item = KINDS_BY_ID.get(kind_id) if isinstance(kind_id, str) else None
            if item is None:
                raise PolicyError(
                    f"{field_name}.kinds[{index}] must be one of: {', '.join(KIND_IDS)}",
                    field=f"{field_name}.kinds.{index}",
                )
            resolved[item.id] = item.default_mask
    elif isinstance(kinds, Mapping):
        for kind_id, mask in kinds.items():
            item = KINDS_BY_ID.get(kind_id) if isinstance(kind_id, str) else None
            if item is None:
                raise PolicyError(
                    f"{field_name}.kinds has an unknown kind {kind_id!r}; kinds are: {', '.join(KIND_IDS)}",
                    field=f"{field_name}.kinds.{kind_id}",
                )
            if mask not in MASKS:
                raise PolicyError(
                    f"{field_name}.kinds.{kind_id} must be one of: {', '.join(MASKS)}",
                    field=f"{field_name}.kinds.{kind_id}",
                )
            if not item.allows(mask):
                raise PolicyError(
                    f"{item.label} cannot be masked with {mask}; allowed: {', '.join(item.masks)}",
                    field=f"{field_name}.kinds.{kind_id}",
                )
            resolved[item.id] = mask
    else:
        raise PolicyError(f"{field_name}.kinds must be an object of kind to mask, or a list of kinds", field=f"{field_name}.kinds")
    return ProtectionPolicy(kinds=resolved)


#: How a field's values were written from one protection revision on: `(revision, kind, mask)`,
#: with kind and mask None while they were written plain.
FieldState = tuple[int, "str | None", "str | None"]


class _Unknown:
    """What `FieldProtection.state_at` answers for a revision its record no longer describes."""

    __slots__ = ()

    def __repr__(self) -> str:
        return "UNKNOWN"


UNKNOWN = _Unknown()

#: States a field keeps; the oldest beyond it are dropped, and with them what is known before.
STATES_LIMIT = 100


@dataclass(frozen=True, slots=True)
class FieldProtection:
    """What a table records of one field: its kind and mask, from which schema, until which, and
    how its values were masked under each protection revision.

    `since` is the schema id from which the field's values were masked; `until`, when set, the
    schema id from which they are written plain again (its kind left the policy). The path is the
    field's physical path. `states` says, revision by revision, what the writers did to the field's
    values (oldest first); before its first state they wrote them plain, or the field did not exist
    yet. A data file's name says the revision it was written under, so together they say whether a
    file holds the field's values plain - what `reprotect` rewrites (design §15.3). `known_from` is
    the first revision `states` describes in full: 0 for a field recorded with states from the
    start, the revision a record was first rewritten at when it was written before states were kept.
    """

    field_id: int
    path: tuple[str, ...]
    kind: str
    mask: str
    since: int
    until: int | None = None
    states: tuple[FieldState, ...] = ()
    known_from: int = 0

    @property
    def active(self) -> bool:
        return self.until is None

    def state_at(self, revision: int) -> "tuple[str, str] | None | _Unknown":
        """How the field's values were masked under `revision`: `(kind, mask)`, None for plain, or
        `UNKNOWN` for a revision before `known_from`. Revision 0 - no protection - is always plain."""
        if revision <= 0:
            return None
        if revision < self.known_from:
            return UNKNOWN
        current: tuple[str, str] | None = None
        for state_revision, kind, mask in self.states:
            if state_revision > revision:
                break
            current = None if kind is None or mask is None else (kind, mask)
        return current

    def with_state(self, revision: int, state_kind: str | None, state_mask: str | None, **changes: Any) -> "FieldProtection":
        """The field with its state from `revision` on recorded - replacing one recorded at the same
        revision - and `changes` applied."""
        states = [state for state in self.states if state[0] < revision] + [(revision, state_kind, state_mask)]
        known_from = self.known_from
        if len(states) > STATES_LIMIT:
            states = states[-STATES_LIMIT:]
            known_from = max(known_from, states[0][0])
        return replace(self, states=tuple(states), known_from=known_from, **changes)

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {"path": list(self.path), "kind": self.kind, "mask": self.mask, "since": self.since}
        if self.until is not None:
            payload["until"] = self.until
        payload["states"] = [list(state) for state in self.states]
        if self.known_from:
            payload["known_from"] = self.known_from
        return payload

    @classmethod
    def from_dict(cls, field_id: int, value: Mapping[str, Any]) -> "FieldProtection":
        until = value.get("until")
        states: list[FieldState] = []
        for item in value.get("states") or ():
            if isinstance(item, (list, tuple)) and len(item) == 3:
                kind, mask = item[1], item[2]
                states.append((int(item[0]), None if kind is None else str(kind), None if mask is None else str(mask)))
        states.sort(key=lambda state: state[0])
        return cls(
            field_id=int(field_id),
            path=tuple(str(item) for item in value.get("path") or ()),
            kind=str(value["kind"]),
            mask=str(value["mask"]),
            since=int(value.get("since") or 0),
            until=None if until is None else int(until),
            states=tuple(states),
            # A record from before states were kept: the table's from_dict anchors it.
            known_from=int(value.get("known_from") or 0) if "states" in value else -1,
        )


@dataclass(frozen=True, slots=True)
class ProtectionChange:
    """One policy a table took on: when, by whom, at which schema, with which key."""

    revision: int
    policy: ProtectionPolicy
    at: str | None = None
    by: str | None = None
    schema_id: int | None = None
    sequence_number: int | None = None
    key_id: str | None = None
    reason: str | None = None

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "revision": self.revision,
            "policy": self.policy.to_dict(),
            "at": self.at,
            "by": self.by,
            "schema_id": self.schema_id,
            "sequence_number": self.sequence_number,
            "key_id": self.key_id,
        }
        if self.reason is not None:
            payload["reason"] = self.reason
        return payload

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "ProtectionChange":
        def optional_int(item: Any) -> int | None:
            return None if item is None else int(item)

        return cls(
            revision=int(value.get("revision") or 0),
            policy=ProtectionPolicy.from_dict(value.get("policy")),
            at=value.get("at"),
            by=value.get("by"),
            schema_id=optional_int(value.get("schema_id")),
            sequence_number=optional_int(value.get("sequence_number")),
            key_id=value.get("key_id"),
            reason=value.get("reason"),
        )


#: History entries a table keeps; the oldest beyond it are dropped.
HISTORY_LIMIT = 100


@dataclass(frozen=True, slots=True)
class TableProtection:
    """The `materializer.protection` property: the policy a table runs, and what it masks.

    `revision` is the materialization's policy revision the table recorded last; a data file's name
    carries the revision its rows were masked under (`lakehouse/files.py`). `fields` holds every
    field that is or was protected, by field id.
    """

    revision: int = 0
    policy: ProtectionPolicy = NONE
    key_id: str | None = None
    fields: Mapping[int, FieldProtection] = field(default_factory=dict)
    history: tuple[ProtectionChange, ...] = ()
    version: int = PROTECTION_FORMAT_VERSION

    @property
    def empty(self) -> bool:
        """Nothing protected now, and nothing to protect: the writers skip the walk."""
        return self.policy.empty and not any(item.active for item in self.fields.values())

    def active_fields(self) -> dict[int, FieldProtection]:
        return {field_id: item for field_id, item in self.fields.items() if item.active}

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "revision": self.revision,
            "policy": self.policy.to_dict(),
            "key_id": self.key_id,
            "fields": {str(field_id): self.fields[field_id].to_dict() for field_id in sorted(self.fields)},
            "history": [change.to_dict() for change in self.history[-HISTORY_LIMIT:]],
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":"))

    @classmethod
    def from_dict(cls, value: Any) -> "TableProtection":
        if not isinstance(value, Mapping):
            return cls()
        revision = int(value.get("revision") or 0)
        fields: dict[int, FieldProtection] = {}
        for field_id, item in (value.get("fields") or {}).items():
            if not isinstance(item, Mapping):
                continue
            try:
                parsed = FieldProtection.from_dict(int(field_id), item)
            except (KeyError, TypeError, ValueError):
                continue
            if parsed.known_from < 0:
                # Recorded before states were kept: what it says now holds at the table's revision,
                # and nothing is known of the revisions before.
                state = (revision, parsed.kind, parsed.mask) if parsed.active else (revision, None, None)
                parsed = replace(parsed, states=(state,) if revision > 0 else (), known_from=revision)
            fields[parsed.field_id] = parsed
        history = []
        for item in value.get("history") or ():
            if isinstance(item, Mapping):
                try:
                    history.append(ProtectionChange.from_dict(item))
                except (TypeError, ValueError):
                    continue
        return cls(
            revision=revision,
            policy=ProtectionPolicy.from_dict(value.get("policy")),
            key_id=value.get("key_id"),
            fields=fields,
            history=tuple(history),
            version=int(value.get("version") or PROTECTION_FORMAT_VERSION),
        )

    @classmethod
    def from_json(cls, value: str | None) -> "TableProtection":
        if not value:
            return cls()
        try:
            return cls.from_dict(json.loads(value))
        except (TypeError, ValueError):
            return cls()

    def policy_of_revision(self, revision: int) -> ProtectionPolicy | None:
        """The policy a revision stood for, from the history; None when the history has lost it."""
        for change in reversed(self.history):
            if change.revision == revision:
                return change.policy
        return None


def protection_of_properties(properties: Mapping[str, str] | None) -> TableProtection:
    """A table's recorded protection, from its properties; none recorded reads as nothing protected."""
    if not properties:
        return TableProtection()
    return TableProtection.from_json(properties.get(PROPERTY_PROTECTION))
