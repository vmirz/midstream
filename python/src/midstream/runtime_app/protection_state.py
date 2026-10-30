"""The protection policy the control plane delivers to a runtime (design §5.8, §6.4).

A `protection_policy` message on the runtime control topic carries the materialization's policy
and its revision. It is sent at every apply of the runtime - its creation, an edit, a resume, the
sweep's re-apply - so a runtime that missed one catches up at its next apply. The settings agent
stores the newest revision in the runtime's state table under `protection-policy`; the committer
records it in each table (`committer.py`), and the writers follow the tables.

This module imports nothing from the runtime's hot path, so the settings agent can use it alone.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import logging
from typing import Any

from midstream.protection.policy import ProtectionPolicy

LOGGER = logging.getLogger(__name__)

#: `kind` of a protection-policy message on the runtime control topic.
PROTECTION_POLICY_KIND = "protection_policy"
#: Key of the delivered policy in the runtime's state table.
PROTECTION_POLICY_KEY = "protection-policy"


@dataclass(frozen=True, slots=True)
class DeliveredPolicy:
    """The newest policy delivered to the runtime."""

    revision: int
    policy: ProtectionPolicy
    changed_by: str | None = None
    published_at: str | None = None
    applied_at: str | None = None
    reason: str | None = None

    def to_dict(self) -> dict[str, Any]:
        payload = {
            "revision": self.revision,
            "policy": self.policy.to_dict(),
            "changed_by": self.changed_by,
            "published_at": self.published_at,
            "applied_at": self.applied_at,
        }
        if self.reason is not None:
            payload["reason"] = self.reason
        return payload

    @classmethod
    def from_dict(cls, value: Any) -> "DeliveredPolicy | None":
        if not isinstance(value, dict):
            return None
        revision = value.get("revision")
        if isinstance(revision, bool) or not isinstance(revision, int) or revision < 0:
            return None
        return cls(
            revision=revision,
            policy=ProtectionPolicy.from_dict(value.get("policy")),
            changed_by=value.get("changed_by"),
            published_at=value.get("published_at"),
            applied_at=value.get("applied_at"),
            reason=value.get("reason"),
        )


def is_protection_policy_message(value: Any) -> bool:
    return isinstance(value, dict) and value.get("kind") == PROTECTION_POLICY_KIND


def read_delivered_policy(state_table: Any) -> DeliveredPolicy | None:
    """The newest policy delivered to this runtime; None when none was ever delivered."""
    if state_table is None:
        return None
    return DeliveredPolicy.from_dict(state_table.get(PROTECTION_POLICY_KEY))


def apply_protection_policy_message(
    value: dict[str, Any],
    state_table: Any,
    *,
    materialization_id: str | None = None,
    applied_at: str | None = None,
) -> DeliveredPolicy | None:
    """Store a delivered policy when it is newer than the stored one; return what was stored.

    One addressed to another materialization, one not newer than what is stored, or a malformed one
    changes nothing; it is never raised, because the same agent carries every later delivery.
    """
    target = value.get("materialization_id")
    if materialization_id is not None and target not in (None, materialization_id):
        return None
    delivered = DeliveredPolicy.from_dict(
        {
            "revision": value.get("revision"),
            "policy": value.get("policy"),
            "changed_by": value.get("changed_by"),
            "published_at": value.get("published_at"),
            "reason": value.get("reason"),
        }
    )
    if delivered is None:
        LOGGER.warning("Ignoring a protection policy without a valid revision: %r", value.get("revision"))
        return None
    stored = read_delivered_policy(state_table)
    if stored is not None and stored.revision >= delivered.revision:
        return None
    resolved = DeliveredPolicy(
        revision=delivered.revision,
        policy=delivered.policy,
        changed_by=delivered.changed_by,
        published_at=delivered.published_at,
        applied_at=applied_at or _utc_now(),
        reason=delivered.reason,
    )
    state_table[PROTECTION_POLICY_KEY] = resolved.to_dict()
    LOGGER.info(
        "Stored protection policy revision %s of %s: %s",
        resolved.revision,
        materialization_id,
        ", ".join(f"{kind}={mask}" for kind, mask in resolved.policy.kinds.items()) or "nothing protected",
    )
    return resolved


#: Why this member cannot hash right now, when it cannot: `protection_key_missing` or
#: `protection_key_mismatch` (`protection/secrets.py`). Reported on its heartbeat.
_KEY_PROBLEM: str | None = None


def note_key_problem(problem: str | None) -> None:
    global _KEY_PROBLEM
    _KEY_PROBLEM = problem


def current_key_problem() -> str | None:
    return _KEY_PROBLEM


def reset_protection_state() -> None:
    """Forget the member's key problem. Tests use this between cases."""
    note_key_problem(None)


def _utc_now() -> str:
    return datetime.now(tz=UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
