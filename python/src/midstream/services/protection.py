"""Data protection on the control plane: delivering a materialization's policy to its runtime, and
how far a policy has reached (design §5.8, handbook 22).

Every apply of a runtime - its creation, an edit, a resume, the sweep's re-apply, an upgrade - sends
the materialization's current policy as a `protection_policy` message on the runtime control topic,
as it sends the runtime settings. An edit of the policy is such an apply with an unchanged runtime
spec, so it restarts nothing: the runtime's settings agent stores the policy, its committer records
it in every table, and the writers follow the tables.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from midstream.config import ControlPlaneConfig
from midstream.models import Materialization
from midstream.observation.progress import MEMBER_STATE_FRESHNESS_SECONDS
from midstream.runtime_app.protection_state import PROTECTION_POLICY_KIND
from midstream.scheduler import build_direct_topic_output
from midstream.storage import get_materialization
from midstream.storage.table import TableProtocol


def protection_routing_key(materialization_id: str) -> str:
    """The key of a materialization's policy deliveries on the runtime control topic."""
    return f"protection-policy:{materialization_id}"


def build_protection_policy_message(
    materialization: Materialization,
    *,
    published_at: str,
    reason: str | None = None,
) -> dict[str, Any]:
    """The runtime control message carrying a materialization's policy and its revision.

    Addressed to the materialization's own runtime: every other runtime's settings agent drops it,
    and a runtime from before data protection drops the kind unread.
    """
    message: dict[str, Any] = {
        "kind": PROTECTION_POLICY_KIND,
        "materialization_id": materialization.materialization_id,
        "revision": materialization.protection_revision,
        "policy": materialization.protection.to_dict(),
        "changed_by": materialization.protection_changed_by or materialization.created_by,
        "published_at": published_at,
    }
    if reason is not None:
        message["reason"] = reason
    return message


def protection_policy_output_for_apply(
    table: TableProtocol | None,
    *,
    materialization_id: str,
    published_at: str,
    config: ControlPlaneConfig,
) -> dict[str, Any] | None:
    """The delivery that brings a runtime being (re)deployed onto its materialization's policy.

    None for a materialization created before data protection that no edit gave a policy: its
    runtime has nothing to learn, and its tables record none.
    """
    materialization = None if table is None else get_materialization(table, materialization_id)
    if materialization is None or materialization.protection_revision < 1:
        return None
    return build_direct_topic_output(
        topic=config.schema_authority_updates_topic,
        key=protection_routing_key(materialization_id),
        value=build_protection_policy_message(
            materialization,
            published_at=published_at,
            reason=materialization.protection_reason,
        ),
    )


def protection_state(
    materialization: Materialization,
    *,
    member_progress: dict[str, Any],
    recorded_revisions: dict[str, int],
    reference_time: str,
) -> dict[str, Any]:
    """How far the materialization's policy has reached: `current`, `applying`, `stopped`,
    `not_reporting` (the shape of the settings rollout, handbook 17 §17.7).

    `recorded_revisions` is each written table's recorded revision (its schema projection's);
    `member_progress` each member's last report, with the revisions it masks with.
    """
    revision = materialization.protection_revision
    tables_behind = sorted(name for name, recorded in recorded_revisions.items() if recorded < revision)
    if materialization.desired_phase == "stopped":
        state = "stopped"
        members_behind: list[str] = []
        reporting = 0
    else:
        fresh = {
            member_id: progress
            for member_id, progress in member_progress.items()
            if _is_fresh(getattr(progress, "reported_at", None), reference_time)
        }
        reporting = len(fresh)
        members_behind = sorted(
            member_id
            for member_id, progress in fresh.items()
            if any(
                (getattr(progress, "protection_revisions", {}) or {}).get(name, 0) < revision
                for name in recorded_revisions
            )
        )
        if revision < 1:
            state = "current"
        elif not fresh:
            state = "not_reporting"
        elif tables_behind or members_behind:
            state = "applying"
        else:
            state = "current"
    return {
        "state": state,
        "revision": revision,
        "tables_behind": tables_behind,
        "members_behind": members_behind,
        "members_reporting": reporting,
        "freshness_seconds": MEMBER_STATE_FRESHNESS_SECONDS,
    }


def _is_fresh(reported_at: str | None, reference_time: str) -> bool:
    reported = _parse(reported_at)
    reference = _parse(reference_time)
    if reported is None or reference is None:
        return False
    return (reference - reported).total_seconds() <= MEMBER_STATE_FRESHNESS_SECONDS


def _parse(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
