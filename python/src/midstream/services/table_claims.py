"""Claiming and releasing the names of a materialization's tables in their datasets.

A table's SQL name, `<dataset>.<table>`, is unique across the deployment, and the directory's
agent, applying requests one at a time on one Kafka key, is the only one that decides it:

```text
create view / update view    preview the claim against the directory: refuse what it would refuse
request intake               saves the materialization; sends claim_tables (key: identity)
identity-intake              claims every name or none; answers the workflow (key: the materialization)
  ├─ tables_claimed          the workflow goes on: discovery and the tables, or the change of topics
  └─ claim_rejected          a create is deleted, keeping the reason; an update is not applied
request intake (delete, drop)  sends release_tables once the tables leave the catalog
```

Request intake sends both, for one materialization from one partition, so a release always follows
the claim it undoes. Records are written only in the shard of the event being handled: the
directory agent writes the directory, never the `state` table, and its answer goes back to the
materialization's own partition of the work topic.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from midstream.models import (
    EventCause,
    IdentityRequest,
    IdentityRequestOutcome,
    MaterializationWorkEvent,
    RequestContext,
    TableClaimResult,
    TableSpec,
)
from midstream.storage.identity import SYSTEM_ACTOR, identity_routing_key
from midstream.storage.keys import materialization_routing_key


def claim_request(
    *,
    request_id: str,
    materialization_id: str,
    purpose: str,
    materialization_request_id: str,
    tables: Iterable[TableSpec],
    release: Iterable[TableSpec] = (),
    actor_id: str | None,
    actor_email: str | None,
    submitted_at: str,
) -> IdentityRequest:
    """Ask the directory to take the names of a materialization's new tables, all or none.

    `release` names the tables the same change drops: their names are freed in the same step, so a
    refused change keeps them.
    """
    return IdentityRequest(
        request_id=request_id,
        action="claim_tables",
        submitted_at=submitted_at,
        actor_id=actor_id or SYSTEM_ACTOR,
        actor_email=actor_email or SYSTEM_ACTOR,
        payload={
            "materialization_id": materialization_id,
            "purpose": purpose,
            "materialization_request_id": materialization_request_id,
            "tables": [_claimed(spec) for spec in tables],
            "release": [_released(spec) for spec in release],
        },
        context=RequestContext(),
    )


def release_request(
    *,
    request_id: str,
    materialization_id: str,
    tables: Iterable[TableSpec] = (),
    everything: bool = False,
    reason: str,
    actor_id: str | None,
    actor_email: str | None,
    submitted_at: str,
) -> IdentityRequest:
    """Ask the directory to free the names of tables that left the catalog; `everything` for a
    materialization being deleted."""
    payload: dict[str, Any] = {"materialization_id": materialization_id, "reason": reason}
    if everything:
        payload["all"] = True
    else:
        payload["tables"] = [_released(spec) for spec in tables]
    return IdentityRequest(
        request_id=request_id,
        action="release_tables",
        submitted_at=submitted_at,
        actor_id=actor_id or SYSTEM_ACTOR,
        actor_email=actor_email or SYSTEM_ACTOR,
        payload=payload,
        context=RequestContext(),
    )


def identity_output(request: IdentityRequest) -> dict[str, Any]:
    """A request for the directory, as an agent's routed output: on the one key every identity
    request shares, so it applies in order."""
    return {"route": "identity", "key": identity_routing_key(), "value": request.to_dict()}


def work_output(event: MaterializationWorkEvent) -> dict[str, Any]:
    """A work event as an agent's routed output: keyed by its materialization."""
    return {"route": "work", "key": materialization_routing_key(event.materialization_id), "value": event.to_dict()}


def claim_outcome_work(request: IdentityRequest, outcome: IdentityRequestOutcome | None) -> dict[str, Any] | None:
    """The directory's answer to a claim, for the claiming materialization's workflow; None for any
    other request.

    Built from the stored outcome, so a redelivered claim answers again - the workflow takes the
    same answer twice as once.
    """
    if request.action != "claim_tables" or outcome is None:
        return None
    payload = request.payload
    materialization_id = str(payload.get("materialization_id") or "")
    if not materialization_id:
        return None
    purpose = str(payload.get("purpose") or "create")
    step = "tables_claimed" if outcome.status == "applied" else "claim_rejected"
    cause = str(payload.get("materialization_request_id") or request.request_id)
    event = MaterializationWorkEvent(
        work_id=f"work-claim-{request.request_id}",
        materialization_id=materialization_id,
        step=step,
        requested_at=outcome.completed_at,
        caused_by=EventCause(kind="request", id=cause),
        dedupe_key=f"{step}:{materialization_id}:{request.request_id}",
        claim=TableClaimResult(
            request_id=request.request_id,
            purpose=purpose if purpose in ("create", "update") else "create",
            status=outcome.status,
            reason=outcome.reason,
            message=outcome.message,
            details=dict(outcome.details),
        ),
    )
    return work_output(event)


def _claimed(spec: TableSpec) -> dict[str, str]:
    return {"dataset_id": spec.dataset_id, "dataset": spec.dataset, "name": spec.name, "source_topic": spec.source_topic}


def _released(spec: TableSpec) -> dict[str, str]:
    return {"dataset_id": spec.dataset_id, "dataset": spec.dataset, "name": spec.name}
