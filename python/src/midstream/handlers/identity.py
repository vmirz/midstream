"""Thin Kaspr-facing handler for identity requests, and the triggers the identity tasks send."""

from __future__ import annotations

from typing import Any

from midstream.config import ControlPlaneConfig
from midstream.models.identity import IdentityRequest
from midstream.services.identity import apply_identity_request
from midstream.services.table_claims import claim_outcome_work
from midstream.settings import resolve_config
from midstream.storage.identity import SYSTEM_ACTOR
from midstream.storage.table import TableProtocol


def handle_identity_request(
    value: dict,
    directory_table: TableProtocol,
    audit_table: TableProtocol,
    settings_table: TableProtocol | None,
    control_plane_table: TableProtocol | None = None,
    *,
    config: ControlPlaneConfig | None = None,
) -> dict[str, Any] | None:
    """Deserialize an identity request and apply it with the effective configuration.

    Returns the work event a claim of table names answers its materialization with, routed for the
    agent's output; None for every other request. `control_plane_table` is only read, by the hourly
    sweep of claims.
    """
    cfg = resolve_config(settings_table) if config is None else config
    request = IdentityRequest.from_dict(value)
    outcome = apply_identity_request(
        directory_table,
        audit_table,
        request,
        config=cfg,
        control_plane_table=control_plane_table,
    )
    return claim_outcome_work(request, outcome)


def build_identity_bootstrap_request(*, requested_at: str) -> dict:
    """The startup trigger: store the built-in groups and make sure an administrator exists."""
    return _system_request("bootstrap", requested_at)


def build_identity_maintenance_request(*, requested_at: str) -> dict:
    """The hourly trigger: remove expired sessions, stale sign-in counts and old audit entries."""
    return _system_request("maintenance", requested_at)


def _system_request(action: str, requested_at: str) -> dict:
    return IdentityRequest(
        request_id=f"idr-{action}-{requested_at}",
        action=action,
        submitted_at=requested_at,
        actor_id=SYSTEM_ACTOR,
        actor_email=SYSTEM_ACTOR,
    ).to_dict()
