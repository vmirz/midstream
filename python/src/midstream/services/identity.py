"""Applying identity requests: the identity agent's only job.

The API validates a change and publishes it; this applies it. Every request arrives on one Kafka
key, so they apply strictly one at a time, and each is checked again against the directory as it
is now — another change may have landed since the API accepted it. These rules hold throughout:

- **One account per email**, without regard to case.
- **No one grants access they do not hold.** Adding someone to a group, giving a group a role,
  putting a permission in a role, granting a role on a dataset, or changing an account whose access
  exceeds yours is refused unless you hold everything it would hand out - on each dataset it
  concerns. An administrator holds everything.
- **There is always an active administrator.** A change that would leave none is refused.
- **One table per name in a dataset.** A materialization claims its tables' names before any table
  exists, all of them or none (`claim_tables`), and a name is released when its table leaves the
  catalog (`release_tables`). The hourly maintenance sweeps up claims whose table is gone.

A directory change leaves an outcome, applied or refused with a reason, that the client waits for;
sign-in events and the control plane's own housekeeping do not. A claim's outcome is also handed to
the workflow of its materialization (`handlers/identity.py`).
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
import logging
import time
from typing import Any
from uuid import uuid4

from midstream.config import ControlPlaneConfig, get_config
from midstream.identity.access_tokens import (
    MAX_TOKEN_LIFETIME_DAYS,
    MAX_TOKENS_PER_USER,
    TOKEN_NAME_MAX_LENGTH,
    TOKEN_PERMISSIONS,
)
from midstream.identity.directory import DirectoryView
from midstream.identity.passwords import hash_password
from midstream.identity.permissions import (
    ADMIN_GROUP_ID,
    ALL_USERS_GROUP_ID,
    BUILT_IN_GROUPS,
    DATASET_OWNER_ROLE_ID,
    DATASET_PERMISSION_KEYS,
    DATASET_READER_ROLE_ID,
    GUEST_GROUP_ID,
    built_in_group,
    built_in_role,
    dataset_grantable,
    expand_permissions,
    permission_definition,
)
from midstream.identity.principals import Grants
from midstream.identity.secrets import IdentitySecrets, get_identity_secrets
from midstream.identity.validation import (
    IdentityValidationError,
    clean_description,
    clean_email,
    clean_identifier,
    clean_ids,
    clean_label,
    clean_permissions,
    clean_person_name,
)
from midstream.models.identity import (
    GRANT_PRINCIPAL_TYPES,
    USER_STATUSES,
    AccessToken,
    AuditEntry,
    ClaimRelease,
    Dataset,
    DatasetGrant,
    Group,
    IdentityRequest,
    IdentityRequestOutcome,
    PasswordCredential,
    Role,
    Session,
    SignInGuard,
    TableClaim,
    User,
    UserActivity,
)
from midstream.protocol_logging import log_protocol_event
from midstream.storage.identity import (
    SYSTEM_ACTOR,
    append_audit_entry,
    default_group,
    delete_access_token,
    delete_audit_entries_before,
    delete_claim_release,
    delete_dataset_grant,
    delete_dataset_name,
    delete_dataset_record,
    delete_group_record,
    delete_request_outcome,
    delete_role_record,
    delete_session_record,
    delete_sign_in_guard,
    delete_table_claim,
    delete_user_activity,
    delete_user_email,
    delete_user_record,
    find_user_by_email,
    get_access_token,
    get_claim_release,
    get_directory_state,
    get_request_outcome,
    get_session,
    get_sign_in_guard,
    get_table_claim,
    get_user,
    get_user_activity,
    group_key,
    iter_access_tokens,
    iter_claim_releases,
    iter_request_outcomes,
    iter_sessions,
    iter_sign_in_guards,
    iter_table_claims,
    iter_user_access_tokens,
    iter_user_sessions,
    normalize_email,
    save_access_token,
    save_claim_release,
    save_dataset,
    save_dataset_grant,
    save_dataset_name,
    save_directory_state,
    save_group,
    save_request_outcome,
    save_role,
    save_session,
    save_sign_in_guard,
    save_table_claim,
    save_user,
    save_user_activity,
    save_user_email,
)
from midstream.storage.materializations import get_materialization, iter_materializations
from midstream.storage.table import TableProtocol
from midstream.storage.tables import list_materialization_tables
from midstream.table_names import dataset_name_problem, table_name_problem

LOGGER = logging.getLogger(__name__)

#: How long an outcome is kept for a client waiting on its request.
REQUEST_OUTCOME_RETENTION = timedelta(days=1)
#: How long a session's record is kept after it ended or expired, for the account pages.
SESSION_RECORD_RETENTION = timedelta(days=1)
#: How long failed sign-ins are remembered once their email is no longer locked out.
SIGN_IN_GUARD_RETENTION = timedelta(days=1)
#: How long a token's record is kept after it was revoked or expired, for the account page.
TOKEN_RECORD_RETENTION = timedelta(days=7)
#: Most audit entries one hourly cleanup removes; a backlog is caught up over several passes.
AUDIT_PURGE_BATCH_LIMIT = 5000
#: How long a deleted materialization's release is remembered, so a claim of it that arrives late
#: is refused.
CLAIM_RELEASE_RETENTION = timedelta(days=7)
#: How long the sweep leaves a claim, or a materialization, alone after it last changed: long enough
#: that every replica has read what changed, and any claim or release still on its way has landed.
CLAIM_SWEEP_GRACE = timedelta(hours=1)
#: The most datasets one access token may be limited to.
MAX_TOKEN_DATASETS = 100


class IdentityRejection(Exception):
    """A request the agent refuses: `reason` for code, `message` for people, `field` to point at,
    and `details` for a client to tell which part of a request it was about."""

    def __init__(self, reason: str, message: str, *, field: str | None = None, details: dict[str, Any] | None = None) -> None:
        self.reason = reason
        self.message = message
        self.field = field
        self.details = dict(details or {})
        super().__init__(message)


@dataclass(frozen=True, slots=True)
class _Applied:
    entity_type: str | None = None
    entity_id: str | None = None
    #: Whether people, groups or roles changed, which raises the directory revision.
    directory_changed: bool = True


class _Work:
    """One request being applied: its tables, its clock, and the directory as it stands."""

    def __init__(
        self,
        directory_table: TableProtocol,
        audit_table: TableProtocol | None,
        request: IdentityRequest,
        *,
        config: ControlPlaneConfig,
        secrets: IdentitySecrets,
        now: str,
        control_plane_table: TableProtocol | None = None,
    ) -> None:
        self.table = directory_table
        self.audit_table = audit_table
        self.request = request
        self.payload = request.payload
        self.config = config
        self.secrets = secrets
        self.now = now
        #: The materializations' records, read (never written) by the sweep of table-name claims.
        self.control_plane_table = control_plane_table
        self._view: DirectoryView | None = None

    @property
    def view(self) -> DirectoryView:
        if self._view is None:
            self._view = DirectoryView.load(self.table)
        return self._view

    @property
    def now_datetime(self) -> datetime:
        return _parse_time(self.now) or datetime.now(tz=UTC)

    def audit(
        self,
        action: str,
        *,
        outcome: str = "success",
        target: User | Group | Role | Dataset | None = None,
        target_type: str | None = None,
        target_id: str | None = None,
        target_label: str | None = None,
        details: dict[str, Any] | None = None,
        actor_id: str | None = None,
        actor_email: str | None = None,
    ) -> None:
        if self.audit_table is None:
            return
        if target is not None:
            target_type, target_id, target_label = _describe_target(target)
        append_audit_entry(
            self.audit_table,
            AuditEntry(
                entry_id=new_audit_entry_id(),
                occurred_at=self.now,
                action=action,
                outcome=outcome,
                actor_id=self.request.actor_id if actor_id is None else actor_id,
                actor_email=self.request.actor_email if actor_email is None else actor_email,
                target_type=target_type,
                target_id=target_id,
                target_label=target_label,
                details=details or {},
                ip=self.request.context.ip,
                user_agent=self.request.context.user_agent,
            ),
        )


def apply_identity_request(
    directory_table: TableProtocol,
    audit_table: TableProtocol | None,
    request: IdentityRequest,
    *,
    config: ControlPlaneConfig | None = None,
    secrets: IdentitySecrets | None = None,
    applied_at: str | None = None,
    control_plane_table: TableProtocol | None = None,
) -> IdentityRequestOutcome | None:
    """Apply one request; return its outcome when a client may be waiting on it.

    A directory request already applied or refused returns its earlier outcome, so a redelivered
    request changes nothing. `control_plane_table` is read by the hourly maintenance only, to
    sweep up table-name claims whose tables are gone; nothing here writes it.
    """
    cfg = get_config() if config is None else config
    now = applied_at or _utc_now()
    if request.tracked:
        previous = get_request_outcome(directory_table, request.request_id)
        if previous is not None:
            return previous
    work = _Work(
        directory_table,
        audit_table,
        request,
        config=cfg,
        secrets=get_identity_secrets() if secrets is None else secrets,
        now=now,
        control_plane_table=control_plane_table,
    )
    try:
        applied = _HANDLERS[request.action](work)
    except IdentityValidationError as error:
        return _refuse(work, IdentityRejection("invalid", str(error), field=error.field))
    except IdentityRejection as rejection:
        return _refuse(work, rejection)

    if applied.directory_changed:
        state = get_directory_state(directory_table)
        save_directory_state(directory_table, replace(state, revision=state.revision + 1, updated_at=now))
    log_protocol_event(
        LOGGER,
        enabled=cfg.protocol_logging_enabled,
        event="identity_request_applied",
        request_id=request.request_id,
        action=request.action,
        actor_id=request.actor_id,
        entity_id=applied.entity_id,
    )
    if not request.tracked:
        return None
    outcome = IdentityRequestOutcome(
        request_id=request.request_id,
        action=request.action,
        status="applied",
        completed_at=now,
        actor_id=request.actor_id,
        entity_type=applied.entity_type,
        entity_id=applied.entity_id,
    )
    save_request_outcome(directory_table, outcome)
    return outcome


def preview_identity_request(
    directory_table: TableProtocol,
    request: IdentityRequest,
    *,
    config: ControlPlaneConfig | None = None,
    secrets: IdentitySecrets | None = None,
) -> None:
    """Raise `IdentityRejection` if the agent would refuse this directory request now.

    Runs the agent's own handler against an in-memory overlay of the table, so the API refuses a
    request for exactly the reasons the agent would, and nothing is written. The agent still
    checks again when it applies the request: something else may change in between.
    """
    work = _Work(
        _Overlay(directory_table),
        None,
        request,
        config=get_config() if config is None else config,
        secrets=get_identity_secrets() if secrets is None else secrets,
        now=request.submitted_at,
    )
    try:
        _HANDLERS[request.action](work)
    except IdentityValidationError as error:
        raise IdentityRejection("invalid", str(error), field=error.field) from error


class _Overlay:
    """A table whose writes stay in memory, over one that is only read."""

    def __init__(self, base: TableProtocol) -> None:
        self._base = base
        self._writes: dict[str, dict[str, Any]] = {}
        self._deleted: set[str] = set()

    def get(self, key: str) -> dict[str, Any] | None:
        if key in self._deleted:
            return None
        if key in self._writes:
            return self._writes[key]
        return self._base.get(key)

    def __setitem__(self, key: str, value: dict[str, Any]) -> None:
        self._writes[key] = value
        self._deleted.discard(key)

    def __delitem__(self, key: str) -> None:
        self._writes.pop(key, None)
        self._deleted.add(key)

    def prefix_scan(self, prefix: str):
        for key, value in list(self._writes.items()):
            if key.startswith(prefix):
                yield key, value
        for key, value in self._base.prefix_scan(prefix):
            if key not in self._writes and key not in self._deleted:
                yield key, value


def _refuse(work: _Work, rejection: IdentityRejection) -> IdentityRequestOutcome | None:
    request = work.request
    LOGGER.info(
        "Refused identity request %s (%s): %s",
        request.request_id,
        request.action,
        rejection.message,
    )
    if not request.tracked:
        return None
    outcome = IdentityRequestOutcome(
        request_id=request.request_id,
        action=request.action,
        status="rejected",
        completed_at=work.now,
        actor_id=request.actor_id,
        reason=rejection.reason,
        message=rejection.message,
        field=rejection.field,
        details=rejection.details,
    )
    save_request_outcome(work.table, outcome)
    return outcome


# ---------------------------------------------------------------------------------------- checks


def _actor(work: _Work, permission: str) -> tuple[User, Grants]:
    """The person who asked, still active and still holding the permission the change needs."""
    actor = work.view.users.get(work.request.actor_id)
    if actor is None or not actor.active:
        raise IdentityRejection("forbidden", "Your account can no longer make changes.")
    grants = work.view.grants_for_user(actor)
    if not grants.all_permissions and permission not in grants.permissions:
        definition = permission_definition(permission)
        label = definition.label if definition is not None else permission
        raise IdentityRejection("forbidden", f"You no longer have the “{label}” permission.")
    return actor, grants


def _require_grantable(actor_grants: Grants, granted: Grants, *, doing: str, view: DirectoryView | None = None) -> None:
    """Refuse handing out (or taking away) access the actor does not hold, on any dataset too."""
    if actor_grants.covers(granted):
        return
    if granted.all_permissions and not actor_grants.all_permissions:
        missing = "every permission (Administrator)"
    else:
        deployment, datasets = actor_grants.missing(granted)
        parts = [_permission_label(key) for key in sorted(deployment)]
        for dataset_id, permissions in sorted(datasets.items()):
            dataset = None if view is None else view.datasets.get(dataset_id)
            where = dataset.name if dataset is not None else dataset_id
            parts.extend(f"{_permission_label(key)} on dataset {where}" for key in sorted(permissions))
        missing = ", ".join(parts)
    raise IdentityRejection(
        "forbidden",
        f"You can only {doing} with access you hold yourself. Missing: {missing}.",
    )


def _require_manageable(view: DirectoryView, actor_grants: Grants, target: User) -> None:
    """Refuse changing an account that holds access the actor does not."""
    if actor_grants.covers(view.grants_for_user(target)):
        return
    raise IdentityRejection(
        "forbidden",
        f"{target.display_name} has access you do not hold, so you cannot change their account.",
    )


def _require_an_administrator(before: DirectoryView, after: DirectoryView) -> None:
    if before.administrators() and not after.administrators():
        raise IdentityRejection(
            "last_administrator",
            "This would leave no active administrator. Make someone else an administrator first.",
        )


def _require_revision(work: _Work, current: int, what: str) -> None:
    base = work.request.base_revision
    if base is not None and base != current:
        raise IdentityRejection("changed", f"{what} changed after you opened it. Reload it and try again.")


def _require_user(work: _Work, field: str = "user_id") -> User:
    user_id = clean_identifier(work.payload.get(field), field=field)
    user = work.view.users.get(user_id)
    if user is None:
        raise IdentityRejection("not_found", "That account no longer exists.", field=field)
    return user


def _require_group(work: _Work) -> Group:
    group_id = clean_identifier(work.payload.get("group_id"), field="group_id")
    group = work.view.groups.get(group_id)
    if group is None:
        raise IdentityRejection("not_found", "That group no longer exists.", field="group_id")
    return group


def _require_role(work: _Work) -> Role:
    role_id = clean_identifier(work.payload.get("role_id"), field="role_id")
    role = work.view.roles.get(role_id)
    if role is None:
        raise IdentityRejection("not_found", "That role no longer exists.", field="role_id")
    return role


def _require_assignable_groups(view: DirectoryView, group_ids: list[str], *, field: str) -> None:
    for group_id in group_ids:
        group = view.groups.get(group_id)
        if group is None:
            raise IdentityRejection("not_found", "A chosen group no longer exists.", field=field)
        if group.membership != "assigned":
            raise IdentityRejection(
                "invalid",
                f"Nobody is added to {group.name}: "
                + ("everyone who signs in is in it." if group.membership == "authenticated" else "it is for people not signed in."),
                field=field,
            )


def _require_known_roles(view: DirectoryView, role_ids: list[str], *, field: str = "role_ids") -> None:
    for role_id in role_ids:
        if role_id not in view.roles:
            raise IdentityRejection("not_found", "A chosen role no longer exists.", field=field)


def _require_password_hash(work: _Work) -> str:
    value = work.payload.get("password_hash")
    if not isinstance(value, str) or not value.startswith("$scrypt$"):
        raise IdentityRejection("invalid", "The request carries no password.", field="password")
    return value


def _actor_label(work: _Work) -> str:
    return work.request.actor_email or work.request.actor_id


# ------------------------------------------------------------------------------------------ users


def _create_user(work: _Work) -> _Applied:
    _, grants = _actor(work, "users.manage")
    payload = work.payload
    user_id = clean_identifier(payload.get("user_id"), field="user_id")
    email = clean_email(payload.get("email"))
    first_name = clean_person_name(payload.get("first_name"), field="first_name", label="First name")
    last_name = clean_person_name(payload.get("last_name"), field="last_name", label="Last name")
    group_ids = clean_ids(payload.get("group_ids"), field="group_ids")
    password_hash = _require_password_hash(work)
    view = work.view
    if user_id in view.users:
        raise IdentityRejection("already_exists", "That account already exists.", field="user_id")
    if view.user_by_email(email) is not None:
        raise IdentityRejection("already_exists", f"{email} already has an account.", field="email")
    _require_assignable_groups(view, group_ids, field="group_ids")
    _require_grantable(grants, view.grants_for_groups(group_ids), doing="add people to groups", view=view)

    actor = _actor_label(work)
    user = User(
        user_id=user_id,
        email=email,
        first_name=first_name,
        last_name=last_name,
        group_ids=group_ids,
        password=PasswordCredential(hash=password_hash, set_at=work.now, set_by=actor, must_change=True),
        created_at=work.now,
        created_by=actor,
        updated_at=work.now,
        updated_by=actor,
    )
    save_user(work.table, user)
    save_user_email(work.table, email, user_id, updated_at=work.now)
    work.audit(
        "user.created",
        target=user,
        details={"email": email, "name": user.display_name, "groups": _group_names(view, group_ids)},
    )
    return _Applied("user", user_id)


def _update_user(work: _Work) -> _Applied:
    actor, grants = _actor(work, "users.manage")
    view = work.view
    target = _require_user(work)
    _require_revision(work, target.revision, f"{target.display_name}'s account")
    _require_manageable(view, grants, target)
    payload = work.payload
    changes: dict[str, Any] = {}
    updated = target

    for field, label in (("first_name", "First name"), ("last_name", "Last name")):
        if field in payload:
            value = clean_person_name(payload[field], field=field, label=label)
            if value != getattr(target, field):
                changes[field] = {"from": getattr(target, field), "to": value}
                updated = replace(updated, **{field: value})

    if "email" in payload:
        email = clean_email(payload["email"])
        if email != target.email:
            holder = view.user_by_email(email)
            if holder is not None and holder.user_id != target.user_id:
                raise IdentityRejection("already_exists", f"{email} already has an account.", field="email")
            changes["email"] = {"from": target.email, "to": email}
            updated = replace(updated, email=email)

    if "group_ids" in payload:
        group_ids = clean_ids(payload["group_ids"], field="group_ids")
        added = [group_id for group_id in group_ids if group_id not in target.group_ids]
        removed = [group_id for group_id in target.group_ids if group_id not in group_ids]
        if added or removed:
            _require_assignable_groups(view, added, field="group_ids")
            _require_grantable(grants, view.grants_for_groups(added + removed), doing="change group membership", view=view)
            changes["groups"] = {
                "added": _group_names(view, added),
                "removed": _group_names(view, removed),
            }
            updated = replace(updated, group_ids=group_ids)

    status_change: str | None = None
    if "status" in payload:
        status = payload["status"]
        if status not in USER_STATUSES:
            raise IdentityRejection("invalid", "Status must be active or deactivated.", field="status")
        if status != target.status:
            if status == "deactivated" and target.user_id == actor.user_id:
                raise IdentityRejection("self_action", "You cannot deactivate your own account.", field="status")
            status_change = status
            if status == "deactivated":
                # Ending every session: the epoch every one of them was issued under no longer holds.
                updated = replace(
                    updated,
                    status=status,
                    deactivated_at=work.now,
                    deactivated_by=_actor_label(work),
                    session_epoch=updated.session_epoch + 1,
                )
            else:
                updated = replace(updated, status=status, deactivated_at=None, deactivated_by=None)

    if updated == target:
        return _Applied("user", target.user_id, directory_changed=False)

    _require_an_administrator(view, view.with_user(updated))
    updated = replace(updated, revision=target.revision + 1, updated_at=work.now, updated_by=_actor_label(work))
    save_user(work.table, updated)
    if updated.email != target.email:
        delete_user_email(work.table, target.email)
        save_user_email(work.table, updated.email, updated.user_id, updated_at=work.now)
        # Failed sign-ins were counted against the old address; nothing signs in with it now.
        delete_sign_in_guard(work.table, target.email)
    if status_change == "deactivated":
        _end_sessions(work, updated.user_id, reason="deactivated")
        work.audit("user.deactivated", target=updated)
    elif status_change == "active":
        work.audit("user.reactivated", target=updated)
    if changes:
        work.audit("user.updated", target=updated, details=changes)
    return _Applied("user", updated.user_id)


def _set_user_password(work: _Work) -> _Applied:
    actor, grants = _actor(work, "users.manage")
    target = _require_user(work)
    if target.user_id == actor.user_id:
        raise IdentityRejection(
            "self_action",
            "Use Change password on your account page to change your own password.",
            field="password",
        )
    _require_manageable(work.view, grants, target)
    password_hash = _require_password_hash(work)
    updated = replace(
        target,
        password=PasswordCredential(
            hash=password_hash,
            set_at=work.now,
            set_by=_actor_label(work),
            must_change=True,
        ),
        session_epoch=target.session_epoch + 1,
        revision=target.revision + 1,
        updated_at=work.now,
        updated_by=_actor_label(work),
    )
    save_user(work.table, updated)
    delete_sign_in_guard(work.table, target.email)
    _end_sessions(work, target.user_id, reason="password_reset")
    work.audit("user.password_reset", target=updated)
    return _Applied("user", target.user_id)


def _change_password(work: _Work) -> _Applied:
    target = _require_user(work)
    if target.user_id != work.request.actor_id:
        raise IdentityRejection("forbidden", "You can only change your own password here.")
    if not target.active:
        raise IdentityRejection("forbidden", "This account is deactivated.")
    password_hash = _require_password_hash(work)
    updated = replace(
        target,
        password=PasswordCredential(hash=password_hash, set_at=work.now, set_by=target.email, must_change=False),
        session_epoch=target.session_epoch + 1,
        revision=target.revision + 1,
        updated_at=work.now,
        updated_by=target.email,
    )
    save_user(work.table, updated)
    _end_sessions(work, target.user_id, reason="password_changed")
    work.audit("user.password_changed", target=updated)
    return _Applied("user", target.user_id)


def _sign_out_user(work: _Work) -> _Applied:
    target = _require_user(work)
    if work.request.actor_id != target.user_id:
        _, grants = _actor(work, "users.manage")
        _require_manageable(work.view, grants, target)
    session_id = work.payload.get("session_id")
    if session_id:
        session_id = clean_identifier(session_id, field="session_id")
        ended = _end_session(work, target.user_id, session_id, reason="signed_out_by_administrator")
        if ended:
            work.audit("user.session_ended", target=target, details={"session_id": session_id})
        return _Applied("user", target.user_id, directory_changed=False)
    updated = replace(
        target,
        session_epoch=target.session_epoch + 1,
        revision=target.revision + 1,
        updated_at=work.now,
        updated_by=_actor_label(work),
    )
    save_user(work.table, updated)
    _end_sessions(work, target.user_id, reason="signed_out_everywhere")
    work.audit("user.signed_out", target=updated)
    return _Applied("user", target.user_id)


def _unlock_user(work: _Work) -> _Applied:
    _actor(work, "users.manage")
    target = _require_user(work)
    if get_sign_in_guard(work.table, target.email) is not None:
        delete_sign_in_guard(work.table, target.email)
        work.audit("user.unlocked", target=target)
    return _Applied("user", target.user_id, directory_changed=False)


def _delete_user(work: _Work) -> _Applied:
    actor, grants = _actor(work, "users.manage")
    view = work.view
    target = _require_user(work)
    if target.user_id == actor.user_id:
        raise IdentityRejection("self_action", "You cannot delete your own account.")
    if target.active:
        raise IdentityRejection("invalid", f"Deactivate {target.display_name} before deleting their account.")
    _require_manageable(view, grants, target)
    _require_an_administrator(view, view.without_user(target.user_id))
    for session in iter_user_sessions(work.table, target.user_id):
        delete_session_record(work.table, target.user_id, session.session_id)
    for token in iter_user_access_tokens(work.table, target.user_id):
        delete_access_token(work.table, token.token_id)
    for grant in [grant for grant in view.dataset_grants.values() if (grant.principal_type, grant.principal_id) == ("user", target.user_id)]:
        delete_dataset_grant(work.table, grant.dataset_id, grant.principal_type, grant.principal_id)
    delete_user_record(work.table, target.user_id)
    delete_user_email(work.table, target.email)
    delete_user_activity(work.table, target.user_id)
    delete_sign_in_guard(work.table, target.email)
    work.audit("user.deleted", target=target, details={"email": target.email, "name": target.display_name})
    return _Applied("user", target.user_id)


# ----------------------------------------------------------------------------------------- groups


def _create_group(work: _Work) -> _Applied:
    _, grants = _actor(work, "groups.manage")
    view = work.view
    payload = work.payload
    group_id = clean_identifier(payload.get("group_id"), field="group_id")
    name = clean_label(payload.get("name"))
    description = clean_description(payload.get("description"))
    role_ids = clean_ids(payload.get("role_ids"), field="role_ids")
    member_ids = clean_ids(payload.get("member_ids"), field="member_ids")
    if group_id in view.groups:
        raise IdentityRejection("already_exists", "That group already exists.", field="group_id")
    if view.group_by_name(name) is not None:
        raise IdentityRejection("already_exists", f"A group named {name} already exists.", field="name")
    _require_known_roles(view, role_ids)
    members = _require_users(view, member_ids, field="member_ids")
    _require_grantable(grants, view.grants_for_roles(role_ids), doing="create a group", view=view)

    actor = _actor_label(work)
    group = Group(
        group_id=group_id,
        name=name,
        description=description,
        role_ids=role_ids,
        created_at=work.now,
        created_by=actor,
        updated_at=work.now,
        updated_by=actor,
    )
    save_group(work.table, group)
    for member in members:
        _save_membership(work, member, [*member.group_ids, group_id])
    work.audit(
        "group.created",
        target=group,
        details={"roles": _role_names(view, role_ids), "members": [member.email for member in members]},
    )
    return _Applied("group", group_id)


def _update_group(work: _Work) -> _Applied:
    _, grants = _actor(work, "groups.manage")
    view = work.view
    group = _require_group(work)
    _require_revision(work, group.revision, f"The {group.name} group")
    definition = built_in_group(group.group_id)
    payload = work.payload
    updated = group
    details: dict[str, Any] = {}

    if "name" in payload:
        name = clean_label(payload["name"])
        if name != group.name:
            if definition is not None:
                raise IdentityRejection("built_in", "Built-in groups keep their names.", field="name")
            holder = view.group_by_name(name)
            if holder is not None and holder.group_id != group.group_id:
                raise IdentityRejection("already_exists", f"A group named {name} already exists.", field="name")
            details["name"] = {"from": group.name, "to": name}
            updated = replace(updated, name=name)
    if "description" in payload:
        description = clean_description(payload["description"])
        if description != group.description:
            if definition is not None:
                raise IdentityRejection("built_in", "Built-in groups keep their descriptions.", field="description")
            details["description"] = {"from": group.description, "to": description}
            updated = replace(updated, description=description)

    before_grants = view.grants_for_groups([group.group_id])
    if "role_ids" in payload:
        role_ids = clean_ids(payload["role_ids"], field="role_ids")
        added = [role_id for role_id in role_ids if role_id not in group.role_ids]
        removed = [role_id for role_id in group.role_ids if role_id not in role_ids]
        _require_known_roles(view, added)
        locked = set(definition.locked_role_ids) if definition is not None else set()
        if locked & set(removed):
            raise IdentityRejection(
                "locked_role",
                f"The {group.name} group always holds the {_role_names(view, sorted(locked))[0]} role.",
                field="role_ids",
            )
        if added or removed:
            _require_grantable(grants, view.grants_for_roles(added + removed), doing="change a group's roles", view=view)
            details["roles_added"] = _role_names(view, added)
            details["roles_removed"] = _role_names(view, removed)
            updated = replace(updated, role_ids=role_ids)

    add_ids = clean_ids(payload.get("add_member_ids"), field="add_member_ids")
    remove_ids = clean_ids(payload.get("remove_member_ids"), field="remove_member_ids")
    added_members = [user for user in _require_users(view, add_ids, field="add_member_ids") if group.group_id not in user.group_ids]
    removed_members = [user for user in _require_users(view, remove_ids, field="remove_member_ids") if group.group_id in user.group_ids]
    if (added_members or removed_members) and group.membership != "assigned":
        raise IdentityRejection(
            "invalid",
            f"Nobody is added to or removed from {group.name}; its membership is automatic.",
            field="add_member_ids" if added_members else "remove_member_ids",
        )

    after_view = view.with_group(updated)
    if added_members:
        _require_grantable(grants, after_view.grants_for_groups([group.group_id]), doing="add people to a group", view=view)
        details["members_added"] = [user.email for user in added_members]
    if removed_members:
        _require_grantable(grants, before_grants, doing="remove people from a group", view=view)
        details["members_removed"] = [user.email for user in removed_members]
    changed_users = [replace(user, group_ids=[*user.group_ids, group.group_id]) for user in added_members]
    changed_users += [
        replace(user, group_ids=[group_id for group_id in user.group_ids if group_id != group.group_id])
        for user in removed_members
    ]
    for user in changed_users:
        after_view = after_view.with_user(user)

    if updated == group and not changed_users:
        return _Applied("group", group.group_id, directory_changed=False)
    _require_an_administrator(view, after_view)
    if updated != group:
        # Membership lives on each person, so only the group's own fields advance its revision.
        save_group(
            work.table,
            replace(updated, revision=group.revision + 1, updated_at=work.now, updated_by=_actor_label(work)),
        )
    for user in changed_users:
        _save_membership(work, view.users[user.user_id], user.group_ids)
    work.audit("group.updated", target=updated, details=details)
    return _Applied("group", group.group_id)


def _delete_group(work: _Work) -> _Applied:
    _, grants = _actor(work, "groups.manage")
    view = work.view
    group = _require_group(work)
    if group.built_in or built_in_group(group.group_id) is not None:
        raise IdentityRejection("built_in", "Built-in groups cannot be deleted.")
    _require_grantable(grants, view.grants_for_groups([group.group_id]), doing="delete a group", view=view)
    members = view.members(group.group_id)
    after_view = view.without_group(group.group_id)
    for member in members:
        after_view = after_view.with_user(
            replace(member, group_ids=[group_id for group_id in member.group_ids if group_id != group.group_id])
        )
    _require_an_administrator(view, after_view)
    for member in members:
        _save_membership(work, member, [group_id for group_id in member.group_ids if group_id != group.group_id])
    held = [grant for grant in view.dataset_grants.values() if (grant.principal_type, grant.principal_id) == ("group", group.group_id)]
    for grant in held:
        delete_dataset_grant(work.table, grant.dataset_id, grant.principal_type, grant.principal_id)
    delete_group_record(work.table, group.group_id)
    work.audit(
        "group.deleted",
        target=group,
        details={
            "members": [member.email for member in members],
            "datasets": sorted(_dataset_label(view, grant.dataset_id) for grant in held),
        },
    )
    return _Applied("group", group.group_id)


# ------------------------------------------------------------------------------------------ roles


def _create_role(work: _Work) -> _Applied:
    _, grants = _actor(work, "roles.manage")
    view = work.view
    payload = work.payload
    role_id = clean_identifier(payload.get("role_id"), field="role_id")
    name = clean_label(payload.get("name"))
    description = clean_description(payload.get("description"))
    permissions = clean_permissions(payload.get("permissions") or [])
    if role_id in view.roles or built_in_role(role_id) is not None:
        raise IdentityRejection("already_exists", "That role already exists.", field="role_id")
    if view.role_by_name(name) is not None:
        raise IdentityRejection("already_exists", f"A role named {name} already exists.", field="name")
    _require_grantable(grants, _grants_of(permissions), doing="create a role")
    actor = _actor_label(work)
    role = Role(
        role_id=role_id,
        name=name,
        description=description,
        permissions=permissions,
        created_at=work.now,
        created_by=actor,
        updated_at=work.now,
        updated_by=actor,
    )
    save_role(work.table, role)
    work.audit("role.created", target=role, details={"permissions": permissions})
    return _Applied("role", role_id)


def _update_role(work: _Work) -> _Applied:
    _, grants = _actor(work, "roles.manage")
    view = work.view
    role = _require_role(work)
    if role.built_in:
        raise IdentityRejection("built_in", "Built-in roles cannot be changed. Duplicate one to make your own.")
    _require_revision(work, role.revision, f"The {role.name} role")
    payload = work.payload
    updated = role
    details: dict[str, Any] = {}
    if "name" in payload:
        name = clean_label(payload["name"])
        if name != role.name:
            holder = view.role_by_name(name)
            if holder is not None and holder.role_id != role.role_id:
                raise IdentityRejection("already_exists", f"A role named {name} already exists.", field="name")
            details["name"] = {"from": role.name, "to": name}
            updated = replace(updated, name=name)
    if "description" in payload:
        description = clean_description(payload["description"])
        if description != role.description:
            details["description"] = {"from": role.description, "to": description}
            updated = replace(updated, description=description)
    if "permissions" in payload:
        permissions = clean_permissions(payload["permissions"])
        if permissions != sorted(role.permissions):
            _require_grantable(grants, _grants_of([*role.permissions, *permissions]), doing="change a role")
            details["permissions_added"] = sorted(set(permissions) - set(role.permissions))
            details["permissions_removed"] = sorted(set(role.permissions) - set(permissions))
            updated = replace(updated, permissions=permissions)
    if updated == role:
        return _Applied("role", role.role_id, directory_changed=False)
    _require_an_administrator(view, view.with_role(updated))
    updated = replace(updated, revision=role.revision + 1, updated_at=work.now, updated_by=_actor_label(work))
    save_role(work.table, updated)
    work.audit("role.updated", target=updated, details=details)
    return _Applied("role", role.role_id)


def _delete_role(work: _Work) -> _Applied:
    _, grants = _actor(work, "roles.manage")
    view = work.view
    role = _require_role(work)
    if role.built_in:
        raise IdentityRejection("built_in", "Built-in roles cannot be deleted.")
    _require_grantable(grants, _grants_of(role.permissions), doing="delete a role")
    holders = [group for group in view.groups.values() if role.role_id in group.role_ids]
    after_view = view.without_role(role.role_id)
    changed_groups = [
        replace(group, role_ids=[role_id for role_id in group.role_ids if role_id != role.role_id])
        for group in holders
    ]
    for group in changed_groups:
        after_view = after_view.with_group(group)
    _require_an_administrator(view, after_view)
    for group in changed_groups:
        save_group(
            work.table,
            replace(group, revision=group.revision + 1, updated_at=work.now, updated_by=_actor_label(work)),
        )
    for grant in [grant for grant in view.dataset_grants.values() if role.role_id in grant.role_ids]:
        remaining = [role_id for role_id in grant.role_ids if role_id != role.role_id]
        if remaining:
            save_dataset_grant(work.table, replace(grant, role_ids=remaining, updated_at=work.now, updated_by=_actor_label(work)))
        else:
            delete_dataset_grant(work.table, grant.dataset_id, grant.principal_type, grant.principal_id)
    delete_role_record(work.table, role.role_id)
    work.audit("role.deleted", target=role, details={"groups": [group.name for group in holders]})
    return _Applied("role", role.role_id)


# --------------------------------------------------------------------------------------- sessions


def _sign_in_succeeded(work: _Work) -> _Applied:
    payload = work.payload
    user = get_user(work.table, str(payload.get("user_id", "")))
    session_id = payload.get("session_id")
    if user is None or not isinstance(session_id, str) or not session_id:
        return _Applied(directory_changed=False)
    created_at = payload.get("created_at") or work.now
    save_session(
        work.table,
        Session(
            session_id=session_id,
            user_id=user.user_id,
            method=str(payload.get("method") or "password"),
            created_at=created_at,
            expires_at=str(payload.get("expires_at") or work.now),
            last_seen_at=created_at,
            ip=work.request.context.ip,
            user_agent=work.request.context.user_agent,
        ),
        updated_at=work.now,
    )
    activity = get_user_activity(work.table, user.user_id) or UserActivity(user_id=user.user_id)
    save_user_activity(
        work.table,
        replace(
            activity,
            last_sign_in_at=work.now,
            last_active_at=work.now,
            sign_in_count=activity.sign_in_count + 1,
        ),
        updated_at=work.now,
    )
    delete_sign_in_guard(work.table, user.email)
    work.audit(
        "session.signed_in",
        target=user,
        actor_id=user.user_id,
        actor_email=user.email,
        details={"method": payload.get("method") or "password"},
    )
    return _Applied("session", session_id, directory_changed=False)


def _sign_in_failed(work: _Work) -> _Applied:
    email = normalize_email(str(work.payload.get("email") or ""))
    if not email:
        return _Applied(directory_changed=False)
    now = work.now_datetime
    guard = get_sign_in_guard(work.table, email) or SignInGuard(email=email)
    locked_until = _parse_time(guard.locked_until)
    if locked_until is not None and locked_until <= now:
        # The lockout ran out; counting starts over.
        guard = SignInGuard(email=email)
    attempts = guard.failed_attempts + 1
    locked = attempts >= work.config.sign_in_max_failed_attempts
    guard = SignInGuard(
        email=email,
        failed_attempts=attempts,
        first_failed_at=guard.first_failed_at or work.now,
        last_failed_at=work.now,
        locked_until=_format_time(now + timedelta(minutes=work.config.sign_in_lockout_minutes)) if locked else None,
    )
    save_sign_in_guard(work.table, guard, updated_at=work.now)
    user = find_user_by_email(work.table, email)
    target: dict[str, Any] = {
        "target_type": "user" if user is not None else "email",
        "target_id": user.user_id if user is not None else None,
        "target_label": _user_label(user) if user is not None else email,
    }
    work.audit(
        "session.sign_in_failed",
        outcome="failure",
        details={"reason": work.payload.get("reason") or "invalid_credentials", "attempts": attempts},
        **target,
    )
    if locked:
        work.audit(
            "session.locked_out",
            outcome="failure",
            details={"until": guard.locked_until, "attempts": attempts},
            **target,
        )
    return _Applied(directory_changed=False)


def _session_refreshed(work: _Work) -> _Applied:
    user_id = str(work.payload.get("user_id") or "")
    session_id = str(work.payload.get("session_id") or "")
    session = get_session(work.table, user_id, session_id)
    if session is not None and not session.ended:
        save_session(work.table, replace(session, last_seen_at=work.now), updated_at=work.now)
    activity = get_user_activity(work.table, user_id)
    if activity is not None or get_user(work.table, user_id) is not None:
        activity = activity or UserActivity(user_id=user_id)
        save_user_activity(work.table, replace(activity, last_active_at=work.now), updated_at=work.now)
    return _Applied(directory_changed=False)


def _sign_out(work: _Work) -> _Applied:
    user_id = work.request.actor_id
    payload = work.payload
    current = str(payload.get("session_id") or "")
    scope = payload.get("scope") or "current"
    ended = 0
    if scope == "current" and current:
        ended += _end_session(work, user_id, current, reason="signed_out", expires_at=payload.get("session_expires_at"))
    elif scope == "session":
        target = str(payload.get("target_session_id") or "")
        if target:
            ended += _end_session(work, user_id, target, reason="signed_out_elsewhere")
    elif scope == "others":
        ended += _end_sessions(work, user_id, reason="signed_out_elsewhere", keep=current)
    user = get_user(work.table, user_id)
    work.audit(
        "session.signed_out",
        target_type="user",
        target_id=user_id,
        target_label=_user_label(user) if user is not None else work.request.actor_email,
        details={"scope": scope, "sessions": ended},
    )
    return _Applied(directory_changed=False)


def _end_session(
    work: _Work,
    user_id: str,
    session_id: str,
    *,
    reason: str,
    expires_at: Any = None,
) -> int:
    """End one session; returns 1 when it was still going."""
    session = get_session(work.table, user_id, session_id)
    if session is None:
        if not isinstance(expires_at, int):
            return 0
        # Signed out before the sign-in was recorded: record it ended, so its token is refused.
        session = Session(
            session_id=session_id,
            user_id=user_id,
            created_at=work.now,
            expires_at=_format_time(datetime.fromtimestamp(expires_at, tz=UTC)),
            last_seen_at=work.now,
        )
    elif session.ended:
        return 0
    save_session(
        work.table,
        replace(session, ended_at=work.now, ended_reason=reason, ended_by=_actor_label(work)),
        updated_at=work.now,
    )
    return 1


def _end_sessions(work: _Work, user_id: str, *, reason: str, keep: str | None = None) -> int:
    ended = 0
    for session in iter_user_sessions(work.table, user_id):
        if session.session_id == keep or session.ended:
            continue
        ended += _end_session(work, user_id, session.session_id, reason=reason)
    return ended


# ------------------------------------------------------------------------------------ access tokens


def _create_access_token(work: _Work) -> _Applied:
    """Store a token someone made for themselves: its name, its permissions and its secret's hash.

    A token carries catalog permissions only, and only ones its owner holds. The API made the
    token and showed it once; this request carries only the hash of its secret.
    """
    actor = work.view.users.get(work.request.actor_id)
    if actor is None or not actor.active:
        raise IdentityRejection("forbidden", "Your account can no longer make changes.")
    payload = work.payload
    token_id = clean_identifier(payload.get("token_id"), field="token_id")
    if get_access_token(work.table, token_id) is not None:
        raise IdentityRejection("invalid", "That token exists already.", field="token_id")
    name = clean_label(payload.get("name"), field="name", label="Name")
    if len(name) > TOKEN_NAME_MAX_LENGTH:
        raise IdentityRejection("invalid", f"Name must be at most {TOKEN_NAME_MAX_LENGTH} characters", field="name")
    permissions = clean_permissions(payload.get("permissions"), field="permissions")
    if not permissions or any(item not in TOKEN_PERMISSIONS for item in permissions):
        raise IdentityRejection(
            "invalid",
            "A token carries catalog permissions only: " + ", ".join(_permission_label(key) for key in TOKEN_PERMISSIONS) + ".",
            field="permissions",
        )
    dataset_ids = clean_ids(payload.get("datasets"), field="datasets")
    if len(dataset_ids) > MAX_TOKEN_DATASETS:
        raise IdentityRejection("invalid", f"A token can be limited to at most {MAX_TOKEN_DATASETS} datasets.", field="datasets")
    view = work.view
    for dataset_id in dataset_ids:
        if dataset_id not in view.datasets:
            raise IdentityRejection("not_found", "A chosen dataset no longer exists.", field="datasets")
    grants = view.grants_for_user(actor)
    for permission in permissions:
        # A token is limited to some datasets, or may use its permissions wherever its owner may.
        lacking = [dataset_id for dataset_id in dataset_ids if not grants.holds(permission, dataset_id)]
        if lacking:
            raise IdentityRejection(
                "forbidden",
                f"You can only make a token with access you hold yourself. Missing: {_permission_label(permission)} "
                f"on dataset {', '.join(_dataset_label(view, dataset_id) for dataset_id in lacking)}.",
                field="datasets",
            )
        if not dataset_ids and not grants.holds_anywhere(permission):
            raise IdentityRejection(
                "forbidden",
                f"You can only make a token with access you hold yourself. Missing: {_permission_label(permission)}.",
                field="permissions",
            )
    secret_hash = payload.get("secret_hash")
    if not isinstance(secret_hash, str) or not secret_hash.startswith("sha256:"):
        raise IdentityRejection("invalid", "The request carries no token.", field="token")
    expires_at = _parse_time(payload.get("expires_at"))
    latest = work.now_datetime + timedelta(days=MAX_TOKEN_LIFETIME_DAYS)
    if expires_at is None or expires_at > latest:
        expires_at = latest
    if expires_at <= work.now_datetime:
        raise IdentityRejection("invalid", "A token must expire in the future.", field="expires_at")
    live = [
        token
        for token in iter_user_access_tokens(work.table, actor.user_id)
        if not token.revoked and (_parse_time(token.expires_at) or work.now_datetime) > work.now_datetime
    ]
    if len(live) >= MAX_TOKENS_PER_USER:
        raise IdentityRejection(
            "limit",
            f"You have {MAX_TOKENS_PER_USER} tokens already. Revoke one you no longer use first.",
        )
    token = AccessToken(
        token_id=token_id,
        user_id=actor.user_id,
        name=name,
        permissions=tuple(permissions),
        secret_hash=secret_hash,
        created_at=work.now,
        expires_at=_format_time(expires_at),
        datasets=tuple(dataset_ids),
    )
    save_access_token(work.table, token, updated_at=work.now)
    details: dict[str, Any] = {"token_id": token_id, "name": name, "permissions": permissions, "expires_at": token.expires_at}
    if dataset_ids:
        details["datasets"] = [_dataset_label(view, dataset_id) for dataset_id in dataset_ids]
    work.audit("token.created", target=actor, details=details)
    return _Applied("access_token", token_id, directory_changed=False)


def _revoke_access_token(work: _Work) -> _Applied:
    """Revoke one of the caller's tokens; with Manage users, anyone's they could manage."""
    token_id = clean_identifier(work.payload.get("token_id"), field="token_id")
    token = get_access_token(work.table, token_id)
    if token is None:
        raise IdentityRejection("not_found", "That token no longer exists.", field="token_id")
    if token.user_id != work.request.actor_id:
        _, grants = _actor(work, "users.manage")
        owner = work.view.users.get(token.user_id)
        if owner is not None:
            _require_manageable(work.view, grants, owner)
    if token.revoked:
        return _Applied("access_token", token_id, directory_changed=False)
    save_access_token(
        work.table,
        replace(token, revoked_at=work.now, revoked_by=_actor_label(work)),
        updated_at=work.now,
    )
    owner = work.view.users.get(token.user_id)
    work.audit(
        "token.revoked",
        target=owner,
        target_type=None if owner is not None else "user",
        target_id=None if owner is not None else token.user_id,
        details={"token_id": token_id, "name": token.name},
    )
    return _Applied("access_token", token_id, directory_changed=False)


# --------------------------------------------------------------------------------------- datasets


def _create_dataset(work: _Work) -> _Applied:
    """Create a dataset: its creator owns it, and everyone signed in may read it."""
    actor, _ = _actor(work, "datasets.create")
    payload = work.payload
    dataset_id = clean_identifier(payload.get("dataset_id"), field="dataset_id")
    name = _dataset_name(payload.get("name"))
    description = clean_description(payload.get("description"))
    view = work.view
    if dataset_id in view.datasets:
        raise IdentityRejection("already_exists", "That dataset already exists.", field="dataset_id")
    if view.dataset_by_name(name) is not None:
        raise IdentityRejection("already_exists", f"A dataset named {name} already exists.", field="name")
    label = _actor_label(work)
    dataset = Dataset(
        dataset_id=dataset_id,
        name=name,
        description=description,
        created_at=work.now,
        created_by=label,
        updated_at=work.now,
        updated_by=label,
    )
    save_dataset(work.table, dataset)
    save_dataset_name(work.table, name, dataset_id, updated_at=work.now)
    for principal_type, principal_id, role_id in (
        ("user", actor.user_id, DATASET_OWNER_ROLE_ID),
        ("group", ALL_USERS_GROUP_ID, DATASET_READER_ROLE_ID),
    ):
        save_dataset_grant(
            work.table,
            DatasetGrant(
                dataset_id=dataset_id,
                principal_type=principal_type,
                principal_id=principal_id,
                role_ids=[role_id],
                updated_at=work.now,
                updated_by=label,
            ),
        )
    work.audit(
        "dataset.created",
        target=dataset,
        details={"description": description, "owner": actor.email, "readers": view.groups[ALL_USERS_GROUP_ID].name},
    )
    return _Applied("dataset", dataset_id)


def _update_dataset(work: _Work) -> _Applied:
    """Change a dataset's description. Its name never changes."""
    dataset = _require_dataset(work)
    _actor_on(work, dataset, "dataset.manage")
    _require_revision(work, dataset.revision, f"Dataset {dataset.name}")
    payload = work.payload
    if "name" in payload and payload["name"] != dataset.name:
        raise IdentityRejection("invalid", "A dataset's name never changes.", field="name")
    details: dict[str, Any] = {}
    updated = dataset
    if "description" in payload:
        description = clean_description(payload["description"])
        if description != dataset.description:
            details["description"] = {"from": dataset.description, "to": description}
            updated = replace(updated, description=description)
    if updated == dataset:
        return _Applied("dataset", dataset.dataset_id, directory_changed=False)
    updated = replace(updated, revision=dataset.revision + 1, updated_at=work.now, updated_by=_actor_label(work))
    save_dataset(work.table, updated)
    work.audit("dataset.updated", target=updated, details=details)
    return _Applied("dataset", dataset.dataset_id, directory_changed=False)


def _delete_dataset(work: _Work) -> _Applied:
    """Delete a dataset that holds no table, with its grants; its name is free again."""
    dataset = _require_dataset(work)
    _actor_on(work, dataset, "dataset.manage")
    claims = iter_table_claims(work.table, dataset.dataset_id)
    if claims:
        shown = ", ".join(claim.table for claim in claims[:10]) + (", …" if len(claims) > 10 else "")
        raise IdentityRejection(
            "dataset_not_empty",
            f"Dataset {dataset.name} still holds {len(claims)} table{'s' if len(claims) != 1 else ''}: {shown}. "
            "Drop them, or delete the materializations that write them, first.",
            details={"dataset": dataset.name, "tables": [claim.table for claim in claims]},
        )
    for grant in work.view.grants_on(dataset.dataset_id):
        delete_dataset_grant(work.table, grant.dataset_id, grant.principal_type, grant.principal_id)
    delete_dataset_name(work.table, dataset.name)
    delete_dataset_record(work.table, dataset.dataset_id)
    work.audit("dataset.deleted", target=dataset, details={"description": dataset.description})
    return _Applied("dataset", dataset.dataset_id)


def _set_dataset_grant(work: _Work) -> _Applied:
    """Set the roles a person or a group holds on a dataset; no roles removes their grant.

    Only roles of dataset permissions can be granted on a dataset, never to guests, and only with
    access the actor holds on the dataset themselves.
    """
    dataset = _require_dataset(work)
    _, grants = _actor_on(work, dataset, "dataset.manage")
    payload = work.payload
    view = work.view
    principal_type = payload.get("principal_type")
    if principal_type not in GRANT_PRINCIPAL_TYPES:
        raise IdentityRejection("invalid", "A dataset is shared with a user or a group.", field="principal_type")
    principal_id = clean_identifier(payload.get("principal_id"), field="principal_id")
    role_ids = clean_ids(payload.get("role_ids"), field="role_ids")
    if principal_type == "user":
        principal_user = view.users.get(principal_id)
        if principal_user is None:
            raise IdentityRejection("not_found", "That person no longer has an account.", field="principal_id")
        principal_label = principal_user.email
    else:
        principal_group = view.groups.get(principal_id)
        if principal_group is None:
            raise IdentityRejection("not_found", "That group no longer exists.", field="principal_id")
        if principal_group.membership == "guests":
            raise IdentityRejection(
                "invalid",
                f"{principal_group.name} cannot be given a dataset: reading data needs someone signed in.",
                field="principal_id",
            )
        principal_label = principal_group.name
    for role_id in role_ids:
        role = view.roles.get(role_id)
        if role is None:
            raise IdentityRejection("not_found", "A chosen role no longer exists.", field="role_ids")
        if not dataset_grantable(role.permissions):
            raise IdentityRejection(
                "invalid",
                f"The {role.name} role holds permissions about the whole deployment; only a role of dataset "
                "permissions can be granted on a dataset.",
                field="role_ids",
            )
    current = view.dataset_grants.get((dataset.dataset_id, principal_type, principal_id))
    before = [] if current is None else list(current.role_ids)
    added = [role_id for role_id in role_ids if role_id not in before]
    removed = [role_id for role_id in before if role_id not in role_ids]
    if not added and not removed:
        return _Applied("dataset", dataset.dataset_id, directory_changed=False)
    involved = view.grants_for_roles(added + removed)
    _require_grantable(
        grants,
        Grants(
            group_ids=(),
            role_ids=tuple(added + removed),
            permissions=frozenset(),
            datasets={dataset.dataset_id: frozenset(involved.permissions & DATASET_PERMISSION_KEYS)},
        ),
        doing="share a dataset",
        view=view,
    )
    if role_ids:
        save_dataset_grant(
            work.table,
            DatasetGrant(
                dataset_id=dataset.dataset_id,
                principal_type=principal_type,
                principal_id=principal_id,
                role_ids=role_ids,
                updated_at=work.now,
                updated_by=_actor_label(work),
            ),
        )
    else:
        delete_dataset_grant(work.table, dataset.dataset_id, principal_type, principal_id)
    work.audit(
        "dataset.access_changed",
        target=dataset,
        details={
            "principal_type": principal_type,
            "principal": principal_label,
            "roles_added": _role_names(view, added),
            "roles_removed": _role_names(view, removed),
        },
    )
    return _Applied("dataset", dataset.dataset_id)


def _claim_tables(work: _Work) -> _Applied:
    """Take the names a materialization's new tables will have in their datasets: all, or none.

    Each dataset must exist, the person who asked must still be able to put tables in it, and each
    name must be free - or already the same materialization's, as when a request is applied again.
    `release` names tables the same change drops, whose names are freed in the same step, so a
    refused change keeps them. A materialization that has released everything (it is being
    deleted) can claim nothing.
    """
    payload = work.payload
    materialization_id = clean_identifier(payload.get("materialization_id"), field="materialization_id")
    tables = payload.get("tables")
    releases = payload.get("release") or []
    if not isinstance(tables, list) or not isinstance(releases, list):
        raise IdentityRejection("invalid", "A claim names the tables to claim and to release.", field="tables")
    view = work.view
    grants: Grants | None = None
    if work.request.actor_id != SYSTEM_ACTOR:
        actor = view.users.get(work.request.actor_id)
        if actor is None or not actor.active:
            raise IdentityRejection("forbidden", "Your account can no longer make changes.")
        grants = view.grants_for_user(actor)
    if get_claim_release(work.table, materialization_id) is not None:
        raise IdentityRejection(
            "materialization_deleted",
            f"Materialization {materialization_id} is being deleted, so it cannot take table names.",
            details={"materialization_id": materialization_id},
        )
    claims: list[TableClaim] = []
    seen: set[tuple[str, str]] = set()
    for index, item in enumerate(tables):
        if not isinstance(item, dict):
            raise IdentityRejection("invalid", "A claim names each table's dataset and name.", field=f"tables.{index}")
        dataset_id = str(item.get("dataset_id") or "")
        name = str(item.get("name") or "")
        source_topic = str(item.get("source_topic") or "")
        dataset = view.datasets.get(dataset_id)
        details = {
            "index": index,
            "dataset": dataset.name if dataset is not None else item.get("dataset"),
            "dataset_id": dataset_id,
            "table": name,
            "source_topic": source_topic,
        }
        problem = table_name_problem(name)
        if problem is not None or not source_topic:
            raise IdentityRejection("invalid", problem or "Each table holds a topic.", field=f"tables.{index}.name", details=details)
        if dataset is None:
            raise IdentityRejection(
                "dataset_not_found",
                f"Dataset {item.get('dataset') or dataset_id} does not exist (any more).",
                field=f"tables.{index}.dataset",
                details=details,
            )
        if grants is not None and not grants.holds("dataset.write", dataset_id):
            raise IdentityRejection(
                "forbidden",
                f"You cannot put tables in dataset {dataset.name}: it needs “{_permission_label('dataset.write')}” on it.",
                field=f"tables.{index}.dataset",
                details=details,
            )
        if (dataset_id, name) in seen:
            raise IdentityRejection(
                "invalid",
                f"{dataset.name}.{name} is named twice.",
                field=f"tables.{index}.name",
                details=details,
            )
        seen.add((dataset_id, name))
        held = get_table_claim(work.table, dataset_id, name)
        if held is not None and held.materialization_id != materialization_id:
            raise IdentityRejection(
                "table_name_taken",
                f"{dataset.name}.{name} is taken: another materialization's table has that name. Choose another name.",
                field=f"tables.{index}.name",
                details={**details, "materialization_id": held.materialization_id},
            )
        claims.append(
            TableClaim(
                dataset_id=dataset_id,
                table=name,
                materialization_id=materialization_id,
                source_topic=source_topic,
                claimed_at=work.now,
                claimed_by=_actor_label(work),
                request_id=work.request.request_id,
            )
        )
    released: list[TableClaim] = []
    for item in releases:
        if not isinstance(item, dict):
            continue
        held = get_table_claim(work.table, str(item.get("dataset_id") or ""), str(item.get("name") or ""))
        if held is not None and held.materialization_id == materialization_id:
            released.append(held)
    for claim in claims:
        save_table_claim(work.table, claim)
        _audit_claim(work, "dataset.table_claimed", claim)
    for claim in released:
        delete_table_claim(work.table, claim.dataset_id, claim.table)
        _audit_claim(work, "dataset.table_released", claim, reason="dropped")
    return _Applied("materialization", materialization_id, directory_changed=False)


def _release_tables(work: _Work) -> _Applied:
    """Free the names of a materialization's tables that left the catalog: some, or all of them.

    Only the materialization's own claims are released, never another's of the same name. Releasing
    all of them - it is being deleted - is remembered, so a claim of it that arrives late is refused.
    """
    payload = work.payload
    materialization_id = clean_identifier(payload.get("materialization_id"), field="materialization_id")
    reason = str(payload.get("reason") or "released")
    if payload.get("all"):
        targets = [claim for claim in iter_table_claims(work.table) if claim.materialization_id == materialization_id]
        save_claim_release(work.table, ClaimRelease(materialization_id=materialization_id, released_at=work.now))
    else:
        targets = []
        for item in payload.get("tables") or []:
            if not isinstance(item, dict):
                continue
            held = get_table_claim(work.table, str(item.get("dataset_id") or ""), str(item.get("name") or ""))
            if held is not None and held.materialization_id == materialization_id:
                targets.append(held)
    for claim in targets:
        delete_table_claim(work.table, claim.dataset_id, claim.table)
        _audit_claim(work, "dataset.table_released", claim, reason=reason)
    return _Applied("materialization", materialization_id, directory_changed=False)


def _sweep_table_claims(work: _Work) -> None:
    """Release claims whose table is gone, and claim again the names of tables that lost theirs.

    A release is sent when a table leaves the catalog, so this only catches what went astray. It
    reads the materializations' records and writes nothing but directory records, and it leaves
    alone whatever changed within `CLAIM_SWEEP_GRACE` - a claim still being acted on, a record
    this replica may not have read yet.
    """
    state = work.control_plane_table
    if state is None:
        return
    now = work.now_datetime
    claims = iter_table_claims(work.table)
    by_materialization: dict[str, list[TableClaim]] = {}
    for claim in claims:
        by_materialization.setdefault(claim.materialization_id, []).append(claim)
    for materialization_id, held in by_materialization.items():
        materialization = get_materialization(state, materialization_id)
        if materialization is not None and materialization.desired_phase in ("deleting", "deleted"):
            stale = held
        elif materialization is None:
            stale = [claim for claim in held if _settled(claim.claimed_at, now)]
        elif not _settled(materialization.updated_at, now):
            continue
        else:
            live = {(spec.dataset_id, spec.name.lower()) for spec in materialization.all_tables}
            pending = materialization.pending_topic_change
            if pending is not None:
                live |= {(spec.dataset_id, spec.name.lower()) for spec in pending.added}
            stale = [
                claim
                for claim in held
                if (claim.dataset_id, claim.table.lower()) not in live and _settled(claim.claimed_at, now)
            ]
        for claim in stale:
            delete_table_claim(work.table, claim.dataset_id, claim.table)
            _audit_claim(work, "dataset.table_released", claim, reason="sweep")
    view = work.view
    for materialization in iter_materializations(state):
        if materialization.desired_phase in ("deleting", "deleted") or not _settled(materialization.updated_at, now):
            continue
        records = {record.name: record for record in list_materialization_tables(state, materialization.materialization_id)}
        for spec in materialization.all_tables:
            record = records.get(spec.name)
            if record is None or record.state not in ("active", "retired") or spec.dataset_id not in view.datasets:
                continue
            held_by = get_table_claim(work.table, spec.dataset_id, spec.name)
            if held_by is None:
                claim = TableClaim(
                    dataset_id=spec.dataset_id,
                    table=spec.name,
                    materialization_id=materialization.materialization_id,
                    source_topic=spec.source_topic,
                    claimed_at=work.now,
                    claimed_by=SYSTEM_ACTOR,
                    request_id=work.request.request_id,
                )
                save_table_claim(work.table, claim)
                _audit_claim(work, "dataset.table_claimed", claim, reason="sweep")
            elif held_by.materialization_id != materialization.materialization_id:
                LOGGER.warning(
                    "Table %s.%s of %s has no claim of its own: materialization %s holds the name",
                    view.datasets[spec.dataset_id].name,
                    spec.name,
                    materialization.materialization_id,
                    held_by.materialization_id,
                )


def _settled(moment: str | None, now: datetime) -> bool:
    """Whether `moment` is longer ago than the sweep's grace."""
    parsed = _parse_time(moment)
    return parsed is not None and parsed + CLAIM_SWEEP_GRACE < now


def _audit_claim(work: _Work, action: str, claim: TableClaim, *, reason: str | None = None) -> None:
    dataset = work.view.datasets.get(claim.dataset_id)
    details: dict[str, Any] = {
        "table": claim.table,
        "materialization_id": claim.materialization_id,
        "source_topic": claim.source_topic,
    }
    if reason is not None:
        details["reason"] = reason
    work.audit(
        action,
        target=dataset,
        target_type=None if dataset is not None else "dataset",
        target_id=None if dataset is not None else claim.dataset_id,
        details=details,
    )


# ----------------------------------------------------------------------------------------- system


def _bootstrap(work: _Work) -> _Applied:
    """Store the built-in groups, and make the bootstrap administrator when there is no administrator."""
    changed = False
    for definition in BUILT_IN_GROUPS:
        if not _group_is_stored(work, definition.group_id):
            save_group(work.table, replace(default_group(definition), created_at=work.now, updated_at=work.now))
            changed = True
    state = get_directory_state(work.table)
    if state.seeded_at is None:
        save_directory_state(work.table, replace(state, seeded_at=work.now, updated_at=work.now))
        work.audit("directory.seeded", target_type="directory", target_label="Built-in groups")

    view = DirectoryView.load(work.table)
    if view.administrators():
        return _Applied(directory_changed=changed)
    admin = work.secrets.bootstrap_admin
    if admin is None:
        LOGGER.warning(
            "No active administrator exists and no bootstrap administrator is configured "
            "(AUTH_BOOTSTRAP_ADMIN_EMAIL, AUTH_BOOTSTRAP_ADMIN_PASSWORD); "
            "nobody can manage users"
        )
        return _Applied(directory_changed=changed)
    try:
        email = clean_email(admin.email)
    except IdentityValidationError:
        LOGGER.error("The bootstrap administrator's email %r is not valid", admin.email)
        return _Applied(directory_changed=changed)
    if len(admin.password) < work.config.password_min_length:
        LOGGER.warning(
            "The bootstrap administrator's password is shorter than %d characters; it must be "
            "changed at first sign-in",
            work.config.password_min_length,
        )
    credential = PasswordCredential(hash=hash_password(admin.password), set_at=work.now, set_by=SYSTEM_ACTOR, must_change=True)
    existing = view.user_by_email(email)
    if existing is not None:
        user = replace(
            existing,
            status="active",
            deactivated_at=None,
            deactivated_by=None,
            group_ids=existing.group_ids if ADMIN_GROUP_ID in existing.group_ids else [*existing.group_ids, ADMIN_GROUP_ID],
            password=credential,
            session_epoch=existing.session_epoch + 1,
            revision=existing.revision + 1,
            updated_at=work.now,
            updated_by=SYSTEM_ACTOR,
        )
        action = "user.bootstrap_restored"
    else:
        user = User(
            user_id=new_user_id(),
            email=email,
            first_name=clean_person_name(admin.first_name, field="first_name", label="First name"),
            last_name=clean_person_name(admin.last_name, field="last_name", label="Last name"),
            group_ids=[ADMIN_GROUP_ID],
            password=credential,
            created_at=work.now,
            created_by=SYSTEM_ACTOR,
            updated_at=work.now,
            updated_by=SYSTEM_ACTOR,
        )
        action = "user.bootstrapped"
    save_user(work.table, user)
    save_user_email(work.table, user.email, user.user_id, updated_at=work.now)
    delete_sign_in_guard(work.table, user.email)
    LOGGER.warning("No active administrator existed; %s is now one, from the bootstrap secret", user.email)
    work.audit(action, target=user, actor_id=SYSTEM_ACTOR, actor_email=SYSTEM_ACTOR)
    return _Applied("user", user.user_id)


def _maintenance(work: _Work) -> _Applied:
    """Remove sessions, sign-in counts and outcomes that ran out, and audit entries past retention;
    sweep up table-name claims whose tables are gone."""
    now = work.now_datetime
    for session in list(iter_sessions(work.table)):
        expired = _parse_time(session.expires_at)
        ended = _parse_time(session.ended_at)
        if (expired is not None and expired + SESSION_RECORD_RETENTION < now) or (
            ended is not None and ended + SESSION_RECORD_RETENTION < now
        ):
            delete_session_record(work.table, session.user_id, session.session_id)
    for guard in list(iter_sign_in_guards(work.table)):
        locked_until = _parse_time(guard.locked_until)
        last_failed = _parse_time(guard.last_failed_at)
        if (locked_until is None or locked_until < now) and (
            last_failed is None or last_failed + SIGN_IN_GUARD_RETENTION < now
        ):
            delete_sign_in_guard(work.table, guard.email)
    for outcome in list(iter_request_outcomes(work.table)):
        completed = _parse_time(outcome.completed_at)
        if completed is not None and completed + REQUEST_OUTCOME_RETENTION < now:
            delete_request_outcome(work.table, outcome.request_id)
    for token in list(iter_access_tokens(work.table)):
        ended = _parse_time(token.revoked_at) or _parse_time(token.expires_at)
        if ended is not None and ended + TOKEN_RECORD_RETENTION < now:
            delete_access_token(work.table, token.token_id)
    for release in iter_claim_releases(work.table):
        released = _parse_time(release.released_at)
        if released is not None and released + CLAIM_RELEASE_RETENTION < now:
            delete_claim_release(work.table, release.materialization_id)
    _sweep_table_claims(work)
    if work.audit_table is not None:
        cutoff = _format_time(now - timedelta(days=work.config.audit_log_retention_days))
        delete_audit_entries_before(work.audit_table, cutoff, limit=AUDIT_PURGE_BATCH_LIMIT)
    return _Applied(directory_changed=False)


_HANDLERS: dict[str, Callable[[_Work], _Applied]] = {
    "create_user": _create_user,
    "update_user": _update_user,
    "set_user_password": _set_user_password,
    "change_password": _change_password,
    "sign_out_user": _sign_out_user,
    "unlock_user": _unlock_user,
    "delete_user": _delete_user,
    "create_group": _create_group,
    "update_group": _update_group,
    "delete_group": _delete_group,
    "create_role": _create_role,
    "update_role": _update_role,
    "delete_role": _delete_role,
    "create_access_token": _create_access_token,
    "revoke_access_token": _revoke_access_token,
    "create_dataset": _create_dataset,
    "update_dataset": _update_dataset,
    "delete_dataset": _delete_dataset,
    "set_dataset_grant": _set_dataset_grant,
    "claim_tables": _claim_tables,
    "release_tables": _release_tables,
    "sign_in_succeeded": _sign_in_succeeded,
    "sign_in_failed": _sign_in_failed,
    "session_refreshed": _session_refreshed,
    "sign_out": _sign_out,
    "bootstrap": _bootstrap,
    "maintenance": _maintenance,
}


# ---------------------------------------------------------------------------------------- helpers


def new_user_id() -> str:
    return f"usr-{uuid4().hex[:12]}"


def new_group_id() -> str:
    return f"grp-{uuid4().hex[:12]}"


def new_role_id() -> str:
    return f"rol-{uuid4().hex[:12]}"


def new_dataset_id() -> str:
    return f"ds-{uuid4().hex[:12]}"


def new_request_id() -> str:
    """A directory request's id."""
    return f"idr-{uuid4().hex[:12]}"


_last_audit_stamp = 0


def new_audit_entry_id() -> str:
    """An audit entry id that sorts after every earlier one this process made.

    Audit keys sort by the second an entry occurred, then by its id, so entries of the same second
    list in the order they were recorded: microseconds since the epoch, kept strictly increasing,
    then a random suffix so that two processes cannot collide.
    """
    global _last_audit_stamp
    _last_audit_stamp = max(time.time_ns() // 1_000, _last_audit_stamp + 1)
    return f"aud-{_last_audit_stamp:014x}{uuid4().hex[:4]}"


def _save_membership(work: _Work, user: User, group_ids: list[str]) -> None:
    current = get_user(work.table, user.user_id) or user
    save_user(
        work.table,
        replace(
            current,
            group_ids=group_ids,
            revision=current.revision + 1,
            updated_at=work.now,
            updated_by=_actor_label(work),
        ),
    )


def _require_users(view: DirectoryView, user_ids: list[str], *, field: str) -> list[User]:
    users: list[User] = []
    for user_id in user_ids:
        user = view.users.get(user_id)
        if user is None:
            raise IdentityRejection("not_found", "A chosen person no longer has an account.", field=field)
        users.append(user)
    return users


def _group_is_stored(work: _Work, group_id: str) -> bool:
    return any(key == group_key(group_id) for key, _ in work.table.prefix_scan(group_key(group_id)))


def _grants_of(permissions: list[str]) -> Grants:
    return Grants(group_ids=(), role_ids=(), permissions=expand_permissions(permissions))


def _group_names(view: DirectoryView, group_ids: list[str]) -> list[str]:
    return [view.groups[group_id].name if group_id in view.groups else group_id for group_id in group_ids]


def _role_names(view: DirectoryView, role_ids: list[str]) -> list[str]:
    return [view.roles[role_id].name if role_id in view.roles else role_id for role_id in role_ids]


def _permission_label(key: str) -> str:
    definition = permission_definition(key)
    return definition.label if definition is not None else key


def _user_label(user: User) -> str:
    return f"{user.display_name} <{user.email}>"


def _describe_target(target: User | Group | Role | Dataset) -> tuple[str, str, str]:
    if isinstance(target, User):
        return "user", target.user_id, _user_label(target)
    if isinstance(target, Group):
        return "group", target.group_id, target.name
    if isinstance(target, Dataset):
        return "dataset", target.dataset_id, target.name
    return "role", target.role_id, target.name


def _require_dataset(work: _Work) -> Dataset:
    dataset_id = clean_identifier(work.payload.get("dataset_id"), field="dataset_id")
    dataset = work.view.datasets.get(dataset_id)
    if dataset is None:
        raise IdentityRejection("not_found", "That dataset no longer exists.", field="dataset_id")
    return dataset


def _actor_on(work: _Work, dataset: Dataset, permission: str) -> tuple[User, Grants]:
    """The person who asked, still active and still holding a permission on the dataset."""
    actor = work.view.users.get(work.request.actor_id)
    if actor is None or not actor.active:
        raise IdentityRejection("forbidden", "Your account can no longer make changes.")
    grants = work.view.grants_for_user(actor)
    if not grants.holds(permission, dataset.dataset_id):
        raise IdentityRejection(
            "forbidden",
            f"You do not have “{_permission_label(permission)}” on dataset {dataset.name}.",
        )
    return actor, grants


def _dataset_name(value: Any) -> str:
    problem = dataset_name_problem(value)
    if problem is not None:
        raise IdentityValidationError(problem[0].upper() + problem[1:], field="name")
    return str(value).strip()


def _dataset_label(view: DirectoryView, dataset_id: str) -> str:
    dataset = view.datasets.get(dataset_id)
    return dataset.name if dataset is not None else dataset_id


def _parse_time(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)


def _format_time(value: datetime) -> str:
    return value.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _utc_now() -> str:
    return _format_time(datetime.now(tz=UTC))
