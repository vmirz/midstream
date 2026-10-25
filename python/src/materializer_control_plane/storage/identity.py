"""Persistence for the directory: people, groups, roles, sessions, access tokens, datasets with their
grants and table-name claims, and the audit log.

Two global tables, written only by the identity agent:

- `directory` holds users and the email index, groups, custom roles, datasets and the dataset name
  index, dataset grants, table-name claims, the directory's revision, sessions, access tokens,
  sign-in state and the outcome of each directory request;
- `identity-audit` holds the audit log, keyed by time so a page of it reads one day at a time.

Both are global because every replica authorizes requests from them, and a WebView cannot route a
read to the replica that owns a partition. Every write arrives on one Kafka key, so each record
lives in one shard; reads still keep the newest copy of a key, as the `state` table's do, in case
a partition count ever changes underneath them.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import Any

from materializer_control_plane.identity.permissions import (
    BUILT_IN_GROUPS,
    BUILT_IN_ROLES,
    BuiltInGroup,
    BuiltInRole,
    built_in_group,
    built_in_role,
)
from materializer_control_plane.models import RecordEnvelope
from materializer_control_plane.models.identity import (
    AccessToken,
    AuditEntry,
    ClaimRelease,
    Dataset,
    DatasetGrant,
    DirectoryState,
    Group,
    IdentityRequestOutcome,
    Role,
    Session,
    SignInGuard,
    TableClaim,
    User,
    UserActivity,
)
from materializer_control_plane.storage.envelopes import encode_payload_envelope
from materializer_control_plane.storage.table import TableProtocol

#: The Kafka key of every identity request: one partition, so they apply strictly in order.
IDENTITY_ROUTING_KEY = "identity"

USER_PREFIX = "user:"
USER_EMAIL_PREFIX = "user-email:"
GROUP_PREFIX = "group:"
ROLE_PREFIX = "role:"
SESSION_PREFIX = "session:"
TOKEN_PREFIX = "token:"
SIGN_IN_PREFIX = "sign-in:"
ACTIVITY_PREFIX = "activity:"
REQUEST_PREFIX = "request:"
DIRECTORY_KEY = "directory"
AUDIT_PREFIX = "audit:"
DATASET_PREFIX = "dataset:"
DATASET_NAME_PREFIX = "dataset-name:"
DATASET_GRANT_PREFIX = "dataset-grant:"
TABLE_CLAIM_PREFIX = "dataset-table:"
CLAIM_RELEASE_PREFIX = "claim-release:"

USER_ENTITY = "user"
USER_EMAIL_ENTITY = "user-email"
GROUP_ENTITY = "group"
ROLE_ENTITY = "role"
SESSION_ENTITY = "session"
TOKEN_ENTITY = "access-token"
SIGN_IN_ENTITY = "sign-in-guard"
ACTIVITY_ENTITY = "user-activity"
REQUEST_ENTITY = "identity-request"
DIRECTORY_ENTITY = "directory"
AUDIT_ENTITY = "audit-entry"
DATASET_ENTITY = "dataset"
DATASET_NAME_ENTITY = "dataset-name"
DATASET_GRANT_ENTITY = "dataset-grant"
TABLE_CLAIM_ENTITY = "table-claim"
CLAIM_RELEASE_ENTITY = "claim-release"

#: Stands in for the creation time of a built-in group or role that was never stored.
BUILT_IN_TIMESTAMP = "1970-01-01T00:00:00Z"
SYSTEM_ACTOR = "system"


def identity_routing_key() -> str:
    return IDENTITY_ROUTING_KEY


def normalize_email(email: str) -> str:
    """Emails are compared without regard to case or surrounding space."""
    return email.strip().lower()


def user_key(user_id: str) -> str:
    return f"{USER_PREFIX}{user_id}"


def user_email_key(email: str) -> str:
    return f"{USER_EMAIL_PREFIX}{normalize_email(email)}"


def group_key(group_id: str) -> str:
    return f"{GROUP_PREFIX}{group_id}"


def role_key(role_id: str) -> str:
    return f"{ROLE_PREFIX}{role_id}"


def session_key(user_id: str, session_id: str) -> str:
    return f"{SESSION_PREFIX}{user_id}:{session_id}"


def token_key(token_id: str) -> str:
    return f"{TOKEN_PREFIX}{token_id}"


def sign_in_guard_key(email: str) -> str:
    return f"{SIGN_IN_PREFIX}{normalize_email(email)}"


def activity_key(user_id: str) -> str:
    return f"{ACTIVITY_PREFIX}{user_id}"


def request_outcome_key(request_id: str) -> str:
    return f"{REQUEST_PREFIX}{request_id}"


def audit_key(entry: AuditEntry) -> str:
    # The timestamp leads, so keys sort by time and one day is one prefix.
    return f"{AUDIT_PREFIX}{entry.occurred_at}:{entry.entry_id}"


def dataset_key(dataset_id: str) -> str:
    return f"{DATASET_PREFIX}{dataset_id}"


def dataset_name_key(name: str) -> str:
    # Names are lower case only (`table_names.py`); lowered here too, so an index lookup can never
    # tell apart two spellings an engine takes for one name.
    return f"{DATASET_NAME_PREFIX}{name.strip().lower()}"


def dataset_grant_prefix(dataset_id: str) -> str:
    return f"{DATASET_GRANT_PREFIX}{dataset_id}:"


def dataset_grant_key(dataset_id: str, principal_type: str, principal_id: str) -> str:
    """`dataset-grant:<dataset id>:<user or group>:<their id>`."""
    return f"{dataset_grant_prefix(dataset_id)}{principal_type}:{principal_id}"


def table_claim_prefix(dataset_id: str) -> str:
    return f"{TABLE_CLAIM_PREFIX}{dataset_id}:"


def table_claim_key(dataset_id: str, table: str) -> str:
    return f"{table_claim_prefix(dataset_id)}{table.strip().lower()}"


def claim_release_key(materialization_id: str) -> str:
    return f"{CLAIM_RELEASE_PREFIX}{materialization_id}"


# --------------------------------------------------------------------------------------------- reads


def _decode(raw: Any, entity_type: str) -> RecordEnvelope | None:
    if not isinstance(raw, dict):
        return None
    try:
        envelope = RecordEnvelope.from_dict(raw)
    except (KeyError, TypeError, ValueError):
        return None
    return envelope if envelope.entity_type == entity_type else None


def _rank(envelope: RecordEnvelope) -> tuple[str, int]:
    revision = envelope.payload.get("revision") if isinstance(envelope.payload, dict) else None
    return (envelope.updated_at or "", revision if isinstance(revision, int) else 0)


def _read(table: TableProtocol | None, key: str, entity_type: str) -> dict[str, Any] | None:
    """One record's payload, keeping the newest copy when a key is found in more than one shard."""
    if table is None:
        return None
    newest: RecordEnvelope | None = None
    for scanned_key, raw in table.prefix_scan(key):
        if scanned_key != key:
            continue
        envelope = _decode(raw, entity_type)
        if envelope is not None and (newest is None or _rank(envelope) > _rank(newest)):
            newest = envelope
    if newest is None:
        newest = _decode(table.get(key), entity_type)
    return None if newest is None else newest.payload


def _scan(table: TableProtocol | None, prefix: str, entity_type: str) -> dict[str, dict[str, Any]]:
    """Every record under a prefix, by key, keeping the newest copy of each."""
    if table is None:
        return {}
    newest: dict[str, RecordEnvelope] = {}
    for key, raw in table.prefix_scan(prefix):
        envelope = _decode(raw, entity_type)
        if envelope is None:
            continue
        current = newest.get(key)
        if current is None or _rank(envelope) > _rank(current):
            newest[key] = envelope
    return {key: envelope.payload for key, envelope in newest.items()}


def _write(table: TableProtocol, key: str, *, entity_type: str, entity_id: str, updated_at: str, payload: dict[str, Any]) -> None:
    table[key] = encode_payload_envelope(
        entity_type=entity_type,
        entity_id=entity_id,
        updated_at=updated_at,
        payload=payload,
    )


def _delete(table: TableProtocol, key: str) -> None:
    try:
        del table[key]
    except KeyError:
        pass


# ----------------------------------------------------------------------------------------- directory


def get_directory_state(table: TableProtocol | None) -> DirectoryState:
    payload = _read(table, DIRECTORY_KEY, DIRECTORY_ENTITY)
    return DirectoryState() if payload is None else DirectoryState.from_dict(payload)


def save_directory_state(table: TableProtocol, state: DirectoryState) -> None:
    _write(
        table,
        DIRECTORY_KEY,
        entity_type=DIRECTORY_ENTITY,
        entity_id=DIRECTORY_KEY,
        updated_at=state.updated_at or BUILT_IN_TIMESTAMP,
        payload=state.to_dict(),
    )


def get_user(table: TableProtocol | None, user_id: str) -> User | None:
    payload = _read(table, user_key(user_id), USER_ENTITY)
    return None if payload is None else User.from_dict(payload)


def find_user_by_email(table: TableProtocol | None, email: str) -> User | None:
    """The account that signs in with this email, or None."""
    payload = _read(table, user_email_key(email), USER_EMAIL_ENTITY)
    if payload is None:
        return None
    user = get_user(table, str(payload.get("user_id", "")))
    if user is None or normalize_email(user.email) != normalize_email(email):
        return None
    return user


def iter_users(table: TableProtocol | None) -> Iterator[User]:
    for payload in _scan(table, USER_PREFIX, USER_ENTITY).values():
        yield User.from_dict(payload)


def save_user(table: TableProtocol, user: User) -> None:
    _write(
        table,
        user_key(user.user_id),
        entity_type=USER_ENTITY,
        entity_id=user.user_id,
        updated_at=user.updated_at,
        payload=user.to_dict(),
    )


def delete_user_record(table: TableProtocol, user_id: str) -> None:
    _delete(table, user_key(user_id))


def save_user_email(table: TableProtocol, email: str, user_id: str, *, updated_at: str) -> None:
    normalized = normalize_email(email)
    _write(
        table,
        user_email_key(normalized),
        entity_type=USER_EMAIL_ENTITY,
        entity_id=normalized,
        updated_at=updated_at,
        payload={"email": normalized, "user_id": user_id},
    )


def delete_user_email(table: TableProtocol, email: str) -> None:
    _delete(table, user_email_key(email))


def default_group(definition: BuiltInGroup) -> Group:
    """A built-in group as it is before anyone has changed it."""
    return Group(
        group_id=definition.group_id,
        name=definition.name,
        description=definition.description,
        role_ids=list(definition.role_ids),
        membership=definition.membership,
        built_in=True,
        created_at=BUILT_IN_TIMESTAMP,
        created_by=SYSTEM_ACTOR,
        updated_at=BUILT_IN_TIMESTAMP,
        updated_by=SYSTEM_ACTOR,
    )


def get_group(table: TableProtocol | None, group_id: str) -> Group | None:
    """A group, or a built-in group's defaults when it was never stored."""
    payload = _read(table, group_key(group_id), GROUP_ENTITY)
    if payload is not None:
        return Group.from_dict(payload)
    definition = built_in_group(group_id)
    return None if definition is None else default_group(definition)


def iter_groups(table: TableProtocol | None) -> list[Group]:
    """Every group: the built-in ones first, in their fixed order, then the rest by name."""
    stored = {payload["group_id"]: Group.from_dict(payload) for payload in _scan(table, GROUP_PREFIX, GROUP_ENTITY).values()}
    built_ins = [stored.pop(definition.group_id, None) or default_group(definition) for definition in BUILT_IN_GROUPS]
    return built_ins + sorted(stored.values(), key=lambda group: group.name.lower())


def save_group(table: TableProtocol, group: Group) -> None:
    _write(
        table,
        group_key(group.group_id),
        entity_type=GROUP_ENTITY,
        entity_id=group.group_id,
        updated_at=group.updated_at,
        payload=group.to_dict(),
    )


def delete_group_record(table: TableProtocol, group_id: str) -> None:
    _delete(table, group_key(group_id))


def role_from_built_in(definition: BuiltInRole) -> Role:
    return Role(
        role_id=definition.role_id,
        name=definition.name,
        description=definition.description,
        permissions=list(definition.permissions),
        built_in=True,
        created_at=BUILT_IN_TIMESTAMP,
        created_by=SYSTEM_ACTOR,
        updated_at=BUILT_IN_TIMESTAMP,
        updated_by=SYSTEM_ACTOR,
    )


def get_role(table: TableProtocol | None, role_id: str) -> Role | None:
    """A role: built-in roles come from code, custom ones from the table."""
    definition = built_in_role(role_id)
    if definition is not None:
        return role_from_built_in(definition)
    payload = _read(table, role_key(role_id), ROLE_ENTITY)
    return None if payload is None else Role.from_dict(payload)


def iter_roles(table: TableProtocol | None) -> list[Role]:
    """Every role: the built-in ones first, then custom roles by name."""
    custom = [Role.from_dict(payload) for payload in _scan(table, ROLE_PREFIX, ROLE_ENTITY).values()]
    built_ins = [role_from_built_in(definition) for definition in BUILT_IN_ROLES]
    return built_ins + sorted(
        (role for role in custom if built_in_role(role.role_id) is None),
        key=lambda role: role.name.lower(),
    )


def save_role(table: TableProtocol, role: Role) -> None:
    _write(
        table,
        role_key(role.role_id),
        entity_type=ROLE_ENTITY,
        entity_id=role.role_id,
        updated_at=role.updated_at,
        payload=role.to_dict(),
    )


def delete_role_record(table: TableProtocol, role_id: str) -> None:
    _delete(table, role_key(role_id))


# ------------------------------------------------------------------------------------- access tokens


def get_access_token(table: TableProtocol | None, token_id: str) -> AccessToken | None:
    payload = _read(table, token_key(token_id), TOKEN_ENTITY)
    return None if payload is None else AccessToken.from_dict(payload)


def iter_access_tokens(table: TableProtocol | None) -> Iterator[AccessToken]:
    for payload in _scan(table, TOKEN_PREFIX, TOKEN_ENTITY).values():
        yield AccessToken.from_dict(payload)


def iter_user_access_tokens(table: TableProtocol | None, user_id: str) -> list[AccessToken]:
    """A person's tokens, newest first."""
    tokens = [token for token in iter_access_tokens(table) if token.user_id == user_id]
    return sorted(tokens, key=lambda token: token.created_at, reverse=True)


def save_access_token(table: TableProtocol, token: AccessToken, *, updated_at: str) -> None:
    _write(
        table,
        token_key(token.token_id),
        entity_type=TOKEN_ENTITY,
        entity_id=token.token_id,
        updated_at=updated_at,
        payload=token.to_dict(),
    )


def delete_access_token(table: TableProtocol, token_id: str) -> None:
    _delete(table, token_key(token_id))


# ------------------------------------------------------------------------------------------ datasets


def get_dataset(table: TableProtocol | None, dataset_id: str) -> Dataset | None:
    payload = _read(table, dataset_key(dataset_id), DATASET_ENTITY)
    return None if payload is None else Dataset.from_dict(payload)


def find_dataset_by_name(table: TableProtocol | None, name: str) -> Dataset | None:
    """The dataset called `name`, through the name index, or None."""
    payload = _read(table, dataset_name_key(name), DATASET_NAME_ENTITY)
    if payload is None:
        return None
    dataset = get_dataset(table, str(payload.get("dataset_id", "")))
    if dataset is None or dataset.name != name.strip().lower():
        return None
    return dataset


def resolve_dataset(table: TableProtocol | None, name_or_id: str) -> Dataset | None:
    """A dataset by its name, as paths and SQL name it, or by its id."""
    if not isinstance(name_or_id, str) or not name_or_id.strip():
        return None
    return find_dataset_by_name(table, name_or_id) or get_dataset(table, name_or_id.strip())


def iter_datasets(table: TableProtocol | None) -> list[Dataset]:
    """Every dataset, by name."""
    datasets = [Dataset.from_dict(payload) for payload in _scan(table, DATASET_PREFIX, DATASET_ENTITY).values()]
    return sorted(datasets, key=lambda dataset: dataset.name)


def save_dataset(table: TableProtocol, dataset: Dataset) -> None:
    _write(
        table,
        dataset_key(dataset.dataset_id),
        entity_type=DATASET_ENTITY,
        entity_id=dataset.dataset_id,
        updated_at=dataset.updated_at,
        payload=dataset.to_dict(),
    )


def delete_dataset_record(table: TableProtocol, dataset_id: str) -> None:
    _delete(table, dataset_key(dataset_id))


def save_dataset_name(table: TableProtocol, name: str, dataset_id: str, *, updated_at: str) -> None:
    normalized = name.strip().lower()
    _write(
        table,
        dataset_name_key(normalized),
        entity_type=DATASET_NAME_ENTITY,
        entity_id=normalized,
        updated_at=updated_at,
        payload={"name": normalized, "dataset_id": dataset_id},
    )


def delete_dataset_name(table: TableProtocol, name: str) -> None:
    _delete(table, dataset_name_key(name))


def get_dataset_grant(table: TableProtocol | None, dataset_id: str, principal_type: str, principal_id: str) -> DatasetGrant | None:
    payload = _read(table, dataset_grant_key(dataset_id, principal_type, principal_id), DATASET_GRANT_ENTITY)
    return None if payload is None else DatasetGrant.from_dict(payload)


def iter_dataset_grants(table: TableProtocol | None, dataset_id: str | None = None) -> list[DatasetGrant]:
    """Every dataset grant, or one dataset's."""
    prefix = DATASET_GRANT_PREFIX if dataset_id is None else dataset_grant_prefix(dataset_id)
    return [DatasetGrant.from_dict(payload) for payload in _scan(table, prefix, DATASET_GRANT_ENTITY).values()]


def save_dataset_grant(table: TableProtocol, grant: DatasetGrant) -> None:
    _write(
        table,
        dataset_grant_key(grant.dataset_id, grant.principal_type, grant.principal_id),
        entity_type=DATASET_GRANT_ENTITY,
        entity_id=f"{grant.dataset_id}:{grant.principal_type}:{grant.principal_id}",
        updated_at=grant.updated_at,
        payload=grant.to_dict(),
    )


def delete_dataset_grant(table: TableProtocol, dataset_id: str, principal_type: str, principal_id: str) -> None:
    _delete(table, dataset_grant_key(dataset_id, principal_type, principal_id))


def get_table_claim(table: TableProtocol | None, dataset_id: str, name: str) -> TableClaim | None:
    payload = _read(table, table_claim_key(dataset_id, name), TABLE_CLAIM_ENTITY)
    return None if payload is None else TableClaim.from_dict(payload)


def iter_table_claims(table: TableProtocol | None, dataset_id: str | None = None) -> list[TableClaim]:
    """Every table-name claim, or one dataset's, by table name."""
    prefix = TABLE_CLAIM_PREFIX if dataset_id is None else table_claim_prefix(dataset_id)
    claims = [TableClaim.from_dict(payload) for payload in _scan(table, prefix, TABLE_CLAIM_ENTITY).values()]
    return sorted(claims, key=lambda claim: (claim.dataset_id, claim.table))


def save_table_claim(table: TableProtocol, claim: TableClaim) -> None:
    _write(
        table,
        table_claim_key(claim.dataset_id, claim.table),
        entity_type=TABLE_CLAIM_ENTITY,
        entity_id=f"{claim.dataset_id}:{claim.table}",
        updated_at=claim.claimed_at,
        payload=claim.to_dict(),
    )


def delete_table_claim(table: TableProtocol, dataset_id: str, name: str) -> None:
    _delete(table, table_claim_key(dataset_id, name))


def get_claim_release(table: TableProtocol | None, materialization_id: str) -> ClaimRelease | None:
    payload = _read(table, claim_release_key(materialization_id), CLAIM_RELEASE_ENTITY)
    return None if payload is None else ClaimRelease.from_dict(payload)


def iter_claim_releases(table: TableProtocol | None) -> list[ClaimRelease]:
    return [ClaimRelease.from_dict(payload) for payload in _scan(table, CLAIM_RELEASE_PREFIX, CLAIM_RELEASE_ENTITY).values()]


def save_claim_release(table: TableProtocol, release: ClaimRelease) -> None:
    _write(
        table,
        claim_release_key(release.materialization_id),
        entity_type=CLAIM_RELEASE_ENTITY,
        entity_id=release.materialization_id,
        updated_at=release.released_at,
        payload=release.to_dict(),
    )


def delete_claim_release(table: TableProtocol, materialization_id: str) -> None:
    _delete(table, claim_release_key(materialization_id))


# ------------------------------------------------------------------------------------------ sessions


def get_session(table: TableProtocol | None, user_id: str, session_id: str) -> Session | None:
    payload = _read(table, session_key(user_id, session_id), SESSION_ENTITY)
    return None if payload is None else Session.from_dict(payload)


def iter_user_sessions(table: TableProtocol | None, user_id: str) -> list[Session]:
    """A person's recorded sessions, newest first."""
    sessions = [
        Session.from_dict(payload)
        for payload in _scan(table, f"{SESSION_PREFIX}{user_id}:", SESSION_ENTITY).values()
    ]
    return sorted(sessions, key=lambda session: session.created_at, reverse=True)


def iter_sessions(table: TableProtocol | None) -> Iterator[Session]:
    for payload in _scan(table, SESSION_PREFIX, SESSION_ENTITY).values():
        yield Session.from_dict(payload)


def save_session(table: TableProtocol, session: Session, *, updated_at: str) -> None:
    _write(
        table,
        session_key(session.user_id, session.session_id),
        entity_type=SESSION_ENTITY,
        entity_id=session.session_id,
        updated_at=updated_at,
        payload=session.to_dict(),
    )


def delete_session_record(table: TableProtocol, user_id: str, session_id: str) -> None:
    _delete(table, session_key(user_id, session_id))


def get_sign_in_guard(table: TableProtocol | None, email: str) -> SignInGuard | None:
    payload = _read(table, sign_in_guard_key(email), SIGN_IN_ENTITY)
    return None if payload is None else SignInGuard.from_dict(payload)


def iter_sign_in_guards(table: TableProtocol | None) -> Iterator[SignInGuard]:
    for payload in _scan(table, SIGN_IN_PREFIX, SIGN_IN_ENTITY).values():
        yield SignInGuard.from_dict(payload)


def save_sign_in_guard(table: TableProtocol, guard: SignInGuard, *, updated_at: str) -> None:
    normalized = normalize_email(guard.email)
    _write(
        table,
        sign_in_guard_key(normalized),
        entity_type=SIGN_IN_ENTITY,
        entity_id=normalized,
        updated_at=updated_at,
        payload=guard.to_dict(),
    )


def delete_sign_in_guard(table: TableProtocol, email: str) -> None:
    _delete(table, sign_in_guard_key(email))


def get_user_activity(table: TableProtocol | None, user_id: str) -> UserActivity | None:
    payload = _read(table, activity_key(user_id), ACTIVITY_ENTITY)
    return None if payload is None else UserActivity.from_dict(payload)


def activity_by_user(table: TableProtocol | None) -> dict[str, UserActivity]:
    activities = (UserActivity.from_dict(payload) for payload in _scan(table, ACTIVITY_PREFIX, ACTIVITY_ENTITY).values())
    return {activity.user_id: activity for activity in activities}


def save_user_activity(table: TableProtocol, activity: UserActivity, *, updated_at: str) -> None:
    _write(
        table,
        activity_key(activity.user_id),
        entity_type=ACTIVITY_ENTITY,
        entity_id=activity.user_id,
        updated_at=updated_at,
        payload=activity.to_dict(),
    )


def delete_user_activity(table: TableProtocol, user_id: str) -> None:
    _delete(table, activity_key(user_id))


# ---------------------------------------------------------------------------------- request outcomes


def get_request_outcome(table: TableProtocol | None, request_id: str) -> IdentityRequestOutcome | None:
    payload = _read(table, request_outcome_key(request_id), REQUEST_ENTITY)
    return None if payload is None else IdentityRequestOutcome.from_dict(payload)


def iter_request_outcomes(table: TableProtocol | None) -> Iterator[IdentityRequestOutcome]:
    for payload in _scan(table, REQUEST_PREFIX, REQUEST_ENTITY).values():
        yield IdentityRequestOutcome.from_dict(payload)


def save_request_outcome(table: TableProtocol, outcome: IdentityRequestOutcome) -> None:
    _write(
        table,
        request_outcome_key(outcome.request_id),
        entity_type=REQUEST_ENTITY,
        entity_id=outcome.request_id,
        updated_at=outcome.completed_at,
        payload=outcome.to_dict(),
    )


def delete_request_outcome(table: TableProtocol, request_id: str) -> None:
    _delete(table, request_outcome_key(request_id))


# ------------------------------------------------------------------------------------------- audit


def append_audit_entry(table: TableProtocol, entry: AuditEntry) -> None:
    _write(
        table,
        audit_key(entry),
        entity_type=AUDIT_ENTITY,
        entity_id=entry.entry_id,
        updated_at=entry.occurred_at,
        payload=entry.to_dict(),
    )


def iter_audit_entries(
    table: TableProtocol | None,
    *,
    newest: str,
    oldest: str,
    before: str | None = None,
) -> Iterator[tuple[str, AuditEntry]]:
    """Audit entries from `newest` back to `oldest` (ISO days or instants), newest first.

    Reads one day's prefix at a time, so a page near the present touches only the days it shows.
    `before` is a key from an earlier page; entries at or after it are skipped.
    """
    day = _parse_day(newest)
    stop = _parse_day(oldest)
    if day is None or stop is None:
        return
    while day >= stop:
        prefix = f"{AUDIT_PREFIX}{day.isoformat()}"
        for key, payload in sorted(_scan(table, prefix, AUDIT_ENTITY).items(), reverse=True):
            if before is not None and key >= before:
                continue
            yield key, AuditEntry.from_dict(payload)
        day -= timedelta(days=1)


def delete_audit_entries_before(table: TableProtocol, cutoff: str, *, limit: int) -> int:
    """Remove audit entries older than `cutoff`, at most `limit` of them; returns how many."""
    stale = sorted(key for key, _ in table.prefix_scan(AUDIT_PREFIX) if key[len(AUDIT_PREFIX):] < cutoff)
    for key in stale[:limit]:
        _delete(table, key)
    return min(len(stale), limit)


def _parse_day(value: str) -> Any:
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC).date()
    except ValueError:
        try:
            return datetime.strptime(value[:10], "%Y-%m-%d").date()
        except ValueError:
            return None
