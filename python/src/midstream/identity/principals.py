"""Who is making a request, and what they may do.

A principal is a signed-in person, a guest, or the console's service credential. A person's
permissions are the union of the roles of every group they are in — the groups they were added
to, plus All Users, plus Guest, since signing in never takes access away — with each permission's
implications added. On a dataset they also hold what the roles granted on it give them, to them or
to any of their groups (dataset grants). The resolution is cached per directory revision, so a
change to any person, group, role or grant applies to the very next request.

```text
global(person)      = every role of Guest, All Users and the person's groups
on(person, dataset) = global(person) + every role granted on the dataset to them or a group of theirs
token(t, dataset)   = t.permissions ∩ on(owner, dataset), limited to t.datasets when it names some
```
"""

from __future__ import annotations

from collections import OrderedDict
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any

from midstream.identity.permissions import (
    ALL_PERMISSIONS,
    ALL_USERS_GROUP_ID,
    DATASET_PERMISSION_KEYS,
    GUEST_GROUP_ID,
    PERMISSION_KEYS,
    SERVICE_PERMISSIONS,
    expand_permissions,
    is_dataset_permission,
)
from midstream.models.identity import DatasetGrant, Group, Role, User
from midstream.storage.identity import (
    get_directory_state,
    get_group,
    get_role,
    iter_dataset_grants,
)
from midstream.storage.table import TableProtocol

#: `token` is a person's access token: their identity, narrowed to what the token carries.
PRINCIPAL_KINDS = ("user", "guest", "service", "token")

#: What a guest's changes would be attributed to, were a guest ever allowed to make one.
GUEST_ATTRIBUTION = "guest"
SERVICE_ATTRIBUTION = "service:console"

_NOTHING: frozenset[str] = frozenset()


@dataclass(frozen=True, slots=True)
class Grants:
    """What a set of groups amounts to: the groups and roles involved and their permissions.

    `permissions` hold across the deployment: a dataset permission among them holds on every
    dataset. `datasets` adds, by dataset id, the dataset permissions granted on single datasets.
    `dataset_limit` confines an access token's dataset permissions to the datasets it names.
    """

    group_ids: tuple[str, ...]
    role_ids: tuple[str, ...]
    permissions: frozenset[str]
    all_permissions: bool = False
    datasets: Mapping[str, frozenset[str]] = field(default_factory=dict)
    dataset_limit: frozenset[str] | None = None

    def on(self, dataset_id: str) -> frozenset[str]:
        """Every dataset permission held on one dataset."""
        if self.dataset_limit is not None and dataset_id not in self.dataset_limit:
            return _NOTHING
        if self.all_permissions:
            return DATASET_PERMISSION_KEYS
        return frozenset((self.permissions & DATASET_PERMISSION_KEYS) | self.datasets.get(dataset_id, _NOTHING))

    def holds(self, permission: str, dataset_id: str | None = None) -> bool:
        """Whether a permission is held: on a dataset, when one is named and the permission is
        about datasets; across the deployment otherwise."""
        if dataset_id is not None and is_dataset_permission(permission):
            return permission in self.on(dataset_id)
        if is_dataset_permission(permission) and self.dataset_limit is not None:
            # A token limited to some datasets holds nothing on every dataset.
            return False
        return self.all_permissions or permission in self.permissions

    def holds_anywhere(self, permission: str) -> bool:
        """Whether a permission is held across the deployment or on at least one dataset."""
        if self.holds(permission):
            return True
        if not is_dataset_permission(permission):
            return False
        candidates = self.dataset_limit if self.dataset_limit is not None else self.datasets.keys()
        return any(permission in self.on(dataset_id) for dataset_id in candidates)

    def readable_datasets(self) -> frozenset[str] | None:
        """The datasets these grants read anything of, by id; None when that is every dataset."""
        if self.dataset_limit is None and (self.all_permissions or "dataset.view_schema" in self.permissions):
            return None
        candidates = self.dataset_limit if self.dataset_limit is not None else self.datasets.keys()
        return frozenset(dataset_id for dataset_id in candidates if "dataset.view_schema" in self.on(dataset_id))

    def covers(self, other: "Grants") -> bool:
        """Whether these grants include every permission of `other`, on every dataset, so they may
        hand it out (or take it away)."""
        if self.all_permissions:
            return True
        if other.all_permissions:
            return False
        if not other.permissions <= self.permissions:
            return False
        return all(permissions <= self.on(dataset_id) for dataset_id, permissions in other.datasets.items())

    def missing(self, other: "Grants") -> tuple[frozenset[str], dict[str, frozenset[str]]]:
        """What of `other` these grants lack: across the deployment, and on each dataset."""
        if self.all_permissions:
            return _NOTHING, {}
        deployment = frozenset(other.permissions - self.permissions)
        datasets = {
            dataset_id: frozenset(permissions - self.on(dataset_id))
            for dataset_id, permissions in other.datasets.items()
            if not permissions <= self.on(dataset_id)
        }
        return deployment, datasets


@dataclass(frozen=True, slots=True)
class Principal:
    """The caller of one request, with everything authorization needs about them."""

    kind: str
    grants: Grants
    user_id: str | None = None
    email: str | None = None
    first_name: str | None = None
    last_name: str | None = None
    session_id: str | None = None
    session_expires_at: int | None = None
    expires_at: int | None = None
    issued_at: int | None = None
    signed_in_at: int | None = None
    sign_in_method: str | None = None
    password_change_required: bool = False
    extra: dict[str, Any] = field(default_factory=dict)

    @property
    def authenticated(self) -> bool:
        return self.kind == "user"

    @property
    def permissions(self) -> frozenset[str]:
        return self.grants.permissions

    @property
    def display_name(self) -> str:
        if self.kind in ("user", "token"):
            return f"{self.first_name or ''} {self.last_name or ''}".strip() or (self.email or "")
        return "Guest" if self.kind == "guest" else "Console service"

    @property
    def attribution(self) -> str:
        """What changes this principal makes are recorded as: an email, for a person or their token."""
        if self.kind in ("user", "token"):
            return self.email or self.user_id or GUEST_ATTRIBUTION
        return GUEST_ATTRIBUTION if self.kind == "guest" else SERVICE_ATTRIBUTION

    @property
    def access_token_id(self) -> str | None:
        """The access token this request came with, for a `token` principal."""
        value = self.extra.get("access_token_id")
        return value if isinstance(value, str) else None

    def can(self, permission: str, dataset_id: str | None = None) -> bool:
        """Whether this principal may do something: on a dataset, for a dataset permission and a
        dataset named by id; across the deployment otherwise."""
        return self.grants.holds(permission, dataset_id)

    def can_anywhere(self, permission: str) -> bool:
        """Whether this principal holds a permission on the deployment or on any one dataset."""
        return self.grants.holds_anywhere(permission)

    def describe(self) -> dict[str, Any]:
        """The principal as `GET /auth/session` reports it.

        `dataset_permissions` lists, by dataset id, what grants on a dataset add to what the
        principal holds everywhere; `dataset_limit`, the datasets an access token is confined to.
        """
        return {
            "kind": self.kind,
            "user_id": self.user_id,
            "email": self.email,
            "first_name": self.first_name,
            "last_name": self.last_name,
            "display_name": self.display_name,
            "group_ids": list(self.grants.group_ids),
            "role_ids": list(self.grants.role_ids),
            "permissions": sorted(self.grants.permissions),
            "all_permissions": self.grants.all_permissions,
            "dataset_permissions": {
                dataset_id: sorted(permissions)
                for dataset_id, permissions in sorted(self.grants.datasets.items())
                if permissions
            },
            "dataset_limit": None if self.grants.dataset_limit is None else sorted(self.grants.dataset_limit),
            "password_change_required": self.password_change_required,
        }


def principal_from_description(payload: Mapping[str, Any]) -> Principal:
    """A principal again from what `describe()` reported of it - `GET /auth/session`'s `principal`.

    For a service that checks callers with the control plane rather than with a directory table of
    its own (the tail app): `can()` answers on the rebuilt principal exactly as on the one the control
    plane resolved, on every dataset, a token's dataset limit included.
    """
    datasets = payload.get("dataset_permissions") or {}
    limit = payload.get("dataset_limit")
    grants = Grants(
        group_ids=tuple(str(item) for item in payload.get("group_ids") or ()),
        role_ids=tuple(str(item) for item in payload.get("role_ids") or ()),
        permissions=frozenset(str(item) for item in payload.get("permissions") or ()),
        all_permissions=bool(payload.get("all_permissions", False)),
        datasets={str(dataset_id): frozenset(str(item) for item in permissions) for dataset_id, permissions in datasets.items()},
        dataset_limit=None if limit is None else frozenset(str(item) for item in limit),
    )
    kind = str(payload.get("kind") or "guest")
    return Principal(
        kind=kind if kind in PRINCIPAL_KINDS else "guest",
        grants=grants,
        user_id=payload.get("user_id"),
        email=payload.get("email"),
        first_name=payload.get("first_name"),
        last_name=payload.get("last_name"),
        password_change_required=bool(payload.get("password_change_required", False)),
    )


def resolve_grants(
    group_ids: Iterable[str],
    *,
    group: Callable[[str], Group | None],
    role: Callable[[str], Role | None],
    dataset_grants: Iterable[DatasetGrant] = (),
    user_ids: Iterable[str] = (),
) -> Grants:
    """The roles and permissions a set of groups grants. Unknown groups and roles grant nothing.

    `dataset_grants` are every grant there is; those held by one of the groups, or by one of
    `user_ids` (a person's own), add their dataset permissions on their datasets.
    """
    groups: list[Group] = []
    seen: set[str] = set()
    for group_id in group_ids:
        if group_id in seen:
            continue
        seen.add(group_id)
        found = group(group_id)
        if found is not None:
            groups.append(found)
    role_ids: list[str] = []
    for found in groups:
        for role_id in found.role_ids:
            if role_id not in role_ids:
                role_ids.append(role_id)
    granted: list[str] = []
    held: list[str] = []
    for role_id in role_ids:
        found_role = role(role_id)
        if found_role is not None:
            held.append(role_id)
            granted.extend(found_role.permissions)
    holders = {("group", found.group_id) for found in groups} | {("user", user_id) for user_id in user_ids}
    return Grants(
        group_ids=tuple(found.group_id for found in groups),
        role_ids=tuple(held),
        permissions=expand_permissions(granted),
        all_permissions=ALL_PERMISSIONS in granted,
        datasets=dataset_permissions(dataset_grants, holders, role=role),
    )


def dataset_permissions(
    grants: Iterable[DatasetGrant],
    holders: Iterable[tuple[str, str]],
    *,
    role: Callable[[str], Role | None],
) -> dict[str, frozenset[str]]:
    """The dataset permissions each dataset's grants give the holders - `(user or group, id)` - by
    dataset id.

    Only dataset permissions count, with what they imply (always dataset permissions too): a grant
    on a dataset never gives anything beyond it.
    """
    wanted = set(holders)
    granted: dict[str, set[str]] = {}
    for grant in grants:
        if (grant.principal_type, grant.principal_id) not in wanted:
            continue
        for role_id in grant.role_ids:
            found = role(role_id)
            if found is not None:
                granted.setdefault(grant.dataset_id, set()).update(found.permissions)
    result: dict[str, frozenset[str]] = {}
    for dataset_id, permissions in granted.items():
        expanded = expand_permissions(permissions) & DATASET_PERMISSION_KEYS
        if expanded:
            result[dataset_id] = frozenset(expanded)
    return result


def grants_for_groups(table: TableProtocol | None, group_ids: Iterable[str], *, user_ids: Iterable[str] = ()) -> Grants:
    """The roles and permissions a set of groups grants, read from the directory, dataset grants included."""
    return resolve_grants(
        group_ids,
        group=lambda group_id: get_group(table, group_id),
        role=lambda role_id: get_role(table, role_id),
        dataset_grants=iter_dataset_grants(table),
        user_ids=user_ids,
    )


def grants_for_role(role: Role) -> Grants:
    """What one role grants on its own."""
    return Grants(
        group_ids=(),
        role_ids=(role.role_id,),
        permissions=expand_permissions(role.permissions),
        all_permissions=ALL_PERMISSIONS in role.permissions,
    )


def user_group_ids(user: User) -> tuple[str, ...]:
    """Every group a signed-in person is in: Guest, All Users and the ones they were added to."""
    return (GUEST_GROUP_ID, ALL_USERS_GROUP_ID, *[group_id for group_id in user.group_ids if group_id not in (GUEST_GROUP_ID, ALL_USERS_GROUP_ID)])


def grants_for_user(table: TableProtocol | None, user: User) -> Grants:
    return grants_for_groups(table, user_group_ids(user), user_ids=(user.user_id,))


# Resolved grants by (who, directory revision). Bounded, and dropped wholesale on a new revision.
_CACHE: OrderedDict[tuple[str, int], Grants] = OrderedDict()
_CACHE_LIMIT = 2048


def cached_grants(table: TableProtocol | None, user: User | None) -> Grants:
    """`grants_for_user` (or a guest's grants), reused until the directory changes."""
    revision = get_directory_state(table).revision
    key = (user.user_id if user is not None else "", revision)
    cached = _CACHE.get(key)
    if cached is not None:
        _CACHE.move_to_end(key)
        return cached
    grants = grants_for_groups(table, (GUEST_GROUP_ID,)) if user is None else grants_for_user(table, user)
    if _CACHE and next(reversed(_CACHE))[1] != revision:
        _CACHE.clear()
    _CACHE[key] = grants
    while len(_CACHE) > _CACHE_LIMIT:
        _CACHE.popitem(last=False)
    return grants


def clear_grants_cache() -> None:
    _CACHE.clear()


def guest_principal(table: TableProtocol | None) -> Principal:
    return Principal(kind="guest", grants=cached_grants(table, None))


def service_principal() -> Principal:
    return Principal(
        kind="service",
        grants=Grants(group_ids=(), role_ids=(), permissions=SERVICE_PERMISSIONS & PERMISSION_KEYS),
    )
