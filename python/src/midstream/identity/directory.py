"""The whole directory in memory, to check a change before it is made.

Rules that span records — one account per email, never losing the last administrator, never
granting access you do not hold — are checked against the directory as it would be after a
change. The directory is small (people, groups, roles, datasets and their grants), and changes are
rare, so the identity agent loads it once per change rather than maintaining indexes that could
drift.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace

from midstream.identity.permissions import (
    ALL_USERS_GROUP_ID,
    GUEST_GROUP_ID,
)
from midstream.identity.principals import Grants, resolve_grants, user_group_ids
from midstream.models.identity import Dataset, DatasetGrant, Group, Role, User
from midstream.storage.identity import (
    iter_dataset_grants,
    iter_datasets,
    iter_groups,
    iter_roles,
    iter_users,
    normalize_email,
)
from midstream.storage.table import TableProtocol


@dataclass(frozen=True, slots=True)
class DirectoryView:
    """Users, groups and roles by id. `with_*` and `without_*` return a changed copy."""

    users: dict[str, User] = field(default_factory=dict)
    groups: dict[str, Group] = field(default_factory=dict)
    roles: dict[str, Role] = field(default_factory=dict)
    datasets: dict[str, Dataset] = field(default_factory=dict)
    #: Every dataset grant, keyed by (dataset id, user or group, principal id).
    dataset_grants: dict[tuple[str, str, str], DatasetGrant] = field(default_factory=dict)

    @classmethod
    def load(cls, table: TableProtocol | None) -> "DirectoryView":
        return cls(
            users={user.user_id: user for user in iter_users(table)},
            groups={group.group_id: group for group in iter_groups(table)},
            roles={role.role_id: role for role in iter_roles(table)},
            datasets={dataset.dataset_id: dataset for dataset in iter_datasets(table)},
            dataset_grants={(grant.dataset_id, grant.principal_type, grant.principal_id): grant for grant in iter_dataset_grants(table)},
        )

    def grants_for_groups(self, group_ids: list[str] | tuple[str, ...], *, user_ids: tuple[str, ...] = ()) -> Grants:
        """What a set of groups grants, with the grants they hold on datasets."""
        return resolve_grants(
            group_ids,
            group=self.groups.get,
            role=self.roles.get,
            dataset_grants=self.dataset_grants.values(),
            user_ids=user_ids,
        )

    def grants_for_user(self, user: User) -> Grants:
        return self.grants_for_groups(user_group_ids(user), user_ids=(user.user_id,))

    def grants_of_principal(self, principal_id: str) -> Grants:
        """What a person, or a group's members through it, hold on datasets and across the deployment."""
        user = self.users.get(principal_id)
        if user is not None:
            return self.grants_for_user(user)
        return self.grants_for_groups([principal_id])

    def dataset_by_name(self, name: str) -> Dataset | None:
        wanted = name.strip().lower()
        return next((dataset for dataset in self.datasets.values() if dataset.name == wanted), None)

    def grants_on(self, dataset_id: str) -> list[DatasetGrant]:
        """One dataset's grants."""
        return [grant for (granted_on, _, _), grant in self.dataset_grants.items() if granted_on == dataset_id]

    def grants_for_roles(self, role_ids: list[str] | tuple[str, ...]) -> Grants:
        """What a set of roles grants, as if one group held them all."""
        holder = Group(
            group_id="__roles__",
            name="__roles__",
            role_ids=list(role_ids),
            created_at="-",
            created_by="-",
            updated_at="-",
            updated_by="-",
        )
        return resolve_grants([holder.group_id], group=lambda _: holder, role=self.roles.get)

    def user_by_email(self, email: str) -> User | None:
        normalized = normalize_email(email)
        return next((user for user in self.users.values() if normalize_email(user.email) == normalized), None)

    def group_by_name(self, name: str) -> Group | None:
        lowered = name.strip().lower()
        return next((group for group in self.groups.values() if group.name.strip().lower() == lowered), None)

    def role_by_name(self, name: str) -> Role | None:
        lowered = name.strip().lower()
        return next((role for role in self.roles.values() if role.name.strip().lower() == lowered), None)

    def members(self, group_id: str) -> list[User]:
        """The people in a group: everyone signed in for All Users, nobody for Guest."""
        if group_id == GUEST_GROUP_ID:
            return []
        if group_id == ALL_USERS_GROUP_ID:
            return list(self.users.values())
        return [user for user in self.users.values() if group_id in user.group_ids]

    def administrators(self) -> set[str]:
        """Active people who hold every permission."""
        return {
            user.user_id
            for user in self.users.values()
            if user.active and self.grants_for_user(user).all_permissions
        }

    def with_user(self, user: User) -> "DirectoryView":
        return replace(self, users={**self.users, user.user_id: user})

    def without_user(self, user_id: str) -> "DirectoryView":
        return replace(self, users={key: value for key, value in self.users.items() if key != user_id})

    def with_group(self, group: Group) -> "DirectoryView":
        return replace(self, groups={**self.groups, group.group_id: group})

    def without_group(self, group_id: str) -> "DirectoryView":
        return replace(self, groups={key: value for key, value in self.groups.items() if key != group_id})

    def with_role(self, role: Role) -> "DirectoryView":
        return replace(self, roles={**self.roles, role.role_id: role})

    def without_role(self, role_id: str) -> "DirectoryView":
        return replace(self, roles={key: value for key, value in self.roles.items() if key != role_id})

    def with_dataset_grant(self, grant: DatasetGrant) -> "DirectoryView":
        return replace(self, dataset_grants={**self.dataset_grants, (grant.dataset_id, grant.principal_type, grant.principal_id): grant})

    def without_dataset_grant(self, dataset_id: str, principal_type: str, principal_id: str) -> "DirectoryView":
        key = (dataset_id, principal_type, principal_id)
        return replace(self, dataset_grants={item: value for item, value in self.dataset_grants.items() if item != key})
