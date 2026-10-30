"""Every permission the application defines, and the roles and groups it ships with.

A permission names one thing a person may see or do. Roles bundle permissions into reusable sets;
groups hold roles, and people hold groups. Nothing grants a permission to a person directly, so
where any access comes from is always a group and a role.

A permission has a **scope**. A `deployment` permission is about the whole deployment: seeing
materializations, managing users. A `dataset` permission is about the tables of a dataset: reading
their schemas and data, adding and dropping them, managing the dataset. A role holding dataset
permissions can be granted on one dataset (a dataset grant), where it gives them on that dataset
alone; a group that holds it gives them on every dataset. A permission implies only permissions of
its own scope, so a grant on one dataset never reaches beyond it.

The labels and descriptions below are shown in the console's role editor as they are written.
Adding a permission is a change to this module, to the endpoints that check it, and to whichever
built-in roles should hold it; see `.claude/skills/adding-permissions`.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

#: Granted by a role that holds every permission, including ones a later release adds.
ALL_PERMISSIONS = "*"

#: What a permission is about: the whole deployment, or the tables of one dataset.
DEPLOYMENT_SCOPE = "deployment"
DATASET_SCOPE = "dataset"
PERMISSION_SCOPES = (DEPLOYMENT_SCOPE, DATASET_SCOPE)


@dataclass(frozen=True, slots=True)
class PermissionCategory:
    """A heading in the role editor."""

    category_id: str
    label: str


@dataclass(frozen=True, slots=True)
class PermissionDefinition:
    """One permission: its key, where the role editor lists it, and what it allows.

    `implies` lists permissions this one cannot be used without — managing users needs seeing
    them — so a role that grants this one grants those too; they are always of the same scope.
    `danger` marks a permission whose use can remove data or lock people out. `scope` says whether
    it is about the deployment or about one dataset (the module docstring).
    """

    key: str
    category: str
    label: str
    description: str
    implies: tuple[str, ...] = ()
    danger: bool = False
    scope: str = DEPLOYMENT_SCOPE

    def describe(self) -> dict[str, Any]:
        """The permission as the API reports it."""
        return {
            "key": self.key,
            "category": self.category,
            "label": self.label,
            "description": self.description,
            "implies": list(self.implies),
            "danger": self.danger,
            "scope": self.scope,
        }


PERMISSION_CATEGORIES: tuple[PermissionCategory, ...] = (
    PermissionCategory("users", "User management"),
    PermissionCategory("materializations", "Materializations"),
    PermissionCategory("datasets", "Datasets"),
    PermissionCategory("data", "Data"),
    PermissionCategory("kafka", "Kafka"),
    PermissionCategory("configuration", "Configuration"),
    PermissionCategory("monitoring", "Monitoring"),
)

PERMISSIONS: tuple[PermissionDefinition, ...] = (
    PermissionDefinition(
        "users.view",
        "users",
        "View users page with emails",
        "See every user with their email, groups, status and last activity.",
    ),
    PermissionDefinition(
        "users.view_details",
        "users",
        "View account details",
        "Open a user's account: their groups, effective permissions, sessions and sign-in activity.",
        implies=("users.view",),
    ),
    PermissionDefinition(
        "users.manage",
        "users",
        "Manage users",
        "Add users, edit their details and groups, set their passwords, sign them out, and unlock, "
        "deactivate, reactivate or delete them. Only for people whose access you hold yourself.",
        implies=("users.view_details", "groups.view"),
        danger=True,
    ),
    PermissionDefinition(
        "groups.view",
        "users",
        "View groups",
        "See groups, their members and the roles they grant.",
    ),
    PermissionDefinition(
        "groups.manage",
        "users",
        "Manage groups",
        "Create, rename and delete groups and change their members and roles, for groups whose "
        "access you hold yourself.",
        implies=("groups.view", "roles.view", "users.view"),
    ),
    PermissionDefinition(
        "roles.view",
        "users",
        "View roles",
        "See every role and the permissions it grants.",
    ),
    PermissionDefinition(
        "roles.manage",
        "users",
        "Manage roles",
        "Create, edit and delete custom roles, with permissions you hold yourself.",
        implies=("roles.view",),
    ),
    PermissionDefinition(
        "materializations.view",
        "materializations",
        "View materializations",
        "See the list of materializations and their status, on the Overview and Materializations "
        "pages.",
    ),
    PermissionDefinition(
        "materializations.view_details",
        "materializations",
        "View materialization details",
        "Open a materialization: its settings, status, runtime, throughput and consumer lag.",
        implies=("materializations.view",),
    ),
    PermissionDefinition(
        "materializations.create",
        "materializations",
        "Create materializations",
        "Create materializations. Choosing a source topic from a list also needs Browse Kafka "
        "topics, and putting its table in a dataset needs Add and drop tables on the dataset.",
        implies=("materializations.view_details",),
    ),
    PermissionDefinition(
        "materializations.edit",
        "materializations",
        "Edit materializations",
        "Change a materialization's name, replica count and topics. Adding or removing a topic "
        "also needs Add and drop tables on the dataset of its table.",
        implies=("materializations.view_details",),
    ),
    PermissionDefinition(
        "materializations.stop",
        "materializations",
        "Stop materializations",
        "Stop a materialization's runtime. Rows it has staged but not yet written are read again on resume.",
        implies=("materializations.view_details",),
    ),
    PermissionDefinition(
        "materializations.resume",
        "materializations",
        "Resume materializations",
        "Start a stopped materialization's runtime again, or retry a failed one.",
        implies=("materializations.view_details",),
    ),
    PermissionDefinition(
        "materializations.delete",
        "materializations",
        "Delete materializations",
        "Delete materializations with their runtimes and Kafka topics and, unless turned off in "
        "Settings, their tables and the data in them. Also needs Add and drop tables on every "
        "dataset that holds one of their tables.",
        implies=("materializations.view_details",),
        danger=True,
    ),
    PermissionDefinition(
        "materializations.maintain",
        "materializations",
        "Run table maintenance",
        "Start a materialization table's maintenance now: compaction, snapshot expiry or orphan "
        "file cleanup. Runtimes also run it on their own schedule.",
        implies=("materializations.view_details",),
    ),
    PermissionDefinition(
        "datasets.view",
        "datasets",
        "View datasets",
        "See every dataset: its name, description, owners and the names of its tables. Datasets "
        "you cannot read are marked locked.",
    ),
    PermissionDefinition(
        "datasets.create",
        "datasets",
        "Create datasets",
        "Create datasets, on the Datasets page or while choosing a materialization's topics. "
        "Whoever creates a dataset owns it.",
        implies=("datasets.view",),
    ),
    PermissionDefinition(
        "dataset.view_schema",
        "datasets",
        "View schemas",
        "See the schemas of a dataset's tables, their versions and what changed between them.",
        scope=DATASET_SCOPE,
    ),
    PermissionDefinition(
        "dataset.write",
        "datasets",
        "Add and drop tables",
        "Put tables in a dataset, remove or drop its tables, and delete materializations that "
        "have tables in it.",
        implies=("dataset.view_schema",),
        scope=DATASET_SCOPE,
    ),
    PermissionDefinition(
        "dataset.manage",
        "datasets",
        "Manage datasets",
        "Edit a dataset's description, decide who may use it - with access you hold yourself - "
        "and delete it once it holds no tables.",
        implies=("dataset.view_schema",),
        scope=DATASET_SCOPE,
    ),
    PermissionDefinition(
        "dataset.protect",
        "datasets",
        "Choose data protection",
        "Choose how a materialization writing tables into the dataset masks sensitive data, when "
        "that differs from the deployment's default - masking less, more, or otherwise - at its "
        "creation or by an edit. Needed on every dataset the materialization writes into.",
        implies=("dataset.view_schema",),
        danger=True,
        scope=DATASET_SCOPE,
    ),
    PermissionDefinition(
        "data.query",
        "data",
        "Query data",
        "Browse the rows of a dataset's tables and run SQL over them in the Data explorer.",
        implies=("dataset.view_schema",),
        scope=DATASET_SCOPE,
    ),
    PermissionDefinition(
        "data.export",
        "data",
        "Export data",
        "Download query results as CSV, JSON or Parquet.",
        implies=("data.query",),
        scope=DATASET_SCOPE,
    ),
    PermissionDefinition(
        "catalog.read",
        "data",
        "Connect external tools to the Iceberg catalog",
        "Read a dataset's tables from engines such as Spark, Trino, DuckDB or PyIceberg through "
        "the Iceberg REST catalog, and create access tokens for them.",
        implies=("dataset.view_schema",),
        scope=DATASET_SCOPE,
    ),
    PermissionDefinition(
        "catalog.write",
        "data",
        "Commit to tables from external tools",
        "Change a dataset's tables from an external engine through the Iceberg REST catalog. A "
        "commit can rewrite or remove data the runtime wrote.",
        implies=("catalog.read",),
        danger=True,
        scope=DATASET_SCOPE,
    ),
    PermissionDefinition(
        "topics.view",
        "kafka",
        "Browse Kafka topics",
        "List the cluster's topics and analyze them, as when choosing a source topic.",
    ),
    PermissionDefinition(
        "settings.view",
        "configuration",
        "View settings",
        "See the platform, runtime and console settings, the deployment's settings and their "
        "change history.",
    ),
    PermissionDefinition(
        "settings.manage",
        "configuration",
        "Manage settings",
        "Change and reset settings, including the security settings and ones that can remove data. "
        "The data protection defaults need Manage data protection defaults too.",
        implies=("settings.view",),
        danger=True,
    ),
    PermissionDefinition(
        "settings.manage_protection",
        "configuration",
        "Manage data protection defaults",
        "Change which kinds of sensitive data new materializations mask by default, and how. "
        "Manage settings does not include it, and changing the defaults needs both.",
        implies=("settings.view",),
        danger=True,
    ),
    PermissionDefinition(
        "audit.view",
        "monitoring",
        "View audit log",
        "See sign-ins, failed sign-ins and every change to users, groups, roles and datasets.",
    ),
)

PERMISSION_KEYS: frozenset[str] = frozenset(definition.key for definition in PERMISSIONS)
#: The permissions that are about one dataset at a time (`DATASET_SCOPE`).
DATASET_PERMISSION_KEYS: frozenset[str] = frozenset(
    definition.key for definition in PERMISSIONS if definition.scope == DATASET_SCOPE
)
_BY_KEY = {definition.key: definition for definition in PERMISSIONS}


def permission_definition(key: str) -> PermissionDefinition | None:
    """The catalog entry for a permission, or None when there is no such permission."""
    return _BY_KEY.get(key)


def is_dataset_permission(key: str) -> bool:
    """Whether a permission is about one dataset at a time, and so can be held on one."""
    return key in DATASET_PERMISSION_KEYS


def dataset_grantable(permissions: Iterable[str]) -> bool:
    """Whether a role of these permissions can be granted on a dataset: every one of them is a
    dataset permission. `*` cannot be: it is the whole deployment."""
    held = list(permissions)
    return bool(held) and all(is_dataset_permission(key) for key in held)


def expand_permissions(granted: Iterable[str]) -> frozenset[str]:
    """Every permission `granted` amounts to: itself and everything it implies.

    `*` expands to every permission this release defines. Keys this release does not know —
    stored by a newer release, perhaps — are dropped rather than granted.
    """
    pending = list(granted)
    if ALL_PERMISSIONS in pending:
        return PERMISSION_KEYS
    expanded: set[str] = set()
    while pending:
        key = pending.pop()
        definition = _BY_KEY.get(key)
        if definition is None or key in expanded:
            continue
        expanded.add(key)
        pending.extend(definition.implies)
    return frozenset(expanded)


@dataclass(frozen=True, slots=True)
class BuiltInRole:
    """A role every deployment has. Defined here, so upgrades can give it new permissions."""

    role_id: str
    name: str
    description: str
    permissions: tuple[str, ...]


BUILT_IN_ROLES: tuple[BuiltInRole, ...] = (
    BuiltInRole(
        "administrator",
        "Administrator",
        "Every permission, including permissions added in later releases.",
        (ALL_PERMISSIONS,),
    ),
    BuiltInRole(
        "user-manager",
        "User Manager",
        "Adds and manages users and group membership, granting only access it holds itself, and "
        "reads the audit log.",
        ("users.manage", "groups.manage", "roles.view", "audit.view"),
    ),
    BuiltInRole(
        "operator",
        "Operator",
        "Creates, edits, stops, resumes and deletes materializations, runs their table "
        "maintenance, creates datasets and browses Kafka topics. Putting a table in a dataset, or "
        "removing one, also needs Add and drop tables on the dataset.",
        (
            "materializations.create",
            "materializations.edit",
            "materializations.stop",
            "materializations.resume",
            "materializations.delete",
            "materializations.maintain",
            "datasets.create",
            "topics.view",
        ),
    ),
    BuiltInRole(
        "viewer",
        "Viewer",
        "Sees materializations with their details, and every dataset. Reading a dataset's "
        "schemas and data needs a role on the dataset.",
        ("materializations.view_details", "datasets.view"),
    ),
    BuiltInRole(
        "status-viewer",
        "Status Viewer",
        "Sees the list of materializations and their status.",
        ("materializations.view",),
    ),
    BuiltInRole(
        "dataset-reader",
        "Dataset Reader",
        "Reads a dataset: sees its schemas, queries and exports its data, and reads it from "
        "external tools through the Iceberg catalog.",
        ("dataset.view_schema", "data.query", "data.export", "catalog.read"),
    ),
    BuiltInRole(
        "dataset-writer",
        "Dataset Writer",
        "Reads a dataset, and adds and drops its tables.",
        ("dataset.view_schema", "data.query", "data.export", "catalog.read", "dataset.write"),
    ),
    BuiltInRole(
        "dataset-owner",
        "Dataset Owner",
        "Reads a dataset, adds and drops its tables, and manages it: its description, who may use "
        "it, how materializations writing into it mask sensitive data, and deleting it once it is "
        "empty.",
        (
            "dataset.view_schema",
            "data.query",
            "data.export",
            "catalog.read",
            "dataset.write",
            "dataset.manage",
            "dataset.protect",
        ),
    ),
)

BUILT_IN_ROLE_IDS: frozenset[str] = frozenset(role.role_id for role in BUILT_IN_ROLES)
_ROLES_BY_ID = {role.role_id: role for role in BUILT_IN_ROLES}

ADMINISTRATOR_ROLE_ID = "administrator"
#: What a new dataset grants: its creator owns it, and everyone signed in reads it.
DATASET_OWNER_ROLE_ID = "dataset-owner"
DATASET_READER_ROLE_ID = "dataset-reader"


def built_in_role(role_id: str) -> BuiltInRole | None:
    """The built-in role with this id, or None."""
    return _ROLES_BY_ID.get(role_id)


#: How a group gets its members. Only `assigned` groups have members chosen by people.
GROUP_MEMBERSHIPS = ("assigned", "authenticated", "guests")


@dataclass(frozen=True, slots=True)
class BuiltInGroup:
    """A group every deployment has. Its roles may change; its name, membership and id may not."""

    group_id: str
    name: str
    description: str
    membership: str
    role_ids: tuple[str, ...]
    #: Roles the group must always hold, so the group cannot be emptied of what it exists for.
    locked_role_ids: tuple[str, ...] = ()


GUEST_GROUP_ID = "guest"
ALL_USERS_GROUP_ID = "all-users"
ADMIN_GROUP_ID = "admin"

BUILT_IN_GROUPS: tuple[BuiltInGroup, ...] = (
    BuiltInGroup(
        GUEST_GROUP_ID,
        "Guest",
        "Anyone who has not signed in. Signed-in people keep what guests can do.",
        "guests",
        ("status-viewer",),
    ),
    BuiltInGroup(
        ALL_USERS_GROUP_ID,
        "All Users",
        "Everyone who has signed in.",
        "authenticated",
        ("viewer",),
    ),
    BuiltInGroup(
        "viewer",
        "Viewer",
        "Sees materializations and datasets. What it may read of a dataset comes from the roles "
        "granted on the dataset.",
        "assigned",
        ("viewer",),
    ),
    BuiltInGroup(
        "editor",
        "Editor",
        "Creates and operates materializations, and creates datasets.",
        "assigned",
        ("viewer", "operator"),
    ),
    BuiltInGroup(
        ADMIN_GROUP_ID,
        "Admin",
        "Full access: materializations, datasets, data, settings, users, groups and roles.",
        "assigned",
        (ADMINISTRATOR_ROLE_ID,),
        locked_role_ids=(ADMINISTRATOR_ROLE_ID,),
    ),
)

BUILT_IN_GROUP_IDS: frozenset[str] = frozenset(group.group_id for group in BUILT_IN_GROUPS)
_GROUPS_BY_ID = {group.group_id: group for group in BUILT_IN_GROUPS}


def built_in_group(group_id: str) -> BuiltInGroup | None:
    """The built-in group with this id, or None."""
    return _GROUPS_BY_ID.get(group_id)


#: What the console's service credential may read: everything its live views poll for, the schemas
#: of every dataset included (the console filters them per person before it shows them). It never
#: changes anything; every change the console makes is made with the signed-in person's session.
SERVICE_PERMISSIONS: frozenset[str] = expand_permissions(
    ("materializations.view_details", "datasets.view", "dataset.view_schema", "topics.view", "settings.view")
)


def describe_catalog() -> dict[str, Any]:
    """The permission catalog as the API reports it: categories, permissions, and implications."""
    return {
        "categories": [
            {"id": category.category_id, "label": category.label} for category in PERMISSION_CATEGORIES
        ],
        "permissions": [definition.describe() for definition in PERMISSIONS],
    }
