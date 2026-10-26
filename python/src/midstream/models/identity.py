"""The directory: people, groups, roles, sessions, access tokens, datasets with their grants and
table-name claims, and the requests that change them.

Everything here is stored in the `directory` table (the audit log in `identity-audit`) and written
only by the identity agent. The API validates a change and publishes an `IdentityRequest`; the
agent applies it in order, so rules that span records — one account per email, never losing the
last administrator, one table per name in a dataset — hold without locks.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import dataclasses
from typing import Any

#: A deactivated person cannot sign in, and every session they had ends at its next request.
USER_STATUSES = ("active", "deactivated")

#: How a group gets its members: chosen by people, every signed-in person, or everyone not signed in.
GROUP_MEMBERSHIPS = ("assigned", "authenticated", "guests")

#: Changes to people, groups and roles. Each is applied in order and leaves an outcome a client
#: can wait for.
DIRECTORY_ACTIONS = (
    "create_user",
    "update_user",
    "set_user_password",
    "change_password",
    "sign_out_user",
    "unlock_user",
    "delete_user",
    "create_group",
    "update_group",
    "delete_group",
    "create_role",
    "update_role",
    "delete_role",
    "create_access_token",
    "revoke_access_token",
    # A revocation another domain signed, handed on by the console (handbook 20).
    "apply_token_revocation",
    "create_dataset",
    "update_dataset",
    "delete_dataset",
    "set_dataset_grant",
    # A sign-in through an identity provider (handbook 20): linking the provider's account to the
    # person with its email, or making an account for someone this domain has never seen. Each also
    # records the sign-in, as `sign_in_succeeded` does.
    "link_identity",
    "provision_user",
    # Claiming the names a materialization's new tables take in their datasets: sent by request
    # intake, never by the API, and answered to the workflow as well as to whoever waits on it.
    "claim_tables",
)

#: What happens to sessions. Recorded for activity and the audit log; nobody waits on them.
SESSION_ACTIONS = (
    "sign_in_succeeded",
    "sign_in_failed",
    "session_refreshed",
    "sign_out",
)

#: Work the control plane asks of itself: seeding the built-in groups and the first administrator,
#: removing expired sessions and old audit entries, and releasing the names of tables that left the
#: catalog.
SYSTEM_ACTIONS = ("bootstrap", "maintenance", "release_tables")

#: Who a dataset grant is for: one person, or everyone in a group.
GRANT_PRINCIPAL_TYPES = ("user", "group")

IDENTITY_ACTIONS = DIRECTORY_ACTIONS + SESSION_ACTIONS + SYSTEM_ACTIONS

REQUEST_STATUSES = ("applied", "rejected")
AUDIT_OUTCOMES = ("success", "failure")


def _require(instance: Any, *names: str) -> None:
    for name in names:
        if not getattr(instance, name):
            raise ValueError(f"{name} is required")


def _choice(name: str, value: str, choices: tuple[str, ...]) -> None:
    if value not in choices:
        raise ValueError(f"{name} must be one of: {', '.join(choices)}")


def _strings(value: Any) -> list[str]:
    return [item for item in value or [] if isinstance(item, str)]


@dataclass(frozen=True, slots=True)
class PasswordCredential:
    """A password a person signs in with, as a hash, and whether they must replace it.

    `must_change` is set whenever someone else chose the password — when an administrator adds
    the person or resets it — so only the person ever knows the password they keep using.
    """

    hash: str
    set_at: str
    set_by: str
    must_change: bool = False

    def __post_init__(self) -> None:
        _require(self, "hash", "set_at", "set_by")

    def to_dict(self) -> dict[str, Any]:
        return {
            "hash": self.hash,
            "set_at": self.set_at,
            "set_by": self.set_by,
            "must_change": self.must_change,
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any] | None) -> "PasswordCredential | None":
        if not value:
            return None
        return cls(
            hash=value["hash"],
            set_at=value["set_at"],
            set_by=value["set_by"],
            must_change=bool(value.get("must_change", False)),
        )


@dataclass(frozen=True, slots=True)
class ExternalIdentity:
    """An account at an identity provider that signs in as this person. For single sign-on."""

    provider: str
    subject: str

    def __post_init__(self) -> None:
        _require(self, "provider", "subject")

    def to_dict(self) -> dict[str, str]:
        return {"provider": self.provider, "subject": self.subject}

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "ExternalIdentity":
        return cls(provider=value["provider"], subject=value["subject"])


@dataclass(frozen=True, slots=True)
class User:
    """A person who can sign in.

    `email` is how they sign in and is unique, without regard to case; `user_id` never changes, so
    it is what everything else refers to. `last_name` is empty only for someone an identity provider
    gave a single name. `group_ids` holds the groups they were added to; every
    signed-in person is also in All Users. `session_epoch` rises whenever all their sessions must
    end, and a session issued under an older epoch is refused.
    """

    user_id: str
    email: str
    first_name: str
    last_name: str
    created_at: str
    created_by: str
    updated_at: str
    updated_by: str
    status: str = "active"
    group_ids: list[str] = field(default_factory=list)
    password: PasswordCredential | None = None
    external_identities: list[ExternalIdentity] = field(default_factory=list)
    session_epoch: int = 0
    revision: int = 1
    deactivated_at: str | None = None
    deactivated_by: str | None = None

    def __post_init__(self) -> None:
        _require(self, "user_id", "email", "first_name", "created_at", "created_by")
        _require(self, "updated_at", "updated_by")
        _choice("status", self.status, USER_STATUSES)
        if self.session_epoch < 0:
            raise ValueError("session_epoch must be >= 0")
        if self.revision < 1:
            raise ValueError("revision must be >= 1")

    @property
    def display_name(self) -> str:
        return f"{self.first_name} {self.last_name}".strip()

    @property
    def active(self) -> bool:
        return self.status == "active"

    @property
    def must_change_password(self) -> bool:
        return self.password is not None and self.password.must_change

    def sign_in_methods(self) -> list[str]:
        """How this person can sign in: `password`, and one entry per identity provider."""
        methods = ["password"] if self.password is not None else []
        methods.extend(f"sso:{identity.provider}" for identity in self.external_identities)
        return methods

    def to_dict(self) -> dict[str, Any]:
        return {
            "user_id": self.user_id,
            "email": self.email,
            "first_name": self.first_name,
            "last_name": self.last_name,
            "status": self.status,
            "group_ids": list(self.group_ids),
            "password": None if self.password is None else self.password.to_dict(),
            "external_identities": [identity.to_dict() for identity in self.external_identities],
            "session_epoch": self.session_epoch,
            "revision": self.revision,
            "created_at": self.created_at,
            "created_by": self.created_by,
            "updated_at": self.updated_at,
            "updated_by": self.updated_by,
            "deactivated_at": self.deactivated_at,
            "deactivated_by": self.deactivated_by,
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "User":
        return cls(
            user_id=value["user_id"],
            email=value["email"],
            first_name=value["first_name"],
            last_name=value["last_name"],
            status=value.get("status", "active"),
            group_ids=_strings(value.get("group_ids")),
            password=PasswordCredential.from_dict(value.get("password")),
            external_identities=[
                ExternalIdentity.from_dict(item)
                for item in value.get("external_identities") or []
                if isinstance(item, dict)
            ],
            session_epoch=int(value.get("session_epoch", 0)),
            revision=int(value.get("revision", 1)),
            created_at=value["created_at"],
            created_by=value["created_by"],
            updated_at=value["updated_at"],
            updated_by=value["updated_by"],
            deactivated_at=value.get("deactivated_at"),
            deactivated_by=value.get("deactivated_by"),
        )


@dataclass(frozen=True, slots=True)
class Group:
    """A set of people and the roles they hold through it.

    Built-in groups (Guest, All Users, Viewer, Editor, Admin) cannot be renamed or deleted.
    `external_groups` names identity-provider groups whose members belong here, for single sign-on.
    """

    group_id: str
    name: str
    created_at: str
    created_by: str
    updated_at: str
    updated_by: str
    description: str = ""
    role_ids: list[str] = field(default_factory=list)
    membership: str = "assigned"
    built_in: bool = False
    external_groups: list[str] = field(default_factory=list)
    revision: int = 1

    def __post_init__(self) -> None:
        _require(self, "group_id", "name", "created_at", "created_by", "updated_at", "updated_by")
        _choice("membership", self.membership, GROUP_MEMBERSHIPS)
        if self.revision < 1:
            raise ValueError("revision must be >= 1")

    def to_dict(self) -> dict[str, Any]:
        return {
            "group_id": self.group_id,
            "name": self.name,
            "description": self.description,
            "role_ids": list(self.role_ids),
            "membership": self.membership,
            "built_in": self.built_in,
            "external_groups": list(self.external_groups),
            "revision": self.revision,
            "created_at": self.created_at,
            "created_by": self.created_by,
            "updated_at": self.updated_at,
            "updated_by": self.updated_by,
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "Group":
        return cls(
            group_id=value["group_id"],
            name=value["name"],
            description=value.get("description") or "",
            role_ids=_strings(value.get("role_ids")),
            membership=value.get("membership", "assigned"),
            built_in=bool(value.get("built_in", False)),
            external_groups=_strings(value.get("external_groups")),
            revision=int(value.get("revision", 1)),
            created_at=value["created_at"],
            created_by=value["created_by"],
            updated_at=value["updated_at"],
            updated_by=value["updated_by"],
        )


@dataclass(frozen=True, slots=True)
class Role:
    """A reusable set of permissions that groups hold. Built-in roles are defined in code."""

    role_id: str
    name: str
    created_at: str
    created_by: str
    updated_at: str
    updated_by: str
    description: str = ""
    permissions: list[str] = field(default_factory=list)
    built_in: bool = False
    revision: int = 1

    def __post_init__(self) -> None:
        _require(self, "role_id", "name", "created_at", "created_by", "updated_at", "updated_by")
        if self.revision < 1:
            raise ValueError("revision must be >= 1")

    def to_dict(self) -> dict[str, Any]:
        return {
            "role_id": self.role_id,
            "name": self.name,
            "description": self.description,
            "permissions": list(self.permissions),
            "built_in": self.built_in,
            "revision": self.revision,
            "created_at": self.created_at,
            "created_by": self.created_by,
            "updated_at": self.updated_at,
            "updated_by": self.updated_by,
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "Role":
        return cls(
            role_id=value["role_id"],
            name=value["name"],
            description=value.get("description") or "",
            permissions=_strings(value.get("permissions")),
            built_in=bool(value.get("built_in", False)),
            revision=int(value.get("revision", 1)),
            created_at=value["created_at"],
            created_by=value["created_by"],
            updated_at=value["updated_at"],
            updated_by=value["updated_by"],
        )


@dataclass(frozen=True, slots=True)
class Session:
    """One sign-in: when it started, when it was last used, and how it ended.

    A session can be used before this record exists — the token alone carries what a request
    needs — so a missing record means a session too new to be recorded, never a refused one.
    """

    session_id: str
    user_id: str
    created_at: str
    expires_at: str
    last_seen_at: str
    method: str = "password"
    ip: str | None = None
    user_agent: str | None = None
    ended_at: str | None = None
    ended_reason: str | None = None
    ended_by: str | None = None

    def __post_init__(self) -> None:
        _require(self, "session_id", "user_id", "created_at", "expires_at", "last_seen_at", "method")

    @property
    def ended(self) -> bool:
        return self.ended_at is not None

    def to_dict(self) -> dict[str, Any]:
        return {
            "session_id": self.session_id,
            "user_id": self.user_id,
            "method": self.method,
            "created_at": self.created_at,
            "expires_at": self.expires_at,
            "last_seen_at": self.last_seen_at,
            "ip": self.ip,
            "user_agent": self.user_agent,
            "ended_at": self.ended_at,
            "ended_reason": self.ended_reason,
            "ended_by": self.ended_by,
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "Session":
        return cls(
            session_id=value["session_id"],
            user_id=value["user_id"],
            method=value.get("method", "password"),
            created_at=value["created_at"],
            expires_at=value["expires_at"],
            last_seen_at=value["last_seen_at"],
            ip=value.get("ip"),
            user_agent=value.get("user_agent"),
            ended_at=value.get("ended_at"),
            ended_reason=value.get("ended_reason"),
            ended_by=value.get("ended_by"),
        )


@dataclass(frozen=True, slots=True)
class AccessToken:
    """A token someone made in this domain for a tool (handbook 20), as the domain lists it.

    The token itself is signed and checked by every domain without this record
    (`identity/access_tokens.py`); this is what its owner and administrators see and revoke it by.
    `datasets` limits it to some datasets, as `<domain>.<dataset>`; empty, it may use its
    permissions on every dataset its owner may, in every domain.
    """

    token_id: str
    user_id: str
    name: str
    permissions: tuple[str, ...]
    created_at: str
    expires_at: str
    #: The domain that made it: this one.
    issuer: str = ""
    revoked_at: str | None = None
    revoked_by: str | None = None
    datasets: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        _require(self, "token_id", "user_id", "name", "created_at", "expires_at")

    @property
    def revoked(self) -> bool:
        return self.revoked_at is not None

    def to_dict(self) -> dict[str, Any]:
        return {
            "token_id": self.token_id,
            "user_id": self.user_id,
            "name": self.name,
            "permissions": list(self.permissions),
            "created_at": self.created_at,
            "expires_at": self.expires_at,
            "issuer": self.issuer,
            "revoked_at": self.revoked_at,
            "revoked_by": self.revoked_by,
            "datasets": list(self.datasets),
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "AccessToken":
        return cls(
            token_id=value["token_id"],
            user_id=value["user_id"],
            name=value["name"],
            permissions=tuple(_strings(value.get("permissions"))),
            created_at=value["created_at"],
            expires_at=value["expires_at"],
            issuer=value.get("issuer") or "",
            revoked_at=value.get("revoked_at"),
            revoked_by=value.get("revoked_by"),
            datasets=tuple(_strings(value.get("datasets"))),
        )


@dataclass(frozen=True, slots=True)
class TokenRevocation:
    """That an access token was revoked, in whichever domain it was (handbook 20).

    Every domain keeps one for each revoked token until the token expires, and refuses the token
    while it does. `signature` is the revocation as the domain that revoked it signed it, so it can
    be handed on to another domain; `source` is that domain.
    """

    token_id: str
    expires_at: int
    signature: str
    revoked_at: str
    source: str = ""

    def __post_init__(self) -> None:
        _require(self, "token_id", "signature", "revoked_at")

    def to_dict(self) -> dict[str, Any]:
        return {
            "token_id": self.token_id,
            "expires_at": self.expires_at,
            "signature": self.signature,
            "revoked_at": self.revoked_at,
            "source": self.source,
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "TokenRevocation":
        return cls(
            token_id=value["token_id"],
            expires_at=int(value["expires_at"]),
            signature=value["signature"],
            revoked_at=value["revoked_at"],
            source=value.get("source") or "",
        )


@dataclass(frozen=True, slots=True)
class Dataset:
    """A named group of tables: their Iceberg namespace, so SQL reads `<dataset>.<table>`.

    The name is unique in the deployment and never changes; `dataset_id` is what everything else
    refers to, so a dataset deleted and created again under its name starts with nothing of the
    old one's - neither its grants nor its tables.
    """

    dataset_id: str
    name: str
    created_at: str
    created_by: str
    updated_at: str
    updated_by: str
    description: str = ""
    revision: int = 1

    def __post_init__(self) -> None:
        _require(self, "dataset_id", "name", "created_at", "created_by", "updated_at", "updated_by")
        if self.revision < 1:
            raise ValueError("revision must be >= 1")

    def to_dict(self) -> dict[str, Any]:
        return {
            "dataset_id": self.dataset_id,
            "name": self.name,
            "description": self.description,
            "revision": self.revision,
            "created_at": self.created_at,
            "created_by": self.created_by,
            "updated_at": self.updated_at,
            "updated_by": self.updated_by,
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "Dataset":
        return cls(
            dataset_id=value["dataset_id"],
            name=value["name"],
            description=value.get("description") or "",
            revision=int(value.get("revision", 1)),
            created_at=value["created_at"],
            created_by=value["created_by"],
            updated_at=value["updated_at"],
            updated_by=value["updated_by"],
        )


@dataclass(frozen=True, slots=True)
class DatasetGrant:
    """Roles a person or a group holds on one dataset: what they may do with its tables."""

    dataset_id: str
    principal_type: str
    principal_id: str
    role_ids: list[str]
    updated_at: str
    updated_by: str

    def __post_init__(self) -> None:
        _require(self, "dataset_id", "principal_id", "updated_at", "updated_by")
        _choice("principal_type", self.principal_type, GRANT_PRINCIPAL_TYPES)

    def to_dict(self) -> dict[str, Any]:
        return {
            "dataset_id": self.dataset_id,
            "principal_type": self.principal_type,
            "principal_id": self.principal_id,
            "role_ids": list(self.role_ids),
            "updated_at": self.updated_at,
            "updated_by": self.updated_by,
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "DatasetGrant":
        return cls(
            dataset_id=value["dataset_id"],
            principal_type=value["principal_type"],
            principal_id=value["principal_id"],
            role_ids=_strings(value.get("role_ids")),
            updated_at=value["updated_at"],
            updated_by=value["updated_by"],
        )


@dataclass(frozen=True, slots=True)
class TableClaim:
    """A table name taken in a dataset: `<dataset>.<table>` belongs to one materialization's table.

    Written before the table exists, so two materializations can never create tables of one name,
    and removed once the table leaves the catalog, which frees the name.
    """

    dataset_id: str
    table: str
    materialization_id: str
    source_topic: str
    claimed_at: str
    claimed_by: str
    #: The directory request that claimed it.
    request_id: str

    def __post_init__(self) -> None:
        _require(self, "dataset_id", "table", "materialization_id", "source_topic", "claimed_at", "claimed_by", "request_id")

    def to_dict(self) -> dict[str, Any]:
        return {
            "dataset_id": self.dataset_id,
            "table": self.table,
            "materialization_id": self.materialization_id,
            "source_topic": self.source_topic,
            "claimed_at": self.claimed_at,
            "claimed_by": self.claimed_by,
            "request_id": self.request_id,
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "TableClaim":
        return cls(
            dataset_id=value["dataset_id"],
            table=value["table"],
            materialization_id=value["materialization_id"],
            source_topic=value["source_topic"],
            claimed_at=value["claimed_at"],
            claimed_by=value["claimed_by"],
            request_id=value["request_id"],
        )


@dataclass(frozen=True, slots=True)
class ClaimRelease:
    """That a materialization gave up every name it held, because it is being deleted.

    Kept for a while after, so a claim of it that arrives late - after its release - is refused
    rather than taking a name nothing will ever release.
    """

    materialization_id: str
    released_at: str

    def __post_init__(self) -> None:
        _require(self, "materialization_id", "released_at")

    def to_dict(self) -> dict[str, Any]:
        return {"materialization_id": self.materialization_id, "released_at": self.released_at}

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "ClaimRelease":
        return cls(materialization_id=value["materialization_id"], released_at=value["released_at"])


@dataclass(frozen=True, slots=True)
class SignInGuard:
    """Failed sign-ins for one email, and until when it is locked out.

    Kept per email rather than per account, so an email with no account is locked out the same
    way — the answer never says whether an account exists.
    """

    email: str
    failed_attempts: int = 0
    first_failed_at: str | None = None
    last_failed_at: str | None = None
    locked_until: str | None = None

    def __post_init__(self) -> None:
        _require(self, "email")
        if self.failed_attempts < 0:
            raise ValueError("failed_attempts must be >= 0")

    def to_dict(self) -> dict[str, Any]:
        return {
            "email": self.email,
            "failed_attempts": self.failed_attempts,
            "first_failed_at": self.first_failed_at,
            "last_failed_at": self.last_failed_at,
            "locked_until": self.locked_until,
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "SignInGuard":
        return cls(
            email=value["email"],
            failed_attempts=int(value.get("failed_attempts", 0)),
            first_failed_at=value.get("first_failed_at"),
            last_failed_at=value.get("last_failed_at"),
            locked_until=value.get("locked_until"),
        )


@dataclass(frozen=True, slots=True)
class UserActivity:
    """When a person last signed in and was last active, for the Users page."""

    user_id: str
    last_sign_in_at: str | None = None
    last_active_at: str | None = None
    sign_in_count: int = 0

    def __post_init__(self) -> None:
        _require(self, "user_id")

    def to_dict(self) -> dict[str, Any]:
        return {
            "user_id": self.user_id,
            "last_sign_in_at": self.last_sign_in_at,
            "last_active_at": self.last_active_at,
            "sign_in_count": self.sign_in_count,
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "UserActivity":
        return cls(
            user_id=value["user_id"],
            last_sign_in_at=value.get("last_sign_in_at"),
            last_active_at=value.get("last_active_at"),
            sign_in_count=int(value.get("sign_in_count", 0)),
        )


@dataclass(frozen=True, slots=True)
class RequestContext:
    """Where a request came from, as far as the API could tell, for the audit log."""

    ip: str | None = None
    user_agent: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {"ip": self.ip, "user_agent": self.user_agent}

    @classmethod
    def from_dict(cls, value: dict[str, Any] | None) -> "RequestContext":
        value = value or {}
        return cls(ip=value.get("ip"), user_agent=value.get("user_agent"))


@dataclass(frozen=True, slots=True)
class IdentityRequest:
    """A change to people, groups, roles or sessions, published by the API for the agent to apply.

    `actor_id` is who asked: a user id, or `system` for the control plane's own work. A request
    never carries a password, only its hash.
    """

    request_id: str
    action: str
    submitted_at: str
    actor_id: str
    actor_email: str | None = None
    payload: dict[str, Any] = field(default_factory=dict)
    base_revision: int | None = None
    context: RequestContext = field(default_factory=RequestContext)

    def __post_init__(self) -> None:
        _require(self, "request_id", "submitted_at", "actor_id")
        _choice("action", self.action, IDENTITY_ACTIONS)
        if self.base_revision is not None and self.base_revision < 1:
            raise ValueError("base_revision must be >= 1")

    @property
    def tracked(self) -> bool:
        """Whether a client may wait on this request's outcome."""
        return self.action in DIRECTORY_ACTIONS

    def to_dict(self) -> dict[str, Any]:
        return {
            "request_id": self.request_id,
            "action": self.action,
            "submitted_at": self.submitted_at,
            "actor_id": self.actor_id,
            "actor_email": self.actor_email,
            "payload": dict(self.payload),
            "base_revision": self.base_revision,
            "context": self.context.to_dict(),
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "IdentityRequest":
        return cls(
            request_id=value["request_id"],
            action=value["action"],
            submitted_at=value["submitted_at"],
            actor_id=value["actor_id"],
            actor_email=value.get("actor_email"),
            payload=dict(value.get("payload") or {}),
            base_revision=value.get("base_revision"),
            context=RequestContext.from_dict(value.get("context")),
        )


@dataclass(frozen=True, slots=True)
class IdentityRequestOutcome:
    """Whether the agent applied a directory request, and why not when it refused one.

    `details` says more about a refusal for a client to point at: for a claim, which table and
    dataset it was about.
    """

    request_id: str
    action: str
    status: str
    completed_at: str
    actor_id: str
    entity_type: str | None = None
    entity_id: str | None = None
    reason: str | None = None
    message: str | None = None
    field: str | None = None
    # `field` above is a field of this class, so the function is named in full here.
    details: dict[str, Any] = dataclasses.field(default_factory=dict)

    def __post_init__(self) -> None:
        _require(self, "request_id", "action", "completed_at", "actor_id")
        _choice("status", self.status, REQUEST_STATUSES)

    def to_dict(self) -> dict[str, Any]:
        return {
            "request_id": self.request_id,
            "action": self.action,
            "status": self.status,
            "completed_at": self.completed_at,
            "actor_id": self.actor_id,
            "entity_type": self.entity_type,
            "entity_id": self.entity_id,
            "reason": self.reason,
            "message": self.message,
            "field": self.field,
            "details": dict(self.details),
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "IdentityRequestOutcome":
        return cls(
            request_id=value["request_id"],
            action=value["action"],
            status=value["status"],
            completed_at=value["completed_at"],
            actor_id=value["actor_id"],
            entity_type=value.get("entity_type"),
            entity_id=value.get("entity_id"),
            reason=value.get("reason"),
            message=value.get("message"),
            field=value.get("field"),
            details=dict(value.get("details") or {}),
        )


@dataclass(frozen=True, slots=True)
class DirectoryState:
    """The directory's revision, which every change to people, groups, roles or dataset grants raises.

    Authorization caches what a person may do against it, so a change applies to the next request.
    """

    revision: int = 0
    updated_at: str | None = None
    seeded_at: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {"revision": self.revision, "updated_at": self.updated_at, "seeded_at": self.seeded_at}

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "DirectoryState":
        return cls(
            revision=int(value.get("revision", 0)),
            updated_at=value.get("updated_at"),
            seeded_at=value.get("seeded_at"),
        )


@dataclass(frozen=True, slots=True)
class AuditEntry:
    """One security-relevant event: a sign-in, or a change to people, groups or roles.

    `actor_*` is who did it, `target_*` what it was done to, and `details` what changed. Names and
    emails are copied in, so an entry still reads correctly after the person is renamed or deleted.
    """

    entry_id: str
    occurred_at: str
    action: str
    outcome: str = "success"
    actor_id: str | None = None
    actor_email: str | None = None
    target_type: str | None = None
    target_id: str | None = None
    target_label: str | None = None
    details: dict[str, Any] = field(default_factory=dict)
    ip: str | None = None
    user_agent: str | None = None

    def __post_init__(self) -> None:
        _require(self, "entry_id", "occurred_at", "action")
        _choice("outcome", self.outcome, AUDIT_OUTCOMES)

    def to_dict(self) -> dict[str, Any]:
        return {
            "entry_id": self.entry_id,
            "occurred_at": self.occurred_at,
            "action": self.action,
            "outcome": self.outcome,
            "actor_id": self.actor_id,
            "actor_email": self.actor_email,
            "target_type": self.target_type,
            "target_id": self.target_id,
            "target_label": self.target_label,
            "details": dict(self.details),
            "ip": self.ip,
            "user_agent": self.user_agent,
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "AuditEntry":
        return cls(
            entry_id=value["entry_id"],
            occurred_at=value["occurred_at"],
            action=value["action"],
            outcome=value.get("outcome", "success"),
            actor_id=value.get("actor_id"),
            actor_email=value.get("actor_email"),
            target_type=value.get("target_type"),
            target_id=value.get("target_id"),
            target_label=value.get("target_label"),
            details=dict(value.get("details") or {}),
            ip=value.get("ip"),
            user_agent=value.get("user_agent"),
        )
