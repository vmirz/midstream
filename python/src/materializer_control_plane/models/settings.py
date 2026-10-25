"""Settings overrides, the requests that change them, and the record of each change.

Settings start from the environment each component is deployed with. An administrator may
override some of them from the console; the overrides of one scope live in one
`SettingsDocument`, and every applied change leaves a `SettingsChangeRecord`.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

#: Who a scope's settings configure.
#:
#: `control-plane` and `runtime` are described and validated by this package. `console` belongs
#: to the web console, which validates its own settings; the control plane only stores them, so
#: the console can add a setting without a control-plane release.
SETTINGS_SCOPES = ("control-plane", "runtime", "console")

#: Where a setting's effective value comes from.
SETTING_SOURCES = ("default", "deployment", "override")

#: Requests a document remembers having applied, so a redelivered request is not applied twice.
RECENT_REQUEST_LIMIT = 20


def _validate_scope(scope: str) -> None:
    if scope not in SETTINGS_SCOPES:
        allowed = ", ".join(SETTINGS_SCOPES)
        raise ValueError(f"scope must be one of: {allowed}")


@dataclass(frozen=True, slots=True)
class SettingOverride:
    """The value an administrator chose for one setting, and who chose it when."""

    value: Any
    updated_at: str
    updated_by: str

    def __post_init__(self) -> None:
        if not self.updated_at:
            raise ValueError("updated_at is required")
        if not self.updated_by:
            raise ValueError("updated_by is required")

    def to_dict(self) -> dict[str, Any]:
        return {
            "value": self.value,
            "updated_at": self.updated_at,
            "updated_by": self.updated_by,
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "SettingOverride":
        return cls(
            value=value.get("value"),
            updated_at=value["updated_at"],
            updated_by=value["updated_by"],
        )


@dataclass(frozen=True, slots=True)
class SettingsRejection:
    """A change request the settings agent refused after the API had accepted it."""

    request_id: str
    reason: str
    message: str
    rejected_at: str

    def __post_init__(self) -> None:
        for name in ("request_id", "reason", "message", "rejected_at"):
            if not getattr(self, name):
                raise ValueError(f"{name} is required")

    def to_dict(self) -> dict[str, Any]:
        return {
            "request_id": self.request_id,
            "reason": self.reason,
            "message": self.message,
            "rejected_at": self.rejected_at,
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any] | None) -> "SettingsRejection | None":
        if not value:
            return None
        return cls(
            request_id=value["request_id"],
            reason=value["reason"],
            message=value["message"],
            rejected_at=value["rejected_at"],
        )


@dataclass(frozen=True, slots=True)
class SettingsDocument:
    """Every override in one scope.

    `revision` counts applied changes and only increases; 0 means nothing was ever changed, and
    every setting in the scope takes its deployment or default value.

    `lineage` names this history of the runtime settings, which runtimes receive: begun once, when
    the control plane first starts on a `settings` table without one, and never changed after. It
    starts with the time it began, so a later lineage sorts after an earlier one, and a runtime
    follows a new history even though its revisions start again (handbook 17 §17.7). Only the
    runtime scope has one; None on a document stored before 4.1 until the next start gives it one.
    `lineage_revision` is the revision the scope had when its lineage began: deliveries up to it
    went out without one, so a runtime upgraded from before 4.1 can hold it without a lineage.
    """

    scope: str
    revision: int = 0
    values: dict[str, SettingOverride] = field(default_factory=dict)
    updated_at: str | None = None
    updated_by: str | None = None
    recent_request_ids: list[str] = field(default_factory=list)
    last_rejection: SettingsRejection | None = None
    lineage: str | None = None
    lineage_revision: int | None = None

    def __post_init__(self) -> None:
        _validate_scope(self.scope)
        if self.revision < 0:
            raise ValueError("revision must be >= 0")
        if self.lineage_revision is not None and not 0 <= self.lineage_revision <= self.revision:
            raise ValueError("lineage_revision must be between 0 and revision")

    def override_values(self) -> dict[str, Any]:
        """The overridden settings and their values, without attribution."""
        return {key: override.value for key, override in self.values.items()}

    def to_dict(self) -> dict[str, Any]:
        return {
            "scope": self.scope,
            "revision": self.revision,
            "values": {key: override.to_dict() for key, override in sorted(self.values.items())},
            "updated_at": self.updated_at,
            "updated_by": self.updated_by,
            "recent_request_ids": list(self.recent_request_ids),
            "last_rejection": None if self.last_rejection is None else self.last_rejection.to_dict(),
            "lineage": self.lineage,
            "lineage_revision": self.lineage_revision,
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "SettingsDocument":
        revision = value.get("revision", 0)
        lineage = value.get("lineage")
        lineage_revision = value.get("lineage_revision")
        return cls(
            scope=value["scope"],
            revision=revision,
            values={
                key: SettingOverride.from_dict(override)
                for key, override in (value.get("values") or {}).items()
                if isinstance(override, dict)
            },
            updated_at=value.get("updated_at"),
            updated_by=value.get("updated_by"),
            recent_request_ids=[
                item for item in value.get("recent_request_ids") or [] if isinstance(item, str)
            ],
            last_rejection=SettingsRejection.from_dict(value.get("last_rejection")),
            lineage=lineage if isinstance(lineage, str) and lineage else None,
            lineage_revision=(
                lineage_revision
                if isinstance(lineage_revision, int)
                and not isinstance(lineage_revision, bool)
                and 0 <= lineage_revision <= revision
                else None
            ),
        )


@dataclass(frozen=True, slots=True)
class SettingsBootstrapRequest:
    """The startup trigger: give the runtime settings a lineage if they have none.

    Published by the `settings-bootstrap` task each time the control plane starts, on the
    settings-requests topic with the scope's key, so it applies in order with the scope's changes.
    """

    requested_at: str
    scope: str = "runtime"

    KIND = "bootstrap"

    def __post_init__(self) -> None:
        if not self.requested_at:
            raise ValueError("requested_at is required")
        _validate_scope(self.scope)

    def to_dict(self) -> dict[str, Any]:
        return {"kind": self.KIND, "scope": self.scope, "requested_at": self.requested_at}

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "SettingsBootstrapRequest":
        return cls(requested_at=value["requested_at"], scope=value.get("scope") or "runtime")

    @classmethod
    def matches(cls, value: Any) -> bool:
        """Whether a settings-requests message is this trigger rather than a change request."""
        return isinstance(value, dict) and value.get("kind") == cls.KIND


@dataclass(frozen=True, slots=True)
class SettingFallback:
    """What a console setting reads as when it is not overridden, as the console reported it.

    The control plane does not know the console's settings, so a console change request carries
    these to make its change record readable on its own.
    """

    value: Any
    source: str

    def __post_init__(self) -> None:
        if self.source not in ("default", "deployment"):
            raise ValueError("fallback source must be one of: default, deployment")

    def to_dict(self) -> dict[str, Any]:
        return {"value": self.value, "source": self.source}

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "SettingFallback":
        return cls(value=value.get("value"), source=value["source"])


@dataclass(frozen=True, slots=True)
class SettingsChangeRequest:
    """A request to override some settings of one scope and return others to their defaults.

    Published to the settings-requests topic by the API; the settings agent applies it.
    `base_revision` is the revision the requester last read: the change is refused if the
    scope changed since, so two administrators cannot overwrite each other unknowingly.
    """

    request_id: str
    scope: str
    submitted_at: str
    submitted_by: str
    values: dict[str, Any] = field(default_factory=dict)
    reset: list[str] = field(default_factory=list)
    base_revision: int | None = None
    fallbacks: dict[str, SettingFallback] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for name in ("request_id", "submitted_at", "submitted_by"):
            if not getattr(self, name):
                raise ValueError(f"{name} is required")
        _validate_scope(self.scope)
        if not self.values and not self.reset:
            raise ValueError("a settings change must set or reset at least one setting")
        overlap = sorted(set(self.values) & set(self.reset))
        if overlap:
            raise ValueError(f"a setting cannot be both changed and reset: {', '.join(overlap)}")
        if self.base_revision is not None and self.base_revision < 0:
            raise ValueError("base_revision must be >= 0")

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "request_id": self.request_id,
            "scope": self.scope,
            "submitted_at": self.submitted_at,
            "submitted_by": self.submitted_by,
            "values": dict(self.values),
            "reset": list(self.reset),
            "base_revision": self.base_revision,
        }
        if self.fallbacks:
            payload["fallbacks"] = {key: item.to_dict() for key, item in self.fallbacks.items()}
        return payload

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "SettingsChangeRequest":
        return cls(
            request_id=value["request_id"],
            scope=value["scope"],
            submitted_at=value["submitted_at"],
            submitted_by=value["submitted_by"],
            values=dict(value.get("values") or {}),
            reset=list(value.get("reset") or []),
            base_revision=value.get("base_revision"),
            fallbacks={
                key: SettingFallback.from_dict(item)
                for key, item in (value.get("fallbacks") or {}).items()
                if isinstance(item, dict)
            },
        )


@dataclass(frozen=True, slots=True)
class SettingChange:
    """One setting's effective value before and after a change, and where each came from."""

    key: str
    before: Any
    after: Any
    before_source: str
    after_source: str

    def __post_init__(self) -> None:
        if not self.key:
            raise ValueError("key is required")
        for name in ("before_source", "after_source"):
            if getattr(self, name) not in SETTING_SOURCES:
                allowed = ", ".join(SETTING_SOURCES)
                raise ValueError(f"{name} must be one of: {allowed}")

    def to_dict(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "before": self.before,
            "after": self.after,
            "before_source": self.before_source,
            "after_source": self.after_source,
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "SettingChange":
        return cls(
            key=value["key"],
            before=value.get("before"),
            after=value.get("after"),
            before_source=value["before_source"],
            after_source=value["after_source"],
        )


@dataclass(frozen=True, slots=True)
class SettingsChangeRecord:
    """What one applied request changed in a scope: an entry of its change history."""

    scope: str
    revision: int
    request_id: str
    updated_at: str
    updated_by: str
    changes: list[SettingChange] = field(default_factory=list)

    def __post_init__(self) -> None:
        _validate_scope(self.scope)
        if self.revision < 1:
            raise ValueError("revision must be >= 1")
        for name in ("request_id", "updated_at", "updated_by"):
            if not getattr(self, name):
                raise ValueError(f"{name} is required")

    def to_dict(self) -> dict[str, Any]:
        return {
            "scope": self.scope,
            "revision": self.revision,
            "request_id": self.request_id,
            "updated_at": self.updated_at,
            "updated_by": self.updated_by,
            "changes": [change.to_dict() for change in self.changes],
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "SettingsChangeRecord":
        return cls(
            scope=value["scope"],
            revision=value["revision"],
            request_id=value["request_id"],
            updated_at=value["updated_at"],
            updated_by=value["updated_by"],
            changes=[
                SettingChange.from_dict(item)
                for item in value.get("changes") or []
                if isinstance(item, dict)
            ],
        )
