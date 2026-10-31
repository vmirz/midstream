"""WebView handlers for settings: read a scope, change it, and list what changed.

Like every mutation in this API, a change is validated here and published as a request; the
settings agent applies it. Nothing here writes to a table.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from midstream.config import ControlPlaneConfig, get_config
from midstream.identity.providers import get_provider_settings
from midstream.identity.secrets import get_identity_secrets
from midstream.lakehouse.store import StoreSettings
from midstream.models import SETTINGS_SCOPES, SettingFallback, SettingsChangeRequest
from midstream.observation.progress import MEMBER_STATE_FRESHNESS_SECONDS
from midstream.protection.secrets import get_protection_key
from midstream.runtime.templates import get_installed_package_revision
from midstream.services.settings import validate_change
from midstream.settings.catalog import DEPLOYMENT_SETTINGS, SettingValueError, definition_for
from midstream.settings.effective import (
    CONSOLE_SCOPE,
    CONTROL_PLANE_SCOPE,
    RUNTIME_SCOPE,
    describe_settings,
    runtime_deployment_config,
)
from midstream.storage import get_materialization_status, iter_materializations
from midstream.storage.settings import (
    SETTINGS_HISTORY_LIMIT,
    get_settings_document,
    list_settings_changes,
)
from midstream.storage.table import TableProtocol
from midstream.web.errors import conflict, invalid_request, not_found

if TYPE_CHECKING:
    from midstream.identity.authorization import Principal

_DELETION_PHASES = frozenset({"deleting", "deleted"})


@dataclass(frozen=True, slots=True)
class SettingsAcceptance:
    """The 202 response to a settings change, and the request event that carries it."""

    response: dict[str, Any]
    request_event: SettingsChangeRequest


def settings_scope_view(
    settings_table: TableProtocol | None,
    scope: str,
    *,
    control_plane_table: TableProtocol | None = None,
    config: ControlPlaneConfig | None = None,
    environ: Mapping[str, str] | None = None,
    now: str | None = None,
) -> dict[str, Any]:
    """Everything the settings page shows for one scope.

    For `control-plane` and `runtime`: every catalog setting with its effective value and where it
    comes from, and the deployment's read-only settings. For `runtime`, also how far the current
    revision has reached the running runtimes. For `console`: the stored overrides only, which the
    console interprets against its own catalog.
    """
    _require_scope(scope)
    cfg = get_config() if config is None else config
    document = get_settings_document(settings_table, scope)
    payload: dict[str, Any] = {
        "scope": scope,
        "revision": 0 if document is None else document.revision,
        "updated_at": None if document is None else document.updated_at,
        "updated_by": None if document is None else document.updated_by,
        "recent_request_ids": [] if document is None else list(document.recent_request_ids),
        "last_rejection": (
            None if document is None or document.last_rejection is None else document.last_rejection.to_dict()
        ),
        "lineage": None if document is None else document.lineage,
    }
    if scope == CONSOLE_SCOPE:
        payload["values"] = {} if document is None else {
            key: override.to_dict() for key, override in sorted(document.values.items())
        }
        return payload
    payload["settings"] = [
        setting.describe() for setting in describe_settings(scope, document, config=cfg, environ=environ)
    ]
    payload["deployment"] = _deployment_settings(scope, cfg)
    if scope == RUNTIME_SCOPE:
        payload["rollout"] = runtime_settings_rollout(
            control_plane_table,
            revision=payload["revision"],
            lineage=payload["lineage"],
            lineage_revision=None if document is None else document.lineage_revision,
            now=now or _utc_now(),
        )
    return payload


def update_settings(
    settings_table: TableProtocol | None,
    scope: str,
    body: Any,
    *,
    submitted_by: str,
    request_id: str | None = None,
    accepted_at: str | None = None,
    config: ControlPlaneConfig | None = None,
    environ: Mapping[str, str] | None = None,
    principal: "Principal | None" = None,
) -> SettingsAcceptance:
    """Validate a change to a scope's settings and return the request that will apply it.

    Body: `values` (settings to override, by key), `reset` (settings to return to their
    deployment or default value), and `base_revision`, the revision the client last read. A
    client that sends `base_revision` is refused with 409 when the scope has changed since, so it
    never overwrites a change it did not see.

    A setting whose definition names a `permission` (the protection default's
    `settings.manage_protection`) needs it too: `principal`, the caller the view authorized for
    `settings.manage`, must hold it to set or reset that setting (403 otherwise).
    """
    _require_scope(scope)
    if not isinstance(body, dict):
        raise invalid_request("request body must be an object")
    unknown = sorted(set(body) - {"values", "reset", "base_revision", "fallbacks"})
    if unknown:
        raise invalid_request(f"{unknown[0]} is not part of a settings change", field_name=unknown[0])
    values = {} if body.get("values") is None else body["values"]
    reset = [] if body.get("reset") is None else body["reset"]
    if not isinstance(values, dict):
        raise invalid_request("values must be an object", field_name="values")
    if not isinstance(reset, list) or not all(isinstance(key, str) for key in reset):
        raise invalid_request("reset must be a list of setting keys", field_name="reset")
    base_revision = body.get("base_revision")
    if base_revision is not None and (
        isinstance(base_revision, bool) or not isinstance(base_revision, int) or base_revision < 0
    ):
        raise invalid_request("base_revision must be a non-negative integer", field_name="base_revision")
    fallbacks = _parse_fallbacks(body.get("fallbacks"), scope=scope)
    if principal is not None:
        # Imported here: the authorization module imports the web package, which imports this one.
        from midstream.identity.authorization import require

        for key in [*values, *reset]:
            definition = definition_for(scope, key) if isinstance(key, str) else None
            if definition is not None and definition.permission is not None:
                require(principal, definition.permission)

    document = get_settings_document(settings_table, scope)
    current_revision = 0 if document is None else document.revision
    if base_revision is not None and base_revision != current_revision:
        raise conflict(
            f"these settings changed after they were read (revision {base_revision}, now "
            f"{current_revision}); reload them and try again",
            reason="settings_changed",
        )

    resolved_accepted_at = accepted_at or _utc_now()
    try:
        request = SettingsChangeRequest(
            request_id=request_id or f"set-{uuid4().hex[:12]}",
            scope=scope,
            submitted_at=resolved_accepted_at,
            submitted_by=submitted_by or "anonymous",
            values=dict(values),
            reset=list(dict.fromkeys(reset)),
            base_revision=base_revision,
            fallbacks=fallbacks,
        )
    except ValueError as error:
        raise invalid_request(str(error)) from error
    try:
        validate_change(document, request, config=config, environ=environ)
    except SettingValueError as error:
        raise invalid_request(str(error), field_name=error.key) from error
    return SettingsAcceptance(
        response={
            "scope": scope,
            "request_id": request.request_id,
            "base_revision": current_revision,
            "accepted_at": resolved_accepted_at,
            "status_url": f"/settings/{scope}",
        },
        request_event=request,
    )


def settings_history_view(
    settings_table: TableProtocol | None,
    scope: str,
    *,
    limit: str | int | None = None,
) -> dict[str, Any]:
    """A scope's recorded changes, newest first; at most the last 100 are kept."""
    _require_scope(scope)
    bounded = _parse_history_limit(limit)
    changes = list_settings_changes(settings_table, scope, limit=bounded)
    return {
        "scope": scope,
        "changes": [record.to_dict() for record in changes],
        "returned": len(changes),
        "retained": SETTINGS_HISTORY_LIMIT,
    }


def runtime_settings_rollout(
    control_plane_table: TableProtocol | None,
    *,
    revision: int,
    now: str,
    lineage: str | None = None,
    lineage_revision: int | None = None,
) -> dict[str, Any] | None:
    """How far a runtime-settings revision has reached the materializations' runtimes.

    Each runtime member reports the lineage and revision it runs on its heartbeat. A
    materialization is `current` once every member heard from in the last two minutes runs
    `revision` of `lineage`; `pending` while one still runs an older revision, or settings of
    another lineage, such as an earlier installation's (its `revision` is then None, since a
    number from another history says nothing); `stopped` while it has no members, since it takes
    the current revision when it resumes; `not_reporting` when no member has been heard from
    lately; and `unsupported` when its runtime predates live settings and will take them at its
    next deploy. A member that predates lineages reports none and is judged by its revision. One
    that holds settings without a lineage at `lineage_revision`, with nothing changed since, runs
    the settings this lineage began from: delivered before it began, to a runtime since upgraded.
    """
    if control_plane_table is None:
        return None
    reference = _parse_timestamp(now)
    counts = {"current": 0, "pending": 0, "stopped": 0, "not_reporting": 0, "unsupported": 0}
    items: list[dict[str, Any]] = []
    for materialization in iter_materializations(control_plane_table):
        if materialization.desired_phase in _DELETION_PHASES:
            continue
        status = get_materialization_status(control_plane_table, materialization.materialization_id)
        members = {} if status is None else status.runtime_progress.member_progress
        fresh = [
            member
            for member in members.values()
            if _is_fresh(member.reported_at, reference)
        ]
        reported = [member.settings_revision for member in fresh]
        applied = min((value for value in reported if value is not None), default=None)
        histories = [
            _settings_history(member, lineage=lineage, lineage_revision=lineage_revision) for member in fresh
        ]
        other_lineage = "other" in histories
        if materialization.desired_phase == "stopped":
            state = "stopped"
        elif not fresh:
            state = "not_reporting"
        elif any(value is None for value in reported):
            state = "unsupported"
        elif all(
            history == "current" or (history == "before" and member.settings_revision == revision == lineage_revision)
            for history, member in zip(histories, fresh)
        ) and applied is not None and applied >= revision:
            state = "current"
        else:
            state = "pending"
        counts[state] += 1
        items.append(
            {
                "materialization_id": materialization.materialization_id,
                "name": materialization.name,
                "state": state,
                "revision": None if other_lineage else applied,
                "members": len(fresh),
            }
        )
    items.sort(key=lambda item: (item["state"] == "current", item["name"] or item["materialization_id"]))
    return {"revision": revision, "total": len(items), **counts, "materializations": items}


def _settings_history(member: Any, *, lineage: str | None, lineage_revision: int | None) -> str:
    """Which history of settings a member runs: `current`; `before` the current lineage began
    (settings without one, at or below the revision it began at); or `other` — an earlier
    installation's, or a lineage this control plane does not hold.

    A member from before lineages reports none, and counts as `current`: its revision is all there
    is to judge it by.
    """
    if member.settings_lineage is None or member.settings_lineage == (lineage or ""):
        return "current"
    if (
        member.settings_lineage == ""
        and lineage_revision is not None
        and member.settings_revision is not None
        and member.settings_revision <= lineage_revision
    ):
        return "before"
    return "other"


def _deployment_settings(scope: str, config: ControlPlaneConfig) -> list[dict[str, Any]]:
    component = "control-plane" if scope == CONTROL_PLANE_SCOPE else "runtime"
    runtime = runtime_deployment_config(config) if component == "runtime" else None
    entries: list[dict[str, Any]] = []
    for setting in DEPLOYMENT_SETTINGS:
        if setting.component != component:
            continue
        entries.append(
            {
                "key": setting.key,
                "component": setting.component,
                "label": setting.label,
                "description": setting.description,
                "env_var": setting.env_var,
                "value": _deployment_value(setting.key, config, runtime),
            }
        )
    return entries


def _deployment_value(key: str, config: ControlPlaneConfig, runtime: Any) -> Any:
    if key in ("auth_signing_key", "auth_service_token", "bootstrap_admin_email"):
        # Credentials are reported as configured or not, never by value. That includes the first
        # administrator's email: settings.view must not show an address users.view would hide.
        secrets = get_identity_secrets()
        if key == "auth_signing_key":
            if not secrets.can_sign_in:
                return "not configured: nobody can sign in"
            return "configured, with a previous key" if len(secrets.signing_keys) > 1 else "configured"
        if key == "auth_service_token":
            return "configured" if secrets.service_token else "not configured"
        return "configured" if secrets.bootstrap_admin is not None else "not configured"
    if key == "auth_federation_key":
        secrets = get_identity_secrets()
        current = secrets.current_federation_key
        if current is None:
            return "not configured: no access token can be made or checked"
        return f"{current.key_id}, with a previous key" if len(secrets.federation_keys) > 1 else current.key_id
    if key == "protection_key":
        key_object = get_protection_key()
        return "not configured: nothing can be hashed" if key_object is None else key_object.key_id
    if key == "domain":
        if config.domain_id is None:
            return "none: not part of a shared console"
        return f"{config.domain_name} ({config.domain_id})"
    if key.startswith("sso_"):
        return _describe_provider(key[len("sso_"):])
    if key == "package_revision":
        try:
            return get_installed_package_revision()
        except RuntimeError:
            return None
    if key == "resources":
        resources = config.runtime_app_spec.get("resources") if isinstance(config.runtime_app_spec, dict) else None
        return _describe_resources(resources)
    if key == "vertical_autoscaling":
        spec = config.runtime_app_spec if isinstance(config.runtime_app_spec, dict) else {}
        return _describe_vertical_autoscaling(spec.get("verticalPodAutoscaler"))
    if key == "storage_class":
        storage = config.runtime_app_spec.get("storage") if isinstance(config.runtime_app_spec, dict) else None
        return storage.get("class") if isinstance(storage, dict) else None
    if key in ("heartbeat_interval", "upload_interval"):
        return getattr(runtime, key, None)
    if key in _STORE_SETTINGS:
        return _store_setting(key)
    value = getattr(config, key, None)
    return value


#: Deployment settings that are the object stores' own (`StoreSettings`), by the field each reads.
_STORE_SETTINGS = {
    "storage_s3_endpoint": "s3_endpoint",
    "storage_s3_region": "s3_region",
    "storage_s3_path_style": "s3_path_style",
    "storage_azure_endpoint": "azure_endpoint",
}


def _store_setting(key: str) -> Any:
    """A `STORAGE_*` setting as this process's stores read it from the environment."""
    try:
        settings = StoreSettings.from_environment()
    except ValueError as error:
        return f"invalid: {error}"
    return getattr(settings, _STORE_SETTINGS[key])


def _describe_provider(provider: str) -> str:
    """How the deployment configured an identity provider; never a secret."""
    settings = get_provider_settings()
    if not settings.configured(provider):
        return "not configured"
    if provider == "entra":
        return f"tenant {settings.entra_tenant_id}, client {settings.entra_client_id}"
    if provider == "github":
        return f"client {settings.github_client_id}, secret configured"
    if provider == "google":
        hosted = ", ".join(settings.google_hosted_domains) or "any"
        return f"client {settings.google_client_id}, hosted domains: {hosted}"
    return f"{settings.oidc_issuer}, client {settings.oidc_client_id}"


def _describe_resources(resources: Any) -> str | None:
    if not isinstance(resources, dict):
        return None
    parts = []
    for kind in ("requests", "limits"):
        values = resources.get(kind)
        if isinstance(values, dict) and values:
            described = ", ".join(f"{name} {values[name]}" for name in sorted(values))
            parts.append(f"{kind}: {described}")
    return "; ".join(parts) or None


def _describe_vertical_autoscaling(autoscaler: Any) -> str:
    """The runtime spec's `verticalPodAutoscaler`, read with kaspr-operator's defaults for what it
    leaves out: the CPU requests only, resized in place."""
    if not isinstance(autoscaler, dict) or autoscaler.get("enabled") is not True:
        return "off"
    controlled = autoscaler.get("controlledResources") or ["cpu"]
    values = "requests" if (autoscaler.get("controlledValues") or "RequestsOnly") == "RequestsOnly" else "requests and limits"
    lowest = autoscaler.get("minAllowed") if isinstance(autoscaler.get("minAllowed"), dict) else {}
    highest = autoscaler.get("maxAllowed") if isinstance(autoscaler.get("maxAllowed"), dict) else {}
    parts = []
    for name in controlled:
        if name in lowest and name in highest:
            bounds = f" {lowest[name]}–{highest[name]}"
        elif name in lowest:
            bounds = f" from {lowest[name]}"
        elif name in highest:
            bounds = f" up to {highest[name]}"
        else:
            bounds = ""
        parts.append(f"{name} {values}{bounds}")
    parts.append(f"update mode {autoscaler.get('updateMode') or 'InPlaceOrRecreate'}")
    return ", ".join(parts)


def _parse_fallbacks(raw: Any, *, scope: str) -> dict[str, SettingFallback]:
    if raw is None:
        return {}
    if scope != CONSOLE_SCOPE:
        raise invalid_request("fallbacks are reported only for console settings", field_name="fallbacks")
    if not isinstance(raw, dict):
        raise invalid_request("fallbacks must be an object", field_name="fallbacks")
    parsed: dict[str, SettingFallback] = {}
    for key, value in raw.items():
        if not isinstance(value, dict):
            raise invalid_request("each fallback must be an object", field_name="fallbacks")
        try:
            parsed[key] = SettingFallback.from_dict(value)
        except (KeyError, ValueError) as error:
            raise invalid_request(f"fallback for {key} is invalid: {error}", field_name="fallbacks") from error
    return parsed


def _parse_history_limit(limit: str | int | None) -> int | None:
    if limit in (None, "", "all"):
        return None
    try:
        parsed = int(limit)
    except (TypeError, ValueError):
        raise invalid_request("limit must be a positive integer or 'all'", field_name="limit") from None
    if parsed < 1:
        raise invalid_request("limit must be a positive integer or 'all'", field_name="limit")
    return parsed


def _require_scope(scope: str) -> None:
    if scope not in SETTINGS_SCOPES:
        raise not_found(f"settings scope {scope} was not found", resource="settings-scope", identifier=scope)


def _is_fresh(reported_at: str | None, reference: datetime | None) -> bool:
    reported = _parse_timestamp(reported_at)
    if reported is None or reference is None:
        return False
    return (reference - reported).total_seconds() <= MEMBER_STATE_FRESHNESS_SECONDS


def _parse_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    normalized = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)


def _utc_now() -> str:
    return datetime.now(tz=UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
