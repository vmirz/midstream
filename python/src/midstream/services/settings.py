"""Applying settings changes: the settings agent's only job.

The settings API validates a change and publishes it; this applies it, in order, per scope. It
checks again against what is stored now — another change may have landed in between — records
what changed, and hands runtime settings to every running runtime.

At each start the control plane also asks it to give the runtime settings a lineage if they have
none (`bootstrap_runtime_settings`): every delivery carries it, and a runtime keeps the newest
settings by lineage, then revision, so it follows this control plane even over a delivery topic an
earlier installation left behind.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import replace
from datetime import UTC, datetime
import logging
import math
import re
import secrets
from typing import Any

from midstream.config import ControlPlaneConfig, get_config
from midstream.models import (
    SettingChange,
    SettingOverride,
    SettingsBootstrapRequest,
    SettingsChangeRecord,
    SettingsChangeRequest,
    SettingsDocument,
    SettingsRejection,
)
from midstream.models.settings import RECENT_REQUEST_LIMIT
from midstream.protocol_logging import log_protocol_event
from midstream.runtime_app.handlers.runtime_settings import (
    BROADCAST_MATERIALIZATION_ID,
    RUNTIME_SETTINGS_KIND,
)
from midstream.scheduler import build_direct_topic_output
from midstream.settings.catalog import (
    CATALOG,
    SettingValueError,
    coerce_setting_value,
    definition_for,
    validate_scope_values,
)
from midstream.settings.effective import (
    CONSOLE_SCOPE,
    RUNTIME_SCOPE,
    describe_settings,
    effective_values,
)
from midstream.storage.settings import (
    get_settings_document,
    save_settings_change,
    save_settings_document,
)
from midstream.storage.table import TableProtocol

LOGGER = logging.getLogger(__name__)

#: Kafka key of runtime-settings deliveries on the schema-authority-updates topic.
RUNTIME_SETTINGS_ROUTING_KEY = "runtime-settings"

#: Console settings are the console's to describe; the control plane only bounds what it stores.
CONSOLE_KEY_PATTERN = re.compile(r"^[A-Za-z][A-Za-z0-9_.-]{0,63}$")
CONSOLE_MAX_SETTINGS = 200
CONSOLE_MAX_STRING_LENGTH = 2048


def apply_settings_change(
    settings_table: TableProtocol,
    request: SettingsChangeRequest,
    *,
    config: ControlPlaneConfig | None = None,
    environ: Mapping[str, str] | None = None,
    applied_at: str | None = None,
) -> list[dict[str, Any]]:
    """Apply one change request to its scope and return the routed outputs it produces.

    A request this scope has already applied changes nothing, so a redelivered request is
    harmless. A request read against an older revision, or one whose values no longer fit the
    stored ones, is refused: the refusal is stored on the scope, where the console looks for it.
    """
    cfg = get_config() if config is None else config
    now = applied_at or _utc_now()
    document = get_settings_document(settings_table, request.scope) or SettingsDocument(scope=request.scope)
    if request.request_id in document.recent_request_ids:
        return []

    if request.base_revision is not None and request.base_revision != document.revision:
        _reject(
            settings_table,
            document,
            request,
            reason="settings_changed",
            message=(
                f"These settings changed after they were read (revision {request.base_revision}, "
                f"now {document.revision}). Reload them and try again."
            ),
            now=now,
            config=cfg,
        )
        return []

    try:
        values = _apply_patch(document, request, now=now, config=cfg)
        changes = _describe_changes(document, values, request, config=cfg, environ=environ)
    except SettingValueError as error:
        _reject(
            settings_table,
            document,
            request,
            reason="invalid_setting",
            message=str(error),
            now=now,
            config=cfg,
        )
        return []

    recent = [*document.recent_request_ids, request.request_id][-RECENT_REQUEST_LIMIT:]
    if not changes:
        # Nothing to change, but the request still counts as applied for whoever waits on it.
        save_settings_document(
            settings_table,
            replace(document, recent_request_ids=recent),
            updated_at=document.updated_at or now,
        )
        return []

    updated = replace(
        document,
        revision=document.revision + 1,
        values=values,
        updated_at=now,
        updated_by=request.submitted_by,
        recent_request_ids=recent,
    )
    if request.scope == RUNTIME_SCOPE and not document.lineage:
        # Normally begun at startup; a change that comes first begins it instead.
        updated = replace(updated, lineage=new_settings_lineage(now), lineage_revision=document.revision)
    save_settings_document(settings_table, updated, updated_at=now)
    save_settings_change(
        settings_table,
        SettingsChangeRecord(
            scope=request.scope,
            revision=updated.revision,
            request_id=request.request_id,
            updated_at=now,
            updated_by=request.submitted_by,
            changes=changes,
        ),
    )
    log_protocol_event(
        LOGGER,
        enabled=cfg.protocol_logging_enabled,
        event="control_plane_settings_applied",
        scope=request.scope,
        request_id=request.request_id,
        settings_revision=updated.revision,
        settings_lineage=updated.lineage,
        changed=[change.key for change in changes],
        submitted_by=request.submitted_by,
    )
    if request.scope != RUNTIME_SCOPE:
        return []
    return [
        build_direct_topic_output(
            topic=cfg.schema_authority_updates_topic,
            key=RUNTIME_SETTINGS_ROUTING_KEY,
            value=build_runtime_settings_message(updated, published_at=now),
        )
    ]


def bootstrap_runtime_settings(
    settings_table: TableProtocol,
    request: SettingsBootstrapRequest,
    *,
    config: ControlPlaneConfig | None = None,
) -> list[dict[str, Any]]:
    """Begin a lineage for the runtime settings if they have none, and deliver them under it.

    The `settings-bootstrap` task asks for this at every start; only a start on settings without a
    lineage changes anything: a new installation, the first start after an upgrade from before
    lineages, or a start after the `settings` table was lost. The delivery goes to every runtime:
    one running settings of an earlier history — an earlier installation's, read back from the
    delivery topic — drops them for these, and one already on these values learns their lineage.
    """
    if request.scope != RUNTIME_SCOPE:
        return []
    cfg = get_config() if config is None else config
    document = get_settings_document(settings_table, RUNTIME_SCOPE) or SettingsDocument(scope=RUNTIME_SCOPE)
    if document.lineage:
        return []
    now = request.requested_at
    updated = replace(document, lineage=new_settings_lineage(now), lineage_revision=document.revision)
    save_settings_document(settings_table, updated, updated_at=document.updated_at or now)
    LOGGER.info(
        "Runtime settings begin lineage %s at revision %d; delivering them to every runtime",
        updated.lineage,
        updated.revision,
    )
    log_protocol_event(
        LOGGER,
        enabled=cfg.protocol_logging_enabled,
        event="runtime_settings_lineage_begun",
        settings_lineage=updated.lineage,
        settings_revision=updated.revision,
    )
    return [
        build_direct_topic_output(
            topic=cfg.schema_authority_updates_topic,
            key=RUNTIME_SETTINGS_ROUTING_KEY,
            value=build_runtime_settings_message(updated, published_at=now),
        )
    ]


def new_settings_lineage(begun_at: str) -> str:
    """A new lineage for the runtime settings: when it began, then a random part.

    The time comes first, in a fixed width, so lineages compare as strings by when they began;
    runtimes rely on that to follow the newest (`runtime_app/live_settings.py`). An unreadable
    time falls back to the clock's.
    """
    try:
        moment = datetime.fromisoformat(begun_at.replace("Z", "+00:00"))
    except (AttributeError, ValueError):
        moment = datetime.now(tz=UTC)
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=UTC)
    return f"{moment.astimezone(UTC).strftime('%Y%m%dT%H%M%SZ')}-{secrets.token_hex(4)}"


def build_runtime_settings_message(
    document: SettingsDocument,
    *,
    published_at: str,
    target_materialization_id: str | None = None,
) -> dict[str, Any]:
    """The schema-authority-updates message that carries runtime settings to runtimes.

    It carries every override, not the change, so whichever delivery a runtime sees last leaves
    it on the right values, and the settings' lineage, by which a runtime tells this history from
    an earlier one. Addressed to `*`, which runtimes too old to understand it filter out;
    `target_materialization_id` narrows it to one runtime, as when a runtime is (re)deployed.
    """
    return {
        "kind": RUNTIME_SETTINGS_KIND,
        "materialization_id": BROADCAST_MATERIALIZATION_ID,
        "target_materialization_id": target_materialization_id,
        "revision": document.revision,
        "lineage": document.lineage,
        "values": document.override_values(),
        "published_at": published_at,
    }


def runtime_settings_output_for_apply(
    settings_table: TableProtocol | None,
    *,
    materialization_id: str,
    published_at: str,
    config: ControlPlaneConfig,
) -> dict[str, Any] | None:
    """The delivery that brings a runtime being (re)deployed onto the current runtime settings.

    Sent with no change made too, once the settings have a lineage: a new runtime reads the
    delivery topic from its start, and must not keep what an earlier installation left there.
    None when the settings have neither changes nor a lineage: the runtime's environment is right.
    """
    document = get_settings_document(settings_table, RUNTIME_SCOPE)
    if document is None or (document.revision < 1 and not document.lineage):
        return None
    return build_direct_topic_output(
        topic=config.schema_authority_updates_topic,
        key=RUNTIME_SETTINGS_ROUTING_KEY,
        value=build_runtime_settings_message(
            document,
            published_at=published_at,
            target_materialization_id=materialization_id,
        ),
    )


def validate_change(
    document: SettingsDocument | None,
    request: SettingsChangeRequest,
    *,
    config: ControlPlaneConfig | None = None,
    environ: Mapping[str, str] | None = None,
) -> None:
    """Raise `SettingValueError` when a request cannot apply to the stored settings.

    The API runs this before accepting a request, so most mistakes come back as a 400 instead of
    a refusal found later.
    """
    cfg = get_config() if config is None else config
    current = document or SettingsDocument(scope=request.scope)
    values = _apply_patch(current, request, now=request.submitted_at, config=cfg)
    _describe_changes(current, values, request, config=cfg, environ=environ)


def _apply_patch(
    document: SettingsDocument,
    request: SettingsChangeRequest,
    *,
    now: str,
    config: ControlPlaneConfig,
) -> dict[str, SettingOverride]:
    values = dict(document.values)
    for key in request.reset:
        _require_known(request.scope, key)
        values.pop(key, None)
    for key, raw in request.values.items():
        if request.scope == CONSOLE_SCOPE:
            value = _validate_console_value(key, raw)
        else:
            definition = _require_known(request.scope, key)
            value = coerce_setting_value(definition, raw, allow_file_destinations=config.allow_file_destinations)
        current = values.get(key)
        if current is not None and current.value == value:
            continue
        values[key] = SettingOverride(value=value, updated_at=now, updated_by=request.submitted_by)
    if request.scope == CONSOLE_SCOPE and len(values) > CONSOLE_MAX_SETTINGS:
        raise SettingValueError(
            f"the console may store at most {CONSOLE_MAX_SETTINGS} settings",
            key=next(iter(request.values), "values"),
        )
    return values


def _describe_changes(
    document: SettingsDocument,
    values: dict[str, SettingOverride],
    request: SettingsChangeRequest,
    *,
    config: ControlPlaneConfig | None,
    environ: Mapping[str, str] | None,
) -> list[SettingChange]:
    """What the change does to each setting's effective value, after checking the result works."""
    if request.scope == CONSOLE_SCOPE:
        return _describe_console_changes(document, values, request)
    after_document = replace(document, values=values)
    before = effective_values(describe_settings(request.scope, document, config=config, environ=environ))
    after = effective_values(describe_settings(request.scope, after_document, config=config, environ=environ))
    validate_scope_values(request.scope, {key: value for key, (value, _) in after.items()})
    changes: list[SettingChange] = []
    for definition in CATALOG[request.scope]:
        if before[definition.key] == after[definition.key]:
            continue
        (before_value, before_source), (after_value, after_source) = before[definition.key], after[definition.key]
        changes.append(
            SettingChange(
                key=definition.key,
                before=before_value,
                after=after_value,
                before_source=before_source,
                after_source=after_source,
            )
        )
    return changes


def _describe_console_changes(
    document: SettingsDocument,
    values: dict[str, SettingOverride],
    request: SettingsChangeRequest,
) -> list[SettingChange]:
    changes: list[SettingChange] = []
    for key in sorted(set(document.values) | set(values)):
        before = document.values.get(key)
        after = values.get(key)
        if (before is None and after is None) or (
            before is not None and after is not None and before.value == after.value
        ):
            continue
        fallback = request.fallbacks.get(key)
        fallback_value = None if fallback is None else fallback.value
        fallback_source = "default" if fallback is None else fallback.source
        changes.append(
            SettingChange(
                key=key,
                before=fallback_value if before is None else before.value,
                after=fallback_value if after is None else after.value,
                before_source=fallback_source if before is None else "override",
                after_source=fallback_source if after is None else "override",
            )
        )
    return changes


def _require_known(scope: str, key: str):
    if scope == CONSOLE_SCOPE:
        if not isinstance(key, str) or not CONSOLE_KEY_PATTERN.match(key):
            raise SettingValueError(f"{key!r} is not a valid console setting name", key=str(key))
        return None
    definition = definition_for(scope, key)
    if definition is None:
        raise SettingValueError(f"{key} is not a {scope} setting that can be changed", key=key)
    return definition


def _validate_console_value(key: str, raw: Any) -> Any:
    _require_known(CONSOLE_SCOPE, key)
    if raw is None or isinstance(raw, bool):
        return raw
    if isinstance(raw, int):
        return raw
    if isinstance(raw, float):
        if not math.isfinite(raw):
            raise SettingValueError(f"{key} must be a finite number", key=key)
        return raw
    if isinstance(raw, str):
        if len(raw) > CONSOLE_MAX_STRING_LENGTH:
            raise SettingValueError(
                f"{key} must be at most {CONSOLE_MAX_STRING_LENGTH} characters",
                key=key,
            )
        return raw
    raise SettingValueError(f"{key} must be a string, number, boolean or null", key=key)


def _reject(
    settings_table: TableProtocol,
    document: SettingsDocument,
    request: SettingsChangeRequest,
    *,
    reason: str,
    message: str,
    now: str,
    config: ControlPlaneConfig,
) -> None:
    rejected = replace(
        document,
        last_rejection=SettingsRejection(
            request_id=request.request_id,
            reason=reason,
            message=message,
            rejected_at=now,
        ),
    )
    save_settings_document(settings_table, rejected, updated_at=document.updated_at or now)
    log_protocol_event(
        LOGGER,
        enabled=config.protocol_logging_enabled,
        event="control_plane_settings_rejected",
        scope=request.scope,
        request_id=request.request_id,
        reason=reason,
        message=message,
    )


def _utc_now() -> str:
    return datetime.now(tz=UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
