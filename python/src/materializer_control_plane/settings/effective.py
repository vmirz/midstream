"""Effective configuration: the deployment's environment with administrator overrides applied.

Precedence, for every catalog setting: an override chosen from the console, else the value the
deployment's environment sets, else the built-in default. Environment-only settings are never
overridden.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, fields, replace
import logging
from typing import Any

from materializer_control_plane.config import ControlPlaneConfig, deployment_environment, get_config
from materializer_control_plane.models import SettingsDocument
from materializer_control_plane.runtime_app.config import RuntimeAppConfig, runtime_config_from_environment
from materializer_control_plane.settings.catalog import (
    CATALOG,
    SettingDefinition,
    SettingValueError,
    coerce_setting_value,
)
from materializer_control_plane.storage.settings import get_settings_document
from materializer_control_plane.storage.table import TableProtocol

LOGGER = logging.getLogger(__name__)

CONTROL_PLANE_SCOPE = "control-plane"
RUNTIME_SCOPE = "runtime"
CONSOLE_SCOPE = "console"


@dataclass(frozen=True, slots=True)
class EffectiveSetting:
    """A catalog setting's value as it applies now, and every value it could fall back to."""

    definition: SettingDefinition
    value: Any
    source: str
    default_value: Any
    deployment_value: Any
    deployment_set: bool
    overridden: bool
    updated_at: str | None = None
    updated_by: str | None = None

    def describe(self) -> dict[str, Any]:
        """The setting as the settings API reports it: definition, value and provenance."""
        return {
            **self.definition.describe(),
            "value": self.value,
            "source": self.source,
            "default_value": self.default_value,
            "deployment_value": self.deployment_value,
            "deployment_set": self.deployment_set,
            "overridden": self.overridden,
            "updated_at": self.updated_at,
            "updated_by": self.updated_by,
        }


# The last merged config, kept with what it was built from so a new override or a different base
# never reads a stale merge. Resolved once per event, so this keeps the common case to one lookup.
_merged: tuple[ControlPlaneConfig, int, str | None, ControlPlaneConfig] | None = None


def resolve_config(
    settings_table: TableProtocol | None,
    *,
    base: ControlPlaneConfig | None = None,
) -> ControlPlaneConfig:
    """The control plane's configuration with any administrator overrides applied.

    Every handler resolves this once per event and passes it down, so a change made from the
    console applies to the next event an agent handles and the next request a view answers. With
    no settings table — tests, and callers that predate settings — the environment applies alone.
    """
    global _merged
    base_config = get_config() if base is None else base
    document = get_settings_document(settings_table, CONTROL_PLANE_SCOPE)
    if document is None or not document.values:
        return base_config
    cached = _merged
    if (
        cached is not None
        and cached[0] is base_config
        and cached[1] == document.revision
        and cached[2] == document.updated_at
    ):
        return cached[3]
    merged = apply_control_plane_overrides(base_config, document)
    _merged = (base_config, document.revision, document.updated_at, merged)
    return merged


def apply_control_plane_overrides(base: ControlPlaneConfig, document: SettingsDocument) -> ControlPlaneConfig:
    """`base` with every valid override in `document` applied."""
    overrides: dict[str, Any] = {}
    for definition in CATALOG[CONTROL_PLANE_SCOPE]:
        override = document.values.get(definition.key)
        if override is None:
            continue
        try:
            overrides[definition.key] = coerce_setting_value(definition, override.value)
        except SettingValueError as error:
            # Stored before a rule tightened, perhaps; the deployment value applies instead.
            LOGGER.warning("Ignoring stored override for %s: %s", definition.key, error)
    merged = replace(base, **overrides)
    if merged.default_runtime_replicas > merged.max_runtime_replicas:
        # Each value is valid alone, but the deployment and an override can disagree; a create must
        # never default to more replicas than it is allowed.
        merged = replace(merged, default_runtime_replicas=merged.max_runtime_replicas)
    return merged


def runtime_environment(config: ControlPlaneConfig) -> dict[str, str]:
    """The environment every runtime is deployed with, from the runtime spec's container env."""
    template = config.runtime_app_spec.get("template") if isinstance(config.runtime_app_spec, dict) else None
    container = template.get("kasprContainer") if isinstance(template, dict) else None
    entries = container.get("env") if isinstance(container, dict) else None
    environment: dict[str, str] = {}
    for entry in entries or []:
        if isinstance(entry, dict) and isinstance(entry.get("name"), str) and "value" in entry:
            environment[entry["name"]] = "" if entry["value"] is None else str(entry["value"])
    return environment


def runtime_deployment_config(config: ControlPlaneConfig) -> RuntimeAppConfig:
    """What a runtime's configuration amounts to before any override."""
    return runtime_config_from_environment(runtime_environment(config))


def effective_runtime_config(
    settings_table: TableProtocol | None,
    config: ControlPlaneConfig | None = None,
) -> RuntimeAppConfig:
    """What a runtime's configuration amounts to with the stored runtime overrides applied.

    The control plane uses it where it acts on a runtime's behalf: discovery evolves schema 0
    under the runtimes' schema caps, table creation records their retention settings, and the
    maintenance app's ticks carry their maintenance settings.
    """
    base = runtime_deployment_config(get_config() if config is None else config)
    document = get_settings_document(settings_table, RUNTIME_SCOPE)
    if document is None or not document.values:
        return base
    overrides: dict[str, Any] = {}
    for definition in CATALOG[RUNTIME_SCOPE]:
        override = document.values.get(definition.key)
        if override is None:
            continue
        try:
            overrides[definition.key] = coerce_setting_value(definition, override.value)
        except SettingValueError:
            continue
    try:
        return replace(base, **overrides)
    except (TypeError, ValueError) as error:
        LOGGER.warning("Ignoring stored runtime overrides that do not combine: %s", error)
        return base


def describe_settings(
    scope: str,
    document: SettingsDocument | None,
    *,
    config: ControlPlaneConfig | None = None,
    environ: Mapping[str, str] | None = None,
) -> list[EffectiveSetting]:
    """Every catalog setting of a scope with its effective value and where that came from."""
    base_config = get_config() if config is None else config
    if scope == CONTROL_PLANE_SCOPE:
        deployment: Any = base_config
        defaults: Any = ControlPlaneConfig()
        environment = deployment_environment() if environ is None else environ
    elif scope == RUNTIME_SCOPE:
        environment = runtime_environment(base_config) if environ is None else environ
        deployment = runtime_config_from_environment(environment)
        defaults = RuntimeAppConfig()
    else:
        raise ValueError(f"scope {scope!r} has no catalog")

    settings: list[EffectiveSetting] = []
    for definition in CATALOG[scope]:
        default_value = getattr(defaults, definition.key)
        deployment_set = definition.env_var in environment
        deployment_value = getattr(deployment, definition.key) if deployment_set else None
        fallback = deployment_value if deployment_set else default_value
        fallback_source = "deployment" if deployment_set else "default"
        override = None if document is None else document.values.get(definition.key)
        value, source = fallback, fallback_source
        if override is not None:
            try:
                value, source = coerce_setting_value(definition, override.value), "override"
            except SettingValueError:
                override = None
        settings.append(
            EffectiveSetting(
                definition=definition,
                value=value,
                source=source,
                default_value=default_value,
                deployment_value=deployment_value,
                deployment_set=deployment_set,
                overridden=override is not None,
                updated_at=None if override is None else override.updated_at,
                updated_by=None if override is None else override.updated_by,
            )
        )
    return settings


def effective_values(settings: list[EffectiveSetting]) -> dict[str, tuple[Any, str]]:
    """Each setting's effective value and source, by key."""
    return {setting.definition.key: (setting.value, setting.source) for setting in settings}


def control_plane_config_fields() -> frozenset[str]:
    """Fields of `ControlPlaneConfig`; every control-plane catalog key must be one."""
    return frozenset(item.name for item in fields(ControlPlaneConfig))
