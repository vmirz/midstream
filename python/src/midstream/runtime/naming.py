"""Deterministic runtime naming helpers."""

from __future__ import annotations

import hashlib

from midstream.config import ControlPlaneConfig, get_config
from midstream.models import Materialization

MAX_KUBERNETES_NAME_LENGTH = 63
MAX_RUNTIME_APP_NAME_LENGTH = 59
MAX_RUNTIME_TABLE_NAME_LENGTH = 57
TABLE_COMPONENT_SUFFIXES = {"state"}


def normalize_runtime_app_name(runtime_app_name: str) -> str:
    """Bound runtime app names so operator-derived StatefulSet names stay valid."""
    return _bounded_name(runtime_app_name, max_length=MAX_RUNTIME_APP_NAME_LENGTH)


def runtime_component_name(runtime_app_name: str, component_suffix: str) -> str:
    """Build a deterministic runtime child-resource name from the app name."""
    normalized_app_name = normalize_runtime_app_name(runtime_app_name)
    max_length = (
        MAX_RUNTIME_TABLE_NAME_LENGTH
        if component_suffix in TABLE_COMPONENT_SUFFIXES
        else MAX_KUBERNETES_NAME_LENGTH
    )
    candidate = f"{normalized_app_name}-{component_suffix}"
    if len(candidate) <= max_length:
        return candidate
    digest = hashlib.sha1(normalized_app_name.encode("utf-8")).hexdigest()[:8]
    prefix_length = max_length - len(component_suffix) - len(digest) - 2
    bounded_prefix = normalized_app_name[:prefix_length].rstrip("-")
    return f"{bounded_prefix}-{digest}-{component_suffix}"


def runtime_component_names(runtime_app_name: str) -> dict[str, str]:
    """Return the standard runtime child-resource names for a dedicated app."""
    return {
        "writer_agent_name": runtime_component_name(runtime_app_name, "writer"),
        "committer_agent_name": runtime_component_name(runtime_app_name, "committer"),
        "settings_agent_name": runtime_component_name(runtime_app_name, "settings"),
        "status_task_name": runtime_component_name(runtime_app_name, "emit-status"),
        "pipeline_task_name": runtime_component_name(runtime_app_name, "pipeline"),
        "commit_timer_task_name": runtime_component_name(runtime_app_name, "commit-timer"),
        "commits_topic_name": runtime_component_name(runtime_app_name, "commits"),
        "state_table_name": runtime_component_name(runtime_app_name, "state"),
    }


#: Child resources an earlier version rendered and this one does not, by kind and name suffix. An
#: apply deletes them: a runtime ran its own table maintenance before 2.0, ticked by this task.
RETIRED_RUNTIME_COMPONENTS = (("KasprTask", "maintenance-timer"),)


def retired_runtime_components(runtime_app_name: str) -> list[tuple[str, str]]:
    """(kind, name) of every child resource of the runtime that an apply deletes if it is there."""
    return [(kind, runtime_component_name(runtime_app_name, suffix)) for kind, suffix in RETIRED_RUNTIME_COMPONENTS]


def runtime_consumer_group_name(runtime_app_name: str) -> str:
    """Return the dedicated runtime consumer-group name used by the app StatefulSet."""
    return runtime_component_name(runtime_app_name, "app")


def resolve_runtime_app_name(
    materialization: Materialization,
    *,
    config: ControlPlaneConfig | None = None,
) -> str:
    """The runtime app name a materialization was given, or the one its id derives."""
    cfg = get_config() if config is None else config
    return normalize_runtime_app_name(
        materialization.resolved_runtime_name
        or cfg.runtime_name_for_materialization(materialization.materialization_id)
    )


def _bounded_name(name: str, *, max_length: int) -> str:
    if len(name) <= max_length:
        return name
    digest = hashlib.sha1(name.encode("utf-8")).hexdigest()[:8]
    prefix_length = max_length - len(digest) - 1
    bounded_prefix = name[:prefix_length].rstrip("-")
    return f"{bounded_prefix}-{digest}"