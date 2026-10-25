"""CRD template input and rendering helpers."""

from __future__ import annotations

import hashlib
from importlib import metadata as importlib_metadata
from importlib import resources
import json
from pathlib import Path
from typing import Any

from jinja2 import Environment, FileSystemLoader, StrictUndefined
import yaml

from materializer_control_plane.config import ControlPlaneConfig, get_config
from materializer_control_plane.models import Materialization, MaterializationTable
from materializer_control_plane.runtime.naming import resolve_runtime_app_name, runtime_component_names
from materializer_control_plane.runtime_app.config import get_runtime_app_config

PACKAGE_DISTRIBUTION_NAME = "materializer-control-plane"
PACKAGE_REPOSITORY_URL = "git+https://github.com/vmirz/midstream.git"
#: The runtime's RocksDB write buffer and how many it keeps: its state table's write-ahead log holds
#: at most their product before a flush (`_default_runtime_spec`).
STATE_STORE_WRITE_BUFFER_BYTES = 4 * 1024 * 1024
STATE_STORE_WRITE_BUFFERS = 2


def build_dedicated_runtime_template_input(
    materialization: Materialization,
    *,
    table_records: list[MaterializationTable],
    config: ControlPlaneConfig | None = None,
    managed_agent_names: list[str] | None = None,
    scaled_to_zero: bool = False,
) -> dict[str, Any]:
    """Build the Jinja input used to render a dedicated Kaspr runtime: one app for every table.

    The runtime reads every source topic of the materialization and writes each into its own
    table, so it renders with all of them, in the materialization's order. Nothing schema- or
    settings-dependent goes in: the runtime reads its schemas from its tables and its live settings
    from deliveries, so neither changes the spec hash (and restarts nothing). `scaled_to_zero` holds
    it at no members, as a change of its topics does until its consumer group has emptied.
    """
    cfg = get_config() if config is None else config
    if materialization.runtime_mode != "dedicated":
        raise ValueError("only dedicated runtime mode is supported in MVP")
    by_name = {}
    for record in table_records:
        if record.materialization_id != materialization.materialization_id:
            raise ValueError("every table record must belong to the materialization")
        by_name[record.name] = record
    missing = [table.name for table in materialization.tables if table.name not in by_name]
    if missing:
        raise ValueError(f"the runtime needs every table; not created yet: {', '.join(missing)}")
    tables = [
        {
            "name": table.name,
            # The table's Iceberg namespace: each table has its own dataset.
            "dataset": table.dataset,
            "source_topic": table.source_topic,
            "location": by_name[table.name].location,
            # Which incarnation of the name: a topic removed and added back has a table of its own.
            "table_uuid": by_name[table.name].table_uuid,
        }
        for table in materialization.tables
    ]
    runtime_app_name = resolve_runtime_app_name(materialization, config=cfg)
    runtime_component_config = get_runtime_app_config()
    component_names = runtime_component_names(runtime_app_name)
    package_revision = get_installed_package_revision()
    resolved_agent_names = (
        [
            component_names["writer_agent_name"],
            component_names["committer_agent_name"],
            component_names["settings_agent_name"],
        ]
        if managed_agent_names is None
        else managed_agent_names
    )
    return {
        "materialization_id": materialization.materialization_id,
        "runtime_mode": "dedicated",
        "runtime_app_name": runtime_app_name,
        "namespace": materialization.namespace,
        "source_topics": list(materialization.source_topics),
        "destination_uri": materialization.destination_uri,
        "tables": tables,
        "partition_granularity": materialization.partition_granularity,
        "managed_agent_names": list(resolved_agent_names),
        "resource_profile": materialization.resource_profile,
        "storage_size": materialization.storage_size,
        "replicas": materialization.replicas,
        "skip_tombstones": materialization.skip_tombstones,
        "labels": dict(materialization.labels),
        "annotations": dict(materialization.annotations),
        **{name: component_names[name] for name in RUNTIME_COMPONENT_KEYS},
        "heartbeat_interval": runtime_component_config.heartbeat_interval,
        "upload_interval": runtime_component_config.upload_interval,
        "commit_timer_interval": runtime_component_config.commit_timer_interval,
        "materialization_status_topic": cfg.materialization_status_topic,
        "schema_authority_updates_topic": cfg.schema_authority_updates_topic,
        "table_events_topic": cfg.table_events_topic,
        "package_revision": package_revision,
        "runtime_spec": _pin_runtime_package_revision(
            _merge_runtime_spec(
                _merge_runtime_spec(
                    _default_runtime_spec(cfg),
                    cfg.runtime_app_spec,
                ),
                _runtime_scale_spec(materialization, scaled_to_zero=scaled_to_zero),
            ),
            package_revision,
        ),
        "template_version": cfg.default_runtime_template_version,
    }


#: Component names every runtime template may refer to (`runtime/naming.py`).
RUNTIME_COMPONENT_KEYS = (
    "writer_agent_name",
    "committer_agent_name",
    "settings_agent_name",
    "status_task_name",
    "pipeline_task_name",
    "commit_timer_task_name",
    "commits_topic_name",
    "state_table_name",
)

#: Template file per rendered resource, in the order they are applied (app, table, agents, tasks).
RUNTIME_TEMPLATES = {
    "kaspr_app": "kaspr-app.yaml.j2",
    "kaspr_tables": ("state-table.yaml.j2",),
    "kaspr_agents": ("writer-agent.yaml.j2", "committer-agent.yaml.j2", "settings-agent.yaml.j2"),
    "kaspr_tasks": (
        "status-task.yaml.j2",
        "pipeline-task.yaml.j2",
        "commit-timer-task.yaml.j2",
    ),
}


def render_dedicated_runtime_resources(template_input: dict[str, Any]) -> dict[str, Any]:
    """Render the dedicated runtime template into concrete Kubernetes resources."""
    shared_labels = {
        "materialization_id": template_input["materialization_id"],
        **template_input.get("labels", {}),
    }
    shared_annotations = {
        **template_input.get("annotations", {}),
        "control-plane/template-version": str(template_input["template_version"]),
        "control-plane/package-revision": template_input["package_revision"],
        "control-plane/source-topics": ",".join(template_input["source_topics"]),
        "control-plane/destination-uri": template_input["destination_uri"],
        "control-plane/tables": ",".join(table["name"] for table in template_input["tables"]),
    }
    environment = _runtime_template_environment()
    render_context = {
        "runtime_app_name": template_input["runtime_app_name"],
        "namespace": template_input["namespace"],
        "labels": shared_labels,
        "annotations": shared_annotations,
        "runtime_spec": template_input["runtime_spec"],
        "materialization_id": template_input["materialization_id"],
        "source_topics": template_input["source_topics"],
        "tables": template_input["tables"],
        "partition_granularity": template_input["partition_granularity"],
        "skip_tombstones": template_input.get("skip_tombstones", False),
        **{name: template_input[name] for name in RUNTIME_COMPONENT_KEYS},
        "heartbeat_interval": template_input["heartbeat_interval"],
        "upload_interval": template_input["upload_interval"],
        "commit_timer_interval": template_input["commit_timer_interval"],
        "materialization_status_topic": template_input["materialization_status_topic"],
        "schema_authority_updates_topic": template_input["schema_authority_updates_topic"],
        "table_events_topic": template_input["table_events_topic"],
    }

    def render(name: str) -> dict[str, Any]:
        return yaml.safe_load(environment.get_template(name).render(**render_context))

    return {
        "kaspr_app": render(RUNTIME_TEMPLATES["kaspr_app"]),
        "kaspr_agents": [render(name) for name in RUNTIME_TEMPLATES["kaspr_agents"]],
        "kaspr_tasks": [render(name) for name in RUNTIME_TEMPLATES["kaspr_tasks"]],
        "kaspr_tables": [render(name) for name in RUNTIME_TEMPLATES["kaspr_tables"]],
    }


def iter_rendered_resources(rendered_resources: dict[str, Any]) -> list[dict[str, Any]]:
    """Return rendered resources in the order they should be applied."""
    ordered_resources: list[dict[str, Any]] = []
    kaspr_app = rendered_resources.get("kaspr_app")
    if kaspr_app is not None:
        ordered_resources.append(kaspr_app)
    for resource in rendered_resources.get("kaspr_tables", []):
        ordered_resources.append(resource)
    for resource in rendered_resources.get("kaspr_agents", []):
        ordered_resources.append(resource)
    for resource in rendered_resources.get("kaspr_tasks", []):
        ordered_resources.append(resource)
    return ordered_resources


def spec_hash(rendered_resources: dict[str, Any]) -> str:
    """Compute a stable hash for rendered runtime resources."""
    encoded = json.dumps(rendered_resources, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _runtime_template_environment() -> Environment:
    template_root = resources.files("materializer_control_plane.runtime_app").joinpath("templates")
    environment = Environment(
        loader=FileSystemLoader(str(Path(template_root))),
        autoescape=False,
        trim_blocks=True,
        lstrip_blocks=True,
        undefined=StrictUndefined,
    )
    environment.filters["to_yaml"] = _to_yaml
    environment.filters["to_python"] = _to_python
    return environment


def get_installed_package_revision(
    package_name: str = PACKAGE_DISTRIBUTION_NAME,
) -> str:
    """Return the installed VCS commit for the control-plane package."""
    try:
        distribution = importlib_metadata.distribution(package_name)
    except importlib_metadata.PackageNotFoundError as exc:
        raise RuntimeError(
            f"installed distribution metadata for {package_name} is required"
        ) from exc

    direct_url_text = distribution.read_text("direct_url.json")
    if not direct_url_text:
        raise RuntimeError(
            f"installed distribution metadata for {package_name} is missing direct_url.json"
        )

    try:
        direct_url = json.loads(direct_url_text)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"installed distribution metadata for {package_name} has invalid direct_url.json"
        ) from exc

    commit_id = direct_url.get("vcs_info", {}).get("commit_id")
    if not isinstance(commit_id, str) or not commit_id.strip():
        raise RuntimeError(
            f"installed distribution metadata for {package_name} is missing vcs_info.commit_id"
        )
    return commit_id.strip()


def _default_runtime_spec(config: ControlPlaneConfig) -> dict[str, Any]:
    return {
        "config": {
            "valueSerializer": "json",
            "topicAllowDeclare": True,
        },
        "template": {
            "kasprContainer": {
                "env": [
                    {
                        "name": "MATERIALIZER_RUNTIME_MEMBER_ID",
                        "valueFrom": {
                            "fieldRef": {
                                "fieldPath": "metadata.name",
                            }
                        },
                    },
                    # The writer holds back acknowledgements until rows are durable
                    # (runtime_app/acks.py). Faust's default waits on a rebalance, and at shutdown,
                    # until every delivered record is acknowledged - which a held one is not until
                    # its batch uploads - so it would stall both. Not waiting loses nothing: an
                    # unacknowledged record is read again from the committed offset.
                    {
                        "name": "K_STREAM_WAIT_EMPTY",
                        "value": "false",
                    },
                    # The state table is the runtime's one RocksDB store, on the member's volume
                    # (storage_size, 64Mi by default). RocksDB keeps every write in its write-ahead
                    # log until the memtable holding it is flushed, at Kaspr's 64 MiB by default:
                    # more than the volume holds, for a store whose few keys every commit rewrites.
                    # Flushing every 4 MiB keeps the log under two buffers. Set for the app, as a
                    # KasprTable's own `options` do not survive kaspr-operator's CRD.
                    {
                        "name": "K_STORE_ROCKSDB_WRITE_BUFFER_SIZE",
                        "value": str(STATE_STORE_WRITE_BUFFER_BYTES),
                    },
                    {
                        "name": "K_STORE_ROCKSDB_MAX_WRITE_BUFFER_NUMBER",
                        "value": str(STATE_STORE_WRITE_BUFFERS),
                    },
                ]
            }
        },
    }


def _merge_runtime_spec(
    base_spec: dict[str, Any],
    override_spec: dict[str, Any],
) -> dict[str, Any]:
    merged: dict[str, Any] = dict(base_spec)
    for key, value in override_spec.items():
        existing = merged.get(key)
        if isinstance(existing, dict) and isinstance(value, dict):
            merged[key] = _merge_runtime_spec(existing, value)
        elif key == "env" and _is_env_list(existing) and _is_env_list(value):
            merged[key] = _merge_env(existing, value)
        else:
            merged[key] = value
    return merged


def _is_env_list(value: Any) -> bool:
    return isinstance(value, list) and all(
        isinstance(item, dict) and isinstance(item.get("name"), str) for item in value
    )


def _merge_env(base_env: list[dict[str, Any]], override_env: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Merge container env lists by variable name, the override's entries winning.

    The deployment's runtime spec usually brings an env list of its own; replacing the defaults
    with it, as a plain merge does, would drop variables every runtime needs.
    """
    overridden = {item["name"] for item in override_env}
    return [item for item in base_env if item["name"] not in overridden] + list(override_env)


def _runtime_scale_spec(materialization: Materialization, *, scaled_to_zero: bool = False) -> dict[str, Any]:
    """Per-materialization scale overrides applied on top of the shared runtime spec.

    A stopped materialization runs no members at all, and neither does one whose topics are changing
    until its consumer group has emptied. Its configured `replicas` is left as it was, so the runtime
    is rendered at the scale it had once it runs again.
    """
    return {
        "replicas": 0 if scaled_to_zero or materialization.desired_phase == "stopped" else materialization.replicas,
        "storage": {"size": materialization.storage_size},
    }


def _pin_runtime_package_revision(
    runtime_spec: dict[str, Any],
    package_revision: str,
) -> dict[str, Any]:
    normalized = _merge_runtime_spec({}, runtime_spec)
    python_packages = normalized.get("pythonPackages")
    if not isinstance(python_packages, dict):
        python_packages = {}
    else:
        python_packages = dict(python_packages)

    packages = python_packages.get("packages")
    resolved_packages: list[str] = []
    package_pinned = False
    if isinstance(packages, list):
        for package in packages:
            if _is_runtime_distribution_package(package):
                resolved_packages.append(_runtime_distribution_package_ref(package_revision))
                package_pinned = True
            else:
                resolved_packages.append(package)
    if not package_pinned:
        resolved_packages.insert(0, _runtime_distribution_package_ref(package_revision))

    python_packages["packages"] = resolved_packages
    normalized["pythonPackages"] = python_packages
    return normalized


def _is_runtime_distribution_package(package: Any) -> bool:
    return (
        isinstance(package, str)
        and f"#egg={PACKAGE_DISTRIBUTION_NAME}" in package
        and "subdirectory=python" in package
    )


def _runtime_distribution_package_ref(package_revision: str) -> str:
    return (
        f"{PACKAGE_REPOSITORY_URL}@{package_revision}"
        f"#egg={PACKAGE_DISTRIBUTION_NAME}&subdirectory=python"
    )


def _to_yaml(value: Any, indent: int) -> str:
    dumped = yaml.safe_dump(value, sort_keys=True, default_flow_style=False).rstrip()
    return "\n".join((" " * indent) + line if line else line for line in dumped.splitlines())


def _to_python(value: Any) -> str:
    return repr(value)