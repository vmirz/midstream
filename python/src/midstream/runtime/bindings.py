"""Runtime binding builders."""

from __future__ import annotations

from midstream.models import Materialization, RuntimeBinding


def build_runtime_binding(
    *,
    materialization: Materialization,
    runtime_app_name: str,
    managed_agent_names: list[str],
    template_version: str,
    spec_hash: str,
    operation_id: str,
    updated_at: str,
) -> RuntimeBinding:
    """Build the runtime binding persisted after a runtime apply operation."""
    return RuntimeBinding(
        materialization_id=materialization.materialization_id,
        runtime_mode=materialization.runtime_mode,
        kaspr_app_name=runtime_app_name,
        namespace=materialization.namespace,
        managed_agent_names=managed_agent_names,
        template_version=template_version,
        last_applied_spec_hash=spec_hash,
        last_apply_operation_id=operation_id,
        resource_profile=materialization.resource_profile,
        source_topics=list(materialization.source_topics),
        destination_uri=materialization.destination_uri,
        created_at=updated_at,
        updated_at=updated_at,
    )