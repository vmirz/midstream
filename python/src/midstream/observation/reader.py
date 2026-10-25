"""Normalized KasprApp observation inputs."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from midstream.models import ResourceCondition, RuntimeMemberState


@dataclass(frozen=True, slots=True)
class KasprAppObservationInput:
    namespace: str
    kaspr_app_name: str
    resource_exists: bool
    managed_agent_states: dict[str, str] = field(default_factory=dict)
    metadata_generation: int | None = None
    observed_generation: int | None = None
    ready_condition: ResourceCondition | None = None
    progressing_condition: ResourceCondition | None = None
    rollout_in_progress: bool | None = None
    available_members: str | None = None
    rebalance_required: bool | None = None
    rebalancing_members: str | None = None
    #: Each member's pod and its restarts; None when the pods could not be read.
    members: tuple[RuntimeMemberState, ...] | None = None

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "KasprAppObservationInput":
        return cls(
            namespace=value["namespace"],
            kaspr_app_name=value["kaspr_app_name"],
            resource_exists=value["resource_exists"],
            managed_agent_states=dict(value.get("managed_agent_states", {})),
            metadata_generation=value.get("metadata_generation"),
            observed_generation=value.get("observed_generation"),
            ready_condition=ResourceCondition.from_dict(value.get("ready_condition")),
            progressing_condition=ResourceCondition.from_dict(value.get("progressing_condition")),
            rollout_in_progress=value.get("rollout_in_progress"),
            available_members=value.get("available_members"),
            rebalance_required=value.get("rebalance_required"),
            rebalancing_members=value.get("rebalancing_members"),
            members=None
            if value.get("members") is None
            else tuple(RuntimeMemberState.from_dict(member) for member in value["members"]),
        )