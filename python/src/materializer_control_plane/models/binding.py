"""Runtime binding models."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from materializer_control_plane.models.materialization import RUNTIME_MODES


def _validate_choice(name: str, value: str, choices: tuple[str, ...]) -> None:
    if value not in choices:
        allowed = ", ".join(choices)
        raise ValueError(f"{name} must be one of: {allowed}")


@dataclass(frozen=True, slots=True)
class RuntimeBinding:
    materialization_id: str
    runtime_mode: str
    kaspr_app_name: str
    namespace: str
    managed_agent_names: list[str]
    template_version: str
    last_applied_spec_hash: str
    last_apply_operation_id: str
    resource_profile: str | None = None
    #: The topics the applied runtime reads, in table order.
    source_topics: list[str] = field(default_factory=list)
    destination_uri: str | None = None
    created_at: str | None = None
    updated_at: str | None = None
    #: Set while the runtime is held at zero replicas so its consumer group can start over: Kaspr's
    #: static members keep their assignment, so one that reads a changed set of topics is only
    #: assigned them by a group that emptied first. Cleared when the group is found empty.
    drain_started_at: str | None = None
    #: The topics the runtime read before that change, so the ones it adds are known afterwards.
    topics_before_drain: list[str] = field(default_factory=list)

    @property
    def draining(self) -> bool:
        return self.drain_started_at is not None

    def __post_init__(self) -> None:
        for name in (
            "materialization_id",
            "kaspr_app_name",
            "namespace",
            "template_version",
            "last_applied_spec_hash",
            "last_apply_operation_id",
        ):
            if getattr(self, name) == "":
                raise ValueError(f"{name} is required")
        _validate_choice("runtime_mode", self.runtime_mode, RUNTIME_MODES)
        if not self.managed_agent_names:
            raise ValueError("managed_agent_names must not be empty")
        if len(set(self.managed_agent_names)) != len(self.managed_agent_names):
            raise ValueError("managed_agent_names must be unique")

    def to_dict(self) -> dict[str, Any]:
        return {
            "materialization_id": self.materialization_id,
            "runtime_mode": self.runtime_mode,
            "kaspr_app_name": self.kaspr_app_name,
            "namespace": self.namespace,
            "managed_agent_names": list(self.managed_agent_names),
            "template_version": self.template_version,
            "last_applied_spec_hash": self.last_applied_spec_hash,
            "last_apply_operation_id": self.last_apply_operation_id,
            "resource_profile": self.resource_profile,
            "source_topics": list(self.source_topics),
            "destination_uri": self.destination_uri,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "drain_started_at": self.drain_started_at,
            "topics_before_drain": list(self.topics_before_drain),
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "RuntimeBinding":
        return cls(
            materialization_id=value["materialization_id"],
            runtime_mode=value["runtime_mode"],
            kaspr_app_name=value["kaspr_app_name"],
            namespace=value["namespace"],
            managed_agent_names=list(value["managed_agent_names"]),
            template_version=value["template_version"],
            last_applied_spec_hash=value["last_applied_spec_hash"],
            last_apply_operation_id=value["last_apply_operation_id"],
            resource_profile=value.get("resource_profile"),
            source_topics=[str(topic) for topic in value.get("source_topics") or []],
            destination_uri=value.get("destination_uri"),
            created_at=value.get("created_at"),
            updated_at=value.get("updated_at"),
            drain_started_at=value.get("drain_started_at"),
            topics_before_drain=[str(topic) for topic in value.get("topics_before_drain") or []],
        )