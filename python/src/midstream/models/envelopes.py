"""Shared persistence envelopes for single-table records."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class RecordEnvelope:
    entity_type: str
    entity_id: str
    version: int
    updated_at: str
    payload: dict[str, Any]

    def __post_init__(self) -> None:
        if not self.entity_type:
            raise ValueError("entity_type is required")
        if not self.entity_id:
            raise ValueError("entity_id is required")
        if self.version < 1:
            raise ValueError("version must be >= 1")
        if not self.updated_at:
            raise ValueError("updated_at is required")

    def to_dict(self) -> dict[str, Any]:
        return {
            "entity_type": self.entity_type,
            "entity_id": self.entity_id,
            "version": self.version,
            "updated_at": self.updated_at,
            "payload": self.payload,
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "RecordEnvelope":
        return cls(
            entity_type=value["entity_type"],
            entity_id=value["entity_id"],
            version=value["version"],
            updated_at=value["updated_at"],
            payload=dict(value["payload"]),
        )