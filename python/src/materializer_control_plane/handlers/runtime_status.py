"""Thin Kaspr-facing runtime status handler."""

from __future__ import annotations

from materializer_control_plane.models import MaterializationStatus, RuntimeStatusEvent
from materializer_control_plane.services.observation import apply_runtime_status_event
from materializer_control_plane.storage.table import TableProtocol


def handle_runtime_status(value: dict, table: TableProtocol) -> MaterializationStatus | None:
    """Deserialize a runtime status event and merge it into stored status; None when the control
    plane holds no such materialization, and the event is dropped."""
    event = RuntimeStatusEvent.from_dict(value)
    return apply_runtime_status_event(table, event)