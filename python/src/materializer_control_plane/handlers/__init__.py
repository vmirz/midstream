"""Kaspr-facing adapter handlers live here."""

from materializer_control_plane.handlers.identity import handle_identity_request
from materializer_control_plane.handlers.request_intake import handle_request_event, handle_request_event_for_output
from materializer_control_plane.handlers.resource_observation import (
	handle_observation_work_for_output,
	handle_observation_work_for_output_async,
	handle_resource_observation,
)
from materializer_control_plane.handlers.runtime_status import handle_runtime_status
from materializer_control_plane.handlers.settings import handle_settings_request_for_output
from materializer_control_plane.handlers.table_events import handle_table_event
from materializer_control_plane.handlers.workflow_apply import (
	handle_work_event,
	handle_work_event_for_output,
	handle_work_event_for_routed_output,
	handle_work_event_for_routed_output_async,
)

__all__ = [
	"handle_identity_request",
	"handle_request_event",
	"handle_request_event_for_output",
	"handle_observation_work_for_output",
	"handle_observation_work_for_output_async",
	"handle_resource_observation",
	"handle_settings_request_for_output",
	"handle_runtime_status",
	"handle_table_event",
	"handle_work_event",
	"handle_work_event_for_output",
	"handle_work_event_for_routed_output",
	"handle_work_event_for_routed_output_async",
]