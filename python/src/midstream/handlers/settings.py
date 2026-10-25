"""Thin Kaspr-facing handler for settings change requests."""

from __future__ import annotations

from collections.abc import Iterator

from midstream.config import ControlPlaneConfig
from midstream.models import SettingsBootstrapRequest, SettingsChangeRequest
from midstream.services.settings import apply_settings_change, bootstrap_runtime_settings
from midstream.storage.table import TableProtocol


def handle_settings_request_for_output(
    value: dict,
    settings_table: TableProtocol,
    *,
    config: ControlPlaneConfig | None = None,
) -> Iterator[dict]:
    """Apply a settings change request and yield routed outputs, such as runtime deliveries."""
    if SettingsBootstrapRequest.matches(value):
        yield from bootstrap_runtime_settings(settings_table, SettingsBootstrapRequest.from_dict(value), config=config)
        return
    request = SettingsChangeRequest.from_dict(value)
    yield from apply_settings_change(settings_table, request, config=config)


def build_settings_bootstrap_request(*, requested_at: str) -> dict:
    """The startup trigger: give the runtime settings a lineage if they have none."""
    return SettingsBootstrapRequest(requested_at=requested_at).to_dict()
