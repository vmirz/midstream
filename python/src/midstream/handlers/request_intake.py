"""Thin Kaspr-facing request-intake handler."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

from midstream.config import ControlPlaneConfig
from midstream.models import MaterializationRequestEvent
from midstream.services import IntakeResult, iter_request_outputs, process_request_event
from midstream.settings import resolve_config
from midstream.storage.table import TableProtocol


def handle_request_event(
    value: dict,
    table: TableProtocol,
    *,
    settings_table: TableProtocol | None = None,
    config: ControlPlaneConfig | None = None,
    work_id: str | None = None,
) -> IntakeResult:
    """Deserialize a request message and route it through request-intake processing."""
    cfg = resolve_config(settings_table) if config is None else config
    request_event = MaterializationRequestEvent.from_dict(value)
    return process_request_event(
        table,
        request_event,
        config=cfg,
        work_id=work_id,
    )


def handle_request_event_for_output(
    value: dict,
    table: TableProtocol,
    *,
    settings_table: TableProtocol | None = None,
    config: ControlPlaneConfig | None = None,
    work_id: str | None = None,
) -> Iterator[dict[str, Any]]:
    """Persist request state and return what follows it, routed for Kaspr's outputs: work events
    for the work topic and requests for the directory's topic.

    A generator over outputs already computed: Kaspr sends each one a generator yields, but sends
    a list as a single value.
    """
    cfg = resolve_config(settings_table) if config is None else config
    request_event = MaterializationRequestEvent.from_dict(value)
    outputs = list(iter_request_outputs(table, request_event, config=cfg, work_id=work_id))
    return (output for output in outputs)
