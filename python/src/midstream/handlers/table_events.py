"""Thin Kaspr-facing handler for the `table-events` agent."""

from __future__ import annotations

import logging

from midstream.config import ControlPlaneConfig
from midstream.models import TableEvent
from midstream.services.table_events import apply_table_event
from midstream.settings import resolve_config
from midstream.storage.table import TableProtocol

LOGGER = logging.getLogger(__name__)


def handle_table_event(
    value: dict,
    table: TableProtocol,
    *,
    settings_table: TableProtocol | None = None,
    config: ControlPlaneConfig | None = None,
) -> bool:
    """Deserialize a runtime's table event and apply it to the control plane's records.

    An event that does not parse - one that names no table, from a runtime of an earlier release - is
    dropped with a warning: raised, it would restart the agent and be read again.
    """
    cfg = resolve_config(settings_table) if config is None else config
    try:
        event = TableEvent.from_dict(value)
    except (KeyError, TypeError, ValueError) as error:
        LOGGER.warning("Dropping a table event that does not parse: %s", error)
        return False
    return apply_table_event(table, event, config=cfg)
