"""Runtime handler for what the control plane delivers while a runtime runs: its settings.

The settings agent consumes the control plane's broadcast topic (`materialization-schema-authority-
updates`, named before 1.0). It carries `runtime_settings`, stored in the runtime's state table and
installed (`live_settings.py`). Table maintenance is the maintenance app's (`maintenance_app/`): a
`maintenance_request` an older control plane still sends here is dropped.
"""

from __future__ import annotations

from datetime import UTC, datetime
import logging
from typing import Any

from midstream.protocol_logging import log_protocol_event
from midstream.runtime_app.config import get_runtime_app_config
from midstream.runtime_app.live_settings import (
    RUNTIME_SETTINGS_KEY,
    install_runtime_settings,
    is_valid_lineage,
    read_stored_runtime_settings,
    sanitize_runtime_settings,
    settings_position,
)

LOGGER = logging.getLogger(__name__)

#: `kind` of a runtime-settings message on the schema-authority-updates topic.
RUNTIME_SETTINGS_KIND = "runtime_settings"

#: `materialization_id` of a message meant for every runtime.
#:
#: Each runtime's settings agent drops messages for another materialization before they reach this
#: package, and lets this id through as well.
BROADCAST_MATERIALIZATION_ID = "*"


def is_runtime_settings_message(value: Any) -> bool:
    """Whether a schema-authority-updates message carries runtime settings."""
    return isinstance(value, dict) and value.get("kind") == RUNTIME_SETTINGS_KIND


def handle_control_message(
    value: dict[str, Any],
    state_table: Any,
    *,
    materialization_id: str,
) -> dict[str, Any] | None:
    """Apply a control message. Returns nothing: it is kept as the agent's map for its routed output.

    Never raises: a malformed message is logged and dropped, as the same agent carries every
    later settings delivery.
    """
    if not isinstance(value, dict):
        return None
    try:
        if is_runtime_settings_message(value):
            apply_runtime_settings_message(value, state_table, materialization_id=materialization_id)
        elif value.get("kind") == "maintenance_request":
            LOGGER.info("Ignoring a maintenance request: the maintenance app runs table maintenance")
    except Exception:  # noqa: BLE001 - one bad message must not stop later deliveries
        LOGGER.exception("Could not apply a control message of kind %r", value.get("kind"))
    return None


def apply_runtime_settings_message(
    value: dict[str, Any],
    state_table: Any,
    *,
    materialization_id: str | None = None,
    applied_at: str | None = None,
) -> dict[str, Any] | None:
    """Store delivered runtime settings in the runtime's table and install them on this member.

    The table is globally replicated, so the other members install the same revision on their
    next refresh. A delivery addressed to another materialization, one not newer than what is
    stored (`settings_position`: by lineage, then revision), or one that is malformed changes
    nothing; it is never raised, because the same agent carries every later delivery.
    """
    target = value.get("target_materialization_id")
    if target is not None and materialization_id is not None and target != materialization_id:
        return None
    revision = value.get("revision")
    if isinstance(revision, bool) or not isinstance(revision, int) or revision < 0:
        LOGGER.warning("Ignoring runtime settings without a valid revision: %r", revision)
        return None
    lineage = value.get("lineage")
    if not is_valid_lineage(lineage):
        LOGGER.warning("Ignoring runtime settings with an invalid lineage: %r", lineage)
        return None
    lineage = lineage or None
    values = sanitize_runtime_settings(value.get("values"))

    stored = read_stored_runtime_settings(state_table)
    if stored is not None and settings_position(stored[2], stored[0]) >= settings_position(lineage, revision):
        # Already held, perhaps by another member; make sure this process runs it too. A delivery
        # of an earlier lineage, such as one an earlier installation left on the topic, ends here.
        install_runtime_settings(*stored)
        return None

    resolved_applied_at = applied_at or _utc_now()
    state_table[RUNTIME_SETTINGS_KEY] = {
        "revision": revision,
        "lineage": lineage,
        "values": values,
        "published_at": value.get("published_at"),
        "applied_at": resolved_applied_at,
    }
    install_runtime_settings(revision, values, lineage)
    log_protocol_event(
        LOGGER,
        enabled=get_runtime_app_config().protocol_logging_enabled,
        event="runtime_settings_applied",
        materialization_id=materialization_id,
        settings_revision=revision,
        settings_lineage=lineage,
        settings=sorted(values),
        published_at=value.get("published_at"),
    )
    return {"revision": revision, "lineage": lineage, "values": values, "applied_at": resolved_applied_at}


def _utc_now() -> str:
    return datetime.now(tz=UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
