"""Thin Kaspr-facing handlers of the maintenance app.

The `maintenance-worker` agent hands each message from the control plane to this member's worker,
which answers at once; the `maintenance-step` task, every few seconds, lets the worker finish what is
done and start what is due. Both return the table events to publish, as routed outputs.
"""

from __future__ import annotations

from collections.abc import Generator
import logging
from typing import Any

from midstream.maintenance_app.worker import get_worker
from midstream.models import MaintenanceMessage

LOGGER = logging.getLogger(__name__)


def handle_maintenance_message(value: dict[str, Any]) -> Generator[dict[str, Any], None, None]:
    """Take a tick, a request or a cancel; yield the table events it settles at once."""
    try:
        message = MaintenanceMessage.from_dict(value)
    except (KeyError, TypeError, ValueError) as error:
        LOGGER.warning("Ignoring a malformed maintenance message: %s", error)
        return _generator([])
    return _generator(get_worker().handle(message))


async def advance_maintenance(*, app: Any = None) -> Generator[dict[str, Any], None, None]:
    """Finish what is done and start what is due on every table; return the events to publish.

    A generator over outputs already computed: Kaspr iterates one that an operation returns, but
    sends a list as one value and cannot iterate an async generator.
    """
    outputs = await get_worker().step(should_stop=bool(getattr(app, "should_stop", False)))
    return _generator(outputs)


def _generator(outputs: list[dict[str, Any]]) -> Generator[dict[str, Any], None, None]:
    computed = list(outputs)
    return (output for output in computed)
