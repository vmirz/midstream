"""Thin Kaspr-facing handlers of the tail app.

The `tail-worker` agent hands each message from the control plane to this member's worker; the
`tail-step` task, every few seconds, lets it do what is due; the four WebViews ask it. Kaspr fixes one
content type per WebView, and the rows and the peek are Parquet, so their views send bytes:
`success_*` pick the body, headers and status of an answer, and `error_body` turns a refusal into the
control plane's JSON error body, as bytes, for the octet-stream view to send (`error_headers` says
so). The console reads a non-2xx body as JSON whatever its content type.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from midstream.models import LiveTailMessage
from midstream.tail_app import views
from midstream.tail_app.worker import get_worker

LOGGER = logging.getLogger(__name__)


def handle_tail_message(value: dict[str, Any], *, context: Any = None) -> None:
    """Take a tick or a cancel from the control plane. Returns nothing: the agent has no outputs."""
    try:
        message = LiveTailMessage.from_dict(value)
    except (KeyError, TypeError, ValueError) as error:
        LOGGER.warning("Ignoring a malformed live-tail message: %s", error)
        return None
    worker = get_worker()
    app = None if context is None else context.get("app")
    if app is not None and worker.app is None:
        worker.app = app
    worker.handle(message, partition=_partition(context))
    return None


async def advance_tail(*, app: Any = None) -> None:
    """Do what is due on every table: seeds, parking, trims, budgets, the member map."""
    await get_worker().step(app=app)


async def tail_rows(request: Any, materialization_id: str, table: str) -> dict[str, Any]:
    return await views.tail_rows_view(request, materialization_id, table)


async def tail_head(request: Any, materialization_id: str, table: str) -> dict[str, Any]:
    return await views.tail_head_view(request, materialization_id, table)


async def tail_health(request: Any) -> dict[str, Any]:
    return await views.tail_health_view(request)


async def tail_members(request: Any) -> dict[str, Any]:
    return await views.tail_members_view(request)


# -- response selectors ---------------------------------------------------------------------------


def success_body(data: dict[str, Any]) -> bytes:
    return data["body"]


def success_headers(data: dict[str, Any]) -> dict[str, str]:
    return {str(name): str(value) for name, value in (data.get("headers") or {}).items()}


def success_status(data: dict[str, Any]) -> int:
    return int(data.get("status") or 200)


def error_body(error: dict[str, Any]) -> dict[str, Any]:
    """A refusal as the control plane's error body, which the view sends as JSON (`errorContentType`)."""
    body = error.get("cause_response")
    if not isinstance(body, dict):
        body = {"error": {"code": "internal_error", "message": "The live tail failed to answer.", "details": {}}}
        LOGGER.error("The live tail failed to answer: %s", error.get("cause") or error.get("message"))
    # Round-tripped, so a value JSON cannot hold fails here, logged, rather than in the response.
    return json.loads(json.dumps(body, default=str))


def error_headers(error: dict[str, Any]) -> dict[str, str]:
    headers = {"X-Tail-Error": "1"}
    details = error.get("details") or {}
    if isinstance(details, dict) and details.get("reason") == "tail_not_ready":
        headers["Retry-After"] = "1"
    return headers


def _partition(context: Any) -> int | None:
    event = None if context is None else context.get("event")
    message = getattr(event, "message", None)
    partition = getattr(message, "partition", None)
    return None if partition is None else int(partition)
