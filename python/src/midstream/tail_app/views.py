"""The tail HTTP API: the console's alone (handbook 19), behind the app's four WebViews.

`GET /tail/v1/materializations/{materialization_id}/tables/{table}` answers the table's records past
`after` - per partition, the last offset the caller has - as one Parquet file in the table's
physical format, with `X-Tail-*` headers saying how far the answer goes. `…/head` answers the
topic's newest records, apart from any run. `GET /tail/v1/health` and `/tail/v1/members` describe
this member and the app.

Every request must carry the console's credential (`X-Console-Token`) besides the person's
bearer. A member that does not own the table checks the caller, then forwards the request - its
path, query and caller's headers - to the member that does, once: a forwarded request is never
forwarded again (`X-Tail-Forwarded-By`), and an owner that does not hold the table answers
`409 tail_rebalancing`. The owner authorizes `data.query` on the table's dataset itself.

Errors are the control plane's usual body, `{"error": {"code", "message", "details"}}`.
"""

from __future__ import annotations

from datetime import UTC, datetime
import json
import logging
import re
import time
from typing import Any

from midstream.identity.authorization import bearer_token, header
from midstream.identity.principals import Principal, principal_from_description
from midstream.models import live_tail_routing_key
from midstream.protocol_logging import log_protocol_event
from midstream.tail_app.auth import CONSOLE_HEADER, authorize_tail, refusal, require_console
from midstream.tail_app.buffer import Unavailable
from midstream.tail_app.http import HttpUnavailable
from midstream.tail_app.peek import Peeker
from midstream.tail_app.routing import owner_partition
from midstream.tail_app.serve import build_answer
from midstream.tail_app.table_tail import TableTail
from midstream.tail_app.worker import DEFAULT_TICK_PARTITIONS, TailWorker, get_worker
from midstream.web.errors import WebError, conflict, forbidden, invalid_request, not_found, unauthenticated, unavailable

LOGGER = logging.getLogger(__name__)

FORWARDED_BY = "x-tail-forwarded-by"
#: How long a request may wait for a table activating or catching up, at most.
MAX_WAIT_MS = 5_000
#: How long a forwarded request may take.
FORWARD_TIMEOUT_SECONDS = 15.0
#: The headers a forward passes on.
FORWARD_HEADERS = ("authorization", CONSOLE_HEADER, "x-forwarded-for", "x-forwarded-user-agent", "user-agent")
#: What an id and a table name may be: never a path, never a traversal.
_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$")
_TABLE = re.compile(r"^[a-z_][a-z0-9_]{0,62}$")
#: States in which a table answers rows.
ANSWERING = ("catching_up", "live", "paused")


async def tail_rows_view(request: Any, materialization_id: str, table: str, *, worker: TailWorker | None = None) -> dict[str, Any]:
    """`GET /tail/v1/materializations/{id}/tables/{table}`: the table's records past `after`."""
    worker = worker or get_worker()
    started = time.monotonic()
    _require_names(materialization_id, table)
    query = _query(request)
    after = _after(query.get("after"))
    max_rows = _bounded_int(query.get("max_rows"), name="max_rows", minimum=1)
    max_bytes = _bounded_int(query.get("max_bytes"), name="max_bytes", minimum=1)
    wait_ms = _bounded_int(query.get("wait_ms"), name="wait_ms", minimum=0, default=0)
    fmt = (query.get("format") or "parquet").strip().lower()
    if fmt not in ("parquet", "json"):
        raise invalid_request("format must be parquet or json", field_name="format")
    forwarded = await _route(request, worker, materialization_id, table)
    if forwarded is not None:
        return forwarded
    tail = await _local_table(worker, materialization_id, table)
    await authorize_tail(request, "data.query", sessions=worker.sessions, console_token=worker.console_token, dataset_id=tail.target.dataset_id, dataset=tail.target.dataset)
    worker.activate(tail)
    deadline = time.monotonic() + min(wait_ms or 0, MAX_WAIT_MS) / 1000
    await tail.wait(deadline, lambda: tail.state in ("live", "backlogged", "unavailable") or (tail.ready and tail.state == "paused"))
    _refuse_unanswerable(tail)
    selection = tail.select(after)
    if isinstance(selection, Unavailable):
        raise _unavailable_for_snapshot(selection)
    settings = tail.settings
    rows_cap = min(max_rows or settings.max_rows_per_table, settings.max_rows_per_table)
    bytes_cap = min(max_bytes or settings.max_bytes_per_table, settings.max_bytes_per_table)
    schema = tail.schema
    assert schema is not None
    answer = await worker.run_in_serve_pool(build_answer, selection, schema, max_rows=rows_cap, max_bytes=bytes_cap, fmt=fmt)
    paused = tail.paused_partitions()
    status = "truncated" if (answer.cut or paused) else ("empty" if answer.rows == 0 else "complete")
    headers = {
        "X-Tail-Rows": str(answer.rows),
        "X-Tail-Bytes": str(len(answer.body)),
        "X-Tail-After": _json({str(partition): offset for partition, offset in sorted(_after_for(selection).items())}),
        "X-Tail-Through": _json({str(partition): offset for partition, offset in sorted(answer.through.items())}),
        "X-Tail-End-Offsets": _json({str(partition): offset for partition, offset in tail.end_offsets().items()}),
        "X-Tail-Status": status,
        "X-Tail-Paused": _json(paused),
        "X-Tail-Gap": _json(tail.gaps(selection)),
        "X-Tail-Schema-Id": str(answer.schema_id),
        "X-Tail-Settling": str(answer.settling),
        "X-Tail-Lag-Ms": str(tail.lag_ms() or 0),
        "X-Tail-Member": worker.config.member_id,
        "X-Tail-State": tail.state,
        "X-Tail-Table-Uuid": tail.target.table_uuid or "",
        "X-Tail-Generated-At": _now(),
    }
    if answer.time_min is not None:
        headers["X-Tail-Time-Min"] = str(answer.time_min)
        headers["X-Tail-Time-Max"] = str(answer.time_max)
    tail.counters.served += 1
    tail.counters.rows_served += answer.rows
    tail.counters.bytes_served += len(answer.body)
    worker.counters["served"] += 1
    elapsed = int((time.monotonic() - started) * 1000)
    log_protocol_event(
        LOGGER,
        enabled=tail.settings.protocol_logging_enabled,
        event="tail_served",
        table=tail.key,
        rows=answer.rows,
        bytes=len(answer.body),
        ms=elapsed,
        status=status,
        forwarded=bool(header(request, FORWARDED_BY)),
        member=worker.config.member_id,
    )
    return {"status": 200, "headers": headers, "body": answer.body, "content_type": answer.content_type}


async def tail_head_view(request: Any, materialization_id: str, table: str, *, worker: TailWorker | None = None) -> dict[str, Any]:
    """`GET /tail/v1/materializations/{id}/tables/{table}/head`: the topic's newest records."""
    worker = worker or get_worker()
    _require_names(materialization_id, table)
    query = _query(request)
    rows = _bounded_int(query.get("rows"), name="rows", minimum=1, default=200)
    fmt = (query.get("format") or "parquet").strip().lower()
    if fmt not in ("parquet",):
        raise invalid_request("format must be parquet", field_name="format")
    forwarded = await _route(request, worker, materialization_id, table)
    if forwarded is not None:
        return forwarded
    tail = await _local_table(worker, materialization_id, table)
    await authorize_tail(request, "data.query", sessions=worker.sessions, console_token=worker.console_token, dataset_id=tail.target.dataset_id, dataset=tail.target.dataset)
    rows = min(rows, tail.settings.peek_max_rows)
    worker.activate(tail)
    await tail.wait(time.monotonic() + 3.0, lambda: tail.schema is not None and tail.state != "seeding")
    if tail.schema is None:
        raise _not_ready(tail)
    if worker.peek is None:
        worker.peek = Peeker(clients=worker.clients, concurrency=worker.config.peek_concurrency)
    try:
        answer = await worker.peek.peek(tail, rows)
    except HttpUnavailable as error:  # pragma: no cover - not an HTTP call
        raise unavailable(str(error), reason="peek_failed") from error
    except Exception as error:  # noqa: BLE001 - a failed peek leaves the tail alone
        LOGGER.warning("A head peek of %s failed: %s", tail.key, error)
        raise unavailable("The newest records could not be read just now. Try again in a moment.", reason="peek_failed") from error
    headers = {
        "X-Tail-Peek-Rows": str(answer.rows),
        "X-Tail-Peek-Offsets": _json({str(partition): list(bounds) for partition, bounds in sorted(answer.offsets.items())}),
        "X-Tail-End-Offsets": _json({str(partition): offset for partition, offset in sorted(answer.ends.items())}),
        "X-Tail-Behind": str(answer.behind),
        "X-Tail-State": answer.state,
        "X-Tail-Schema-Id": str(answer.schema_id),
        "X-Tail-Settling": str(answer.settling),
        "X-Tail-Member": worker.config.member_id,
        "X-Tail-Generated-At": answer.generated_at,
    }
    return {"status": 200, "headers": headers, "body": answer.body, "content_type": "application/octet-stream"}


async def tail_health_view(request: Any, *, worker: TailWorker | None = None) -> dict[str, Any]:
    """`GET /tail/v1/health`: this member's tables, partitions, buffers, lag and pauses."""
    worker = worker or get_worker()
    await authorize_tail(request, "materializations.view_details", sessions=worker.sessions, console_token=worker.console_token)
    return worker.health()


async def tail_members_view(request: Any, *, worker: TailWorker | None = None) -> dict[str, Any]:
    """`GET /tail/v1/members`: which member holds which tick partition, and with `?health=true`
    every member's health, asked of each."""
    worker = worker or get_worker()
    await authorize_tail(request, "materializations.view_details", sessions=worker.sessions, console_token=worker.console_token)
    members = await worker.refresh_member_map()
    payload: dict[str, Any] = {
        "member": worker.config.member_id,
        "group_id": worker.config.group_id,
        "tick_partitions": worker.tick_partitions or DEFAULT_TICK_PARTITIONS,
        **members.describe(),
    }
    if (_query(request).get("health") or "").lower() in ("1", "true", "yes"):
        reports: dict[str, Any] = {}
        for member in members.members:
            if member.name == worker.config.member_id:
                reports[member.name] = worker.health()
                continue
            if not member.url:
                reports[member.name] = {"error": "no address"}
                continue
            try:
                response = await worker.http.get(f"{member.url}/tail/v1/health", headers=_forward_headers(request, worker), timeout=5.0)
                reports[member.name] = response.json() if response.status == 200 else {"error": f"answered {response.status}"}
            except (HttpUnavailable, ValueError) as error:
                reports[member.name] = {"error": str(error)}
        payload["health"] = reports
    return payload


# -- routing -----------------------------------------------------------------------------------


async def _route(request: Any, worker: TailWorker, materialization_id: str, table: str) -> dict[str, Any] | None:
    """Forward a request for another member's table there; None when this member answers it."""
    key = live_tail_routing_key(materialization_id, table)
    owned = worker.owns(key)
    if owned is True or (owned is None and key in worker.tables):
        return None
    if header(request, FORWARDED_BY):
        # One hop only: the member that sent it thought this one the owner. Not any more.
        raise _rebalancing()
    # The forwarder checks the caller before it forwards; the owner checks again on the dataset.
    await _check_anywhere(request, worker)
    partition = owner_partition(key, worker.tick_partitions or DEFAULT_TICK_PARTITIONS)
    owner = worker.members.owner(partition)
    if owner is None or owner.name == worker.config.member_id or not owner.url:
        await worker.refresh_member_map()
        owner = worker.members.owner(partition)
    if owner is None or owner.name == worker.config.member_id or not owner.url:
        raise _rebalancing()
    path = getattr(request, "path_qs", None) or str(getattr(request, "rel_url", ""))
    try:
        response = await worker.http.get(f"{owner.url}{path}", headers=_forward_headers(request, worker), timeout=FORWARD_TIMEOUT_SECONDS)
    except HttpUnavailable as error:
        raise unavailable(
            "The member that tails this table cannot be reached just now. Try again in a moment.",
            reason="tail_owner_unreachable",
        ) from error
    worker.counters["forwarded"] += 1
    log_protocol_event(LOGGER, enabled=worker.settings.protocol_logging_enabled, event="tail_forwarded", table=key, owner=owner.name, status=response.status, member=worker.config.member_id)
    if response.status != 200:
        raise _owner_error(response)
    headers = {name: value for name, value in response.headers.items() if name.lower().startswith("x-tail-")}
    headers["X-Tail-Forwarded"] = "1"
    return {"status": 200, "headers": headers, "body": response.body, "content_type": response.header("content-type") or "application/octet-stream"}


async def _check_anywhere(request: Any, worker: TailWorker) -> Principal:
    """The caller may query some dataset: what a forwarder can check without knowing the table's."""
    require_console(request, worker.console_token)
    token = bearer_token(request)
    payload = await worker.sessions.check(token)
    if token and payload.get("session_error"):
        raise unauthenticated("Your session is not valid. Sign in again.", reason=str(payload["session_error"]))
    principal = principal_from_description(payload["principal"])
    if principal.password_change_required:
        raise forbidden("Choose a new password before you continue.", reason="password_change_required")
    if not principal.can_anywhere("data.query"):
        raise refusal(principal, "data.query")
    return principal


def _forward_headers(request: Any, worker: TailWorker) -> dict[str, str]:
    headers = {name: value for name in FORWARD_HEADERS if (value := header(request, name))}
    headers["X-Tail-Forwarded-By"] = worker.config.member_id
    return headers


def _owner_error(response: Any) -> WebError:
    """The owner's refusal, passed through as it is."""
    try:
        body = response.json() or {}
        error = body.get("error") or {}
        return WebError(code=str(error.get("code") or "unavailable"), message=str(error.get("message") or "The owner refused."), details=dict(error.get("details") or {}), status_code=int(response.status))
    except (ValueError, AttributeError, TypeError):
        return unavailable("The member that tails this table answered with an error.", reason="tail_owner_error")


async def _local_table(worker: TailWorker, materialization_id: str, table: str) -> TableTail:
    tail = await worker.resolve(materialization_id, table)
    if tail is None:
        raise not_found_off(materialization_id, table)
    return tail


def not_found_off(materialization_id: str, table: str) -> WebError:
    error = not_found(
        f"Table {table} of materialization {materialization_id} has no live tail: it is not ticked here, or it is not running.",
        resource="live-tail",
        identifier=f"{materialization_id}:{table}",
    )
    error.details["reason"] = "live_tail_off"
    return error


def _refuse_unanswerable(tail: TableTail) -> None:
    if tail.state == "backlogged":
        error = conflict(
            "The live tail starts once the runtime catches up: it is too far behind the topic's head.",
            reason="tail_backlogged",
        )
        error.details.update(
            behind=tail.lag(),
            end_offsets={str(partition): offset for partition, offset in tail.end_offsets().items()},
            committed={str(partition): offset for partition, offset in sorted(tail.committed.items())},
        )
        raise error
    if tail.state == "unavailable":
        raise unavailable(f"The live tail cannot read this table's topic: {tail.last_error or tail.reason}.", reason="topic_unavailable")
    if tail.state not in ANSWERING or not tail.ready:
        raise _not_ready(tail)


def _not_ready(tail: TableTail) -> WebError:
    error = unavailable("The live tail is warming up for this table. Try again in a second.", reason="tail_not_ready")
    error.details["state"] = tail.state if tail.state != "parked" else "seeding"
    return error


def _rebalancing() -> WebError:
    return conflict("This table is moving between the tail's members. Try again in a moment.", reason="tail_rebalancing")


def _unavailable_for_snapshot(selection: Unavailable) -> WebError:
    error = conflict(
        "The live tail no longer holds records this far back: read the table again for a newer snapshot.",
        reason="tail_unavailable",
    )
    error.details["partitions"] = {str(partition): why for partition, why in sorted(selection.partitions.items())}
    return error


def _after_for(selection: Any) -> dict[int, int]:
    return {partition: item.after for partition, item in selection.partitions.items()}


# -- request parsing ------------------------------------------------------------------------------


def _require_names(materialization_id: str, table: str) -> None:
    if not isinstance(materialization_id, str) or not _ID.match(materialization_id):
        raise invalid_request("materialization_id is not a materialization id", field_name="materialization_id")
    if not isinstance(table, str) or not _TABLE.match(table):
        raise invalid_request("table is not a table name", field_name="table")


def _query(request: Any) -> dict[str, str]:
    for name in ("query_params", "query"):
        query = getattr(request, name, None)
        if query is not None and hasattr(query, "get"):
            return {key: query.get(key) for key in query}
    rel_url = getattr(request, "rel_url", None)
    if rel_url is not None:
        return {key: rel_url.query.get(key) for key in rel_url.query}
    return {}


def _after(raw: str | None) -> dict[int, int]:
    if raw is None or raw == "":
        raise invalid_request("after is required: the last offset the caller has of each partition, as JSON", field_name="after")
    try:
        parsed = json.loads(raw)
    except ValueError as error:
        raise invalid_request("after must be a JSON object of partition to offset", field_name="after") from error
    if not isinstance(parsed, dict) or len(parsed) > 10_000:
        raise invalid_request("after must be a JSON object of partition to offset", field_name="after")
    after: dict[int, int] = {}
    for partition, offset in parsed.items():
        try:
            number = int(partition)
        except (TypeError, ValueError) as error:
            raise invalid_request("after's partitions must be numbers", field_name="after") from error
        if isinstance(offset, bool) or not isinstance(offset, int) or offset < -1 or number < 0:
            raise invalid_request("after's offsets must be integers of -1 or more", field_name="after")
        after[number] = offset
    return after


def _bounded_int(raw: str | None, *, name: str, minimum: int, default: int | None = None) -> int | None:
    if raw is None or raw == "":
        return default
    try:
        value = int(raw)
    except (TypeError, ValueError) as error:
        raise invalid_request(f"{name} must be an integer", field_name=name) from error
    if value < minimum:
        raise invalid_request(f"{name} must be at least {minimum}", field_name=name)
    return value


def _json(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True)


def _now() -> str:
    return datetime.now(tz=UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")

