"""Reading a WebView request's query string and JSON body, whatever request object Kaspr passes."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from midstream.web.errors import invalid_request


def query_params(request: Any) -> Mapping[str, Any]:
    """The request's query parameters."""
    for attribute in ("query_params", "query"):
        if hasattr(request, attribute):
            return getattr(request, attribute)
    rel_url = getattr(request, "rel_url", None)
    if rel_url is not None and hasattr(rel_url, "query"):
        return rel_url.query
    return {}


async def json_body(request: Any, *, optional: bool = False) -> Any:
    """The request's JSON body. An empty or unreadable body is None when the body is optional."""
    try:
        return await request.json()
    except Exception as error:  # noqa: BLE001 - every framework raises its own decode error
        if optional:
            return None
        raise invalid_request("request body must be JSON") from error
