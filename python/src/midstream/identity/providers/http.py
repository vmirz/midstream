"""The HTTP a provider check makes: to its keys, its discovery document, or GitHub's API.

Behind a small interface so tests can answer for the providers; the real one is one aiohttp
session, made lazily on the running loop.
"""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from dataclasses import dataclass
import json
from typing import Any, Protocol

#: How much of an answer is read: a key set, a discovery document or an API answer is far smaller.
_MAX_BODY_BYTES = 1024 * 1024


class ProviderUnreachable(RuntimeError):
    """The provider could not be reached, or did not answer in time."""


@dataclass(frozen=True, slots=True)
class HttpAnswer:
    """A provider's answer: its status, and its body as JSON (None when it was not JSON)."""

    status: int
    body: Any


class ProviderHttp(Protocol):
    async def request(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str] | None = None,
        json_body: Any = None,
        basic_auth: tuple[str, str] | None = None,
        timeout: float = 10.0,
    ) -> HttpAnswer: ...


class AiohttpProviderHttp:
    """`ProviderHttp` over one aiohttp session."""

    def __init__(self) -> None:
        self._session: Any = None

    async def request(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str] | None = None,
        json_body: Any = None,
        basic_auth: tuple[str, str] | None = None,
        timeout: float = 10.0,
    ) -> HttpAnswer:
        import aiohttp

        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        auth = aiohttp.BasicAuth(*basic_auth) if basic_auth is not None else None
        try:
            async with self._session.request(
                method,
                url,
                headers=dict(headers or {}),
                json=json_body,
                auth=auth,
                timeout=aiohttp.ClientTimeout(total=timeout),
            ) as response:
                raw = await response.content.read(_MAX_BODY_BYTES + 1)
                if len(raw) > _MAX_BODY_BYTES:
                    raise ProviderUnreachable(f"{url} answered more than {_MAX_BODY_BYTES} bytes")
                try:
                    body = json.loads(raw) if raw else None
                except ValueError:
                    body = None
                return HttpAnswer(status=response.status, body=body)
        except (aiohttp.ClientError, asyncio.TimeoutError, OSError) as error:
            raise ProviderUnreachable(f"{url}: {error}") from error


_DEFAULT: AiohttpProviderHttp | None = None


def default_http() -> ProviderHttp:
    """The process's provider client."""
    global _DEFAULT
    if _DEFAULT is None:
        _DEFAULT = AiohttpProviderHttp()
    return _DEFAULT
