"""The tail app's outbound HTTP: to the control plane, and to the member that owns a table.

One `aiohttp` session per member (aiohttp comes with the Kaspr image and with kubernetes-asyncio),
made on first use on the app's event loop. The seam is small so the tests can stand in for it.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any, Protocol


@dataclass(frozen=True, slots=True)
class HttpResponse:
    status: int
    headers: dict[str, str] = field(default_factory=dict)
    body: bytes = b""

    def json(self) -> Any:
        import json

        return json.loads(self.body.decode("utf-8")) if self.body else None

    def header(self, name: str) -> str | None:
        wanted = name.lower()
        return next((value for key, value in self.headers.items() if key.lower() == wanted), None)


#: How much of a body is read at a time.
_CHUNK_BYTES = 256 * 1024


class HttpUnavailable(RuntimeError):
    """The other end could not be reached, or did not answer in time."""


class HttpClient(Protocol):
    async def get(self, url: str, *, headers: dict[str, str], timeout: float) -> HttpResponse: ...


class AiohttpClient:
    """`HttpClient` over one aiohttp session, made lazily on the running loop."""

    def __init__(self, *, max_body_bytes: int = 512 * 1024 * 1024) -> None:
        self._session: Any = None
        self.max_body_bytes = max_body_bytes

    async def get(self, url: str, *, headers: dict[str, str], timeout: float) -> HttpResponse:
        import aiohttp

        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        try:
            async with self._session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=timeout)) as response:
                # To the end, bounded: `content.read(n)` answers with whatever has arrived so far, so
                # a body of more than one chunk - a forwarded Parquet answer - came back cut short.
                chunks: list[bytes] = []
                size = 0
                async for chunk in response.content.iter_chunked(_CHUNK_BYTES):
                    size += len(chunk)
                    if size > self.max_body_bytes:
                        raise HttpUnavailable(f"{url} answered more than {self.max_body_bytes} bytes")
                    chunks.append(chunk)
                return HttpResponse(status=response.status, headers={key: value for key, value in response.headers.items()}, body=b"".join(chunks))
        except (aiohttp.ClientError, asyncio.TimeoutError, OSError) as error:
            raise HttpUnavailable(f"{url}: {error}") from error

    async def close(self) -> None:
        if self._session is not None and not self._session.closed:
            await self._session.close()
