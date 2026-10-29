"""The tail app's own HTTP client, against a real aiohttp server: what the forwarder and the control plane
client read is the whole body the other end sent, or a failure - never a part of it.

A test cluster showed the forwarder answering a Parquet file of 737 of its 5,797 bytes: `content.read(n)` answers
with whatever has arrived, and a body larger than one chunk arrives in several.
"""

from __future__ import annotations

import asyncio
import os
import unittest

from midstream.tail_app.http import AiohttpClient, HttpUnavailable

try:
    from aiohttp import web
except ImportError:  # pragma: no cover - aiohttp comes with kubernetes-asyncio
    web = None


@unittest.skipIf(web is None, "aiohttp is required")
class AiohttpClientTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.body = os.urandom(3 * 1024 * 1024 + 17)
        self.refusal = {"error": {"code": "conflict", "message": "Moving.", "details": {"reason": "tail_rebalancing"}}}
        app = web.Application()
        app.router.add_get("/pieces", self._pieces)
        app.router.add_get("/cut", self._cut)
        app.router.add_get("/refused", self._refused)
        self.runner = web.AppRunner(app)
        await self.runner.setup()
        site = web.TCPSite(self.runner, "127.0.0.1", 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        self.base = f"http://127.0.0.1:{port}"
        self.client = AiohttpClient()

    async def asyncTearDown(self) -> None:
        await self.client.close()
        await self.runner.cleanup()

    async def _pieces(self, request: web.Request) -> web.StreamResponse:
        response = web.StreamResponse(headers={"X-Tail-Bytes": str(len(self.body)), "Content-Type": "application/octet-stream"})
        await response.prepare(request)
        for start in range(0, len(self.body), 64 * 1024):
            await response.write(self.body[start : start + 64 * 1024])
            await asyncio.sleep(0)
        await response.write_eof()
        return response

    async def _cut(self, request: web.Request) -> web.StreamResponse:
        response = web.StreamResponse(headers={"Content-Length": str(len(self.body))})
        await response.prepare(request)
        await response.write(self.body[:1_000])
        request.transport.close()
        return response

    async def _refused(self, request: web.Request) -> web.Response:
        return web.json_response(self.refusal, status=409)

    async def test_a_body_sent_in_pieces_arrives_whole(self) -> None:
        response = await self.client.get(f"{self.base}/pieces", headers={}, timeout=10)
        self.assertEqual(200, response.status)
        self.assertEqual(len(self.body), len(response.body))
        self.assertEqual(self.body, response.body)
        self.assertEqual(str(len(self.body)), response.header("x-tail-bytes"))

    async def test_a_body_past_the_bound_is_refused_rather_than_cut(self) -> None:
        client = AiohttpClient(max_body_bytes=1024 * 1024)
        try:
            with self.assertRaises(HttpUnavailable):
                await client.get(f"{self.base}/pieces", headers={}, timeout=10)
        finally:
            await client.close()

    async def test_a_connection_closed_before_the_promised_length_is_a_failure(self) -> None:
        with self.assertRaises(HttpUnavailable):
            await self.client.get(f"{self.base}/cut", headers={}, timeout=10)

    async def test_a_refusal_comes_back_with_its_status_and_json_body(self) -> None:
        response = await self.client.get(f"{self.base}/refused", headers={}, timeout=10)
        self.assertEqual(409, response.status)
        self.assertEqual(self.refusal, response.json())


if __name__ == "__main__":
    unittest.main()
