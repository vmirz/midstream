"""What the tail app asks the control plane: who a caller is, a table's schema, and a table itself.

- `GET /auth/session` with the caller's own bearer, to authorize a request as the control plane
  would (`auth.py`);
- `GET /materializations/{id}/tables/{table}/schema?raw=true` with the read-only service credential,
  to seed a table's tail with the schema its runtime conforms rows with;
- `GET /materializations/{id}` and `GET /materializations/{id}/tables/{table}`, with the service
  credential, for a table this member owns but has not been ticked for yet - just after it started
  or gained the table's partition - so a request need not wait up to a minute for the next tick.
"""

from __future__ import annotations

from typing import Any
from urllib.parse import quote

from materializer_control_plane.tail_app.http import HttpClient, HttpResponse, HttpUnavailable

#: Seconds a call to the control plane may take.
CALL_TIMEOUT_SECONDS = 5.0


class ControlPlaneError(RuntimeError):
    """The control plane answered with an error, or could not be reached."""

    def __init__(self, message: str, *, status: int | None = None, body: Any = None) -> None:
        super().__init__(message)
        self.status = status
        self.body = body


class ControlPlane:
    def __init__(self, base_url: str | None, *, service_token: str | None, http: HttpClient, timeout: float = CALL_TIMEOUT_SECONDS) -> None:
        self.base_url = (base_url or "").rstrip("/") or None
        self.service_token = service_token
        self.http = http
        self.timeout = timeout

    async def session(self, token: str | None, *, forwarded: dict[str, str] | None = None) -> dict[str, Any]:
        """`GET /auth/session` as the caller: their principal, or a guest's with `session_error`."""
        headers = dict(forwarded or {})
        if token:
            headers["Authorization"] = f"Bearer {token}"
        response = await self._get("/auth/session", headers=headers)
        if response.status != 200:
            raise ControlPlaneError(f"the control plane answered {response.status} to a session check", status=response.status, body=_body(response))
        payload = response.json()
        if not isinstance(payload, dict) or not isinstance(payload.get("principal"), dict):
            raise ControlPlaneError("the control plane's session answer has no principal")
        return payload

    async def schema(self, materialization_id: str, table: str) -> dict[str, Any]:
        """A table's current schema as the runtime conforms with it: Iceberg schema, fork map, rejected paths."""
        return await self._service_json(f"/materializations/{_segment(materialization_id)}/tables/{_segment(table)}/schema?raw=true")

    async def materialization(self, materialization_id: str) -> dict[str, Any]:
        return await self._service_json(f"/materializations/{_segment(materialization_id)}")

    async def table(self, materialization_id: str, table: str) -> dict[str, Any]:
        return await self._service_json(f"/materializations/{_segment(materialization_id)}/tables/{_segment(table)}")

    async def _service_json(self, path: str) -> dict[str, Any]:
        if not self.service_token:
            raise ControlPlaneError("no service credential is configured (MATERIALIZER_TAIL_SERVICE_TOKEN)")
        response = await self._get(path, headers={"Authorization": f"Bearer {self.service_token}"})
        if response.status != 200:
            raise ControlPlaneError(f"the control plane answered {response.status} to {path.split('?')[0]}", status=response.status, body=_body(response))
        payload = response.json()
        if not isinstance(payload, dict):
            raise ControlPlaneError(f"the control plane's answer to {path} is not an object")
        return payload

    async def _get(self, path: str, *, headers: dict[str, str]) -> HttpResponse:
        if self.base_url is None:
            raise ControlPlaneError("no control plane URL is configured (MATERIALIZER_TAIL_CONTROL_PLANE_URL)")
        try:
            return await self.http.get(f"{self.base_url}{path}", headers=headers, timeout=self.timeout)
        except HttpUnavailable as error:
            raise ControlPlaneError(str(error)) from error


def _segment(value: str) -> str:
    return quote(str(value), safe="")


def _body(response: HttpResponse) -> Any:
    try:
        return response.json()
    except ValueError:
        return None
