"""Who may read a tail: the console alone, for people who may query the table's dataset (handbook 19, §12)."""

from __future__ import annotations

import logging
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from tail_support import (  # noqa: E402
    DATASET_ID,
    OTHER_DATASET_ID,
    TailHarness,
    principal,
    reader_on,
    target,
    tick,
)

from materializer_control_plane.identity.permissions import PERMISSION_KEYS  # noqa: E402
from materializer_control_plane.identity.principals import principal_from_description  # noqa: E402
from materializer_control_plane.tail_app.views import tail_head_view, tail_health_view, tail_members_view, tail_rows_view  # noqa: E402
from materializer_control_plane.web.errors import WebError  # noqa: E402


class PrincipalFromDescriptionTests(unittest.TestCase):
    """The tail app rebuilds a principal from `GET /auth/session`, and decides as the control plane."""

    def test_a_rebuilt_principal_can_exactly_what_the_original_can_on_every_dataset(self) -> None:
        cases = {
            "a reader of one dataset": reader_on(DATASET_ID),
            "an administrator": _administrator(),
            "a token limited to one dataset": principal("token", permissions=["catalog.read", "data.query"], limit=[OTHER_DATASET_ID]),
            "the service": principal("service"),
            "a guest": principal("guest", permissions=["materializations.view"]),
            "a person who must change their password": principal(permissions=["data.query"], password_change=True),
        }
        datasets = (DATASET_ID, OTHER_DATASET_ID, "ds-000000000003", None)
        for name, original in cases.items():
            rebuilt = principal_from_description(original.describe())
            with self.subTest(principal=name):
                self.assertEqual(original.kind, rebuilt.kind)
                self.assertEqual(original.password_change_required, rebuilt.password_change_required)
                for permission in sorted(PERMISSION_KEYS):
                    for dataset in datasets:
                        self.assertEqual(original.can(permission, dataset), rebuilt.can(permission, dataset), (permission, dataset))
                    self.assertEqual(original.can_anywhere(permission), rebuilt.can_anywhere(permission), permission)

    def test_an_unknown_kind_is_a_guest(self) -> None:
        self.assertEqual("guest", principal_from_description({"kind": "root", "permissions": ["data.query"]}).kind)


def _administrator():
    from materializer_control_plane.identity.principals import Grants, Principal

    return Principal(kind="user", grants=Grants(group_ids=("admin",), role_ids=("administrator",), permissions=frozenset(), all_permissions=True), user_id="usr-1", email="a@example.com")


class TailSecurityTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.harness = TailHarness(members=1)
        self.target = target()
        self.harness.table(self.target)
        self.harness.produce("orders", 0, 2, value={"order_id": "secret-value-1234", "amount": 1})
        self.harness.deliver(tick(self.target))
        self.cp = self.harness.control_plane
        self.reader = self.cp.token("tok-reader", reader_on(DATASET_ID))
        self.worker = self.harness.worker

    async def warm(self) -> None:
        try:
            await self.rows(self.reader)
        except WebError:
            pass
        await self.harness.settle()

    async def rows(self, token, *, console: bool = True, forged: str | None = None, headers: dict | None = None):
        request = self.harness.request(token=token, after={}, console=console)
        if forged is not None:
            request.headers["X-Tail-Forwarded-By"] = forged
        request.headers.update(headers or {})
        return await tail_rows_view(request, "mat-1", "orders", worker=self.worker)

    async def refused(self, coroutine) -> WebError:
        with self.assertRaises(WebError) as caught:
            await coroutine
        return caught.exception

    async def test_every_view_answers_the_console_alone(self) -> None:
        await self.warm()
        views = {
            "rows": lambda request: tail_rows_view(request, "mat-1", "orders", worker=self.worker),
            "head": lambda request: tail_head_view(request, "mat-1", "orders", worker=self.worker),
            "health": lambda request: tail_health_view(request, worker=self.worker),
            "members": lambda request: tail_members_view(request, worker=self.worker),
        }
        for name, view in views.items():
            for console in (None, "a-wrong-credential-of-enough-length-000000000"):
                with self.subTest(view=name, console=console):
                    request = self.harness.request(token=self.reader, after={}, console=False, rows=10)
                    if console is not None:
                        request.headers["X-Materializer-Console"] = console
                    error = await self.refused(view(request))
                    self.assertEqual((403, "console_only"), (error.status_code, error.details["reason"]))

    async def test_a_person_who_may_query_the_dataset_reads_its_tail(self) -> None:
        await self.warm()
        self.assertEqual("2", (await self.rows(self.reader))["headers"]["X-Tail-Rows"])

    async def test_no_session_is_asked_to_sign_in(self) -> None:
        await self.warm()
        error = await self.refused(self.rows(None))
        self.assertEqual((401, "sign_in_required"), (error.status_code, error.details["reason"]))

    async def test_a_person_without_the_datasets_query_permission_is_refused_naming_it(self) -> None:
        await self.warm()
        elsewhere = self.cp.token("tok-other", reader_on(OTHER_DATASET_ID))
        error = await self.refused(self.rows(elsewhere))
        self.assertEqual((403, "data.query", "sales"), (error.status_code, error.details["permission"], error.details["dataset"]))

    async def test_a_token_limited_to_another_dataset_is_refused(self) -> None:
        await self.warm()
        limited = self.cp.token("tok_limited", principal("token", permissions=["data.query", "catalog.read"], limit=[OTHER_DATASET_ID]))
        error = await self.refused(self.rows(limited))
        self.assertEqual(403, error.status_code)

    async def test_the_service_credential_reads_no_rows_but_health(self) -> None:
        await self.warm()
        from tail_support import CONSOLE_TOKEN

        self.cp.sessions[CONSOLE_TOKEN] = principal("service")
        for view in (
            tail_rows_view(self.harness.request(token=CONSOLE_TOKEN, after={}), "mat-1", "orders", worker=self.worker),
            tail_head_view(self.harness.request(token=CONSOLE_TOKEN, rows=5), "mat-1", "orders", worker=self.worker),
        ):
            error = await self.refused(view)
            self.assertEqual(403, error.status_code)
        health = await tail_health_view(self.harness.request(token=CONSOLE_TOKEN), worker=self.worker)
        self.assertEqual(self.worker.config.member_id, health["member"])

    async def test_a_person_who_must_change_their_password_is_refused(self) -> None:
        await self.warm()
        pending = self.cp.token("tok-pending", principal(datasets={DATASET_ID: ["data.query"]}, password_change=True))
        error = await self.refused(self.rows(pending))
        self.assertEqual((403, "password_change_required"), (error.status_code, error.details["reason"]))

    async def test_an_ended_session_is_refused_once_the_check_runs_out(self) -> None:
        await self.warm()
        await self.rows(self.reader)
        self.cp.session_errors[self.reader] = "session_ended"

        self.assertEqual("2", (await self.rows(self.reader))["headers"]["X-Tail-Rows"], "trusted for the check's 5 seconds")
        self.harness.clock.advance(5)
        error = await self.refused(self.rows(self.reader))
        self.assertEqual((401, "session_ended"), (error.status_code, error.details["reason"]))

    async def test_a_revoked_access_token_is_refused(self) -> None:
        await self.warm()
        token = self.cp.token("tok_reader", principal("token", permissions=["data.query"], datasets={DATASET_ID: ["data.query"]}))
        self.cp.session_errors[token] = "token_revoked"
        error = await self.refused(self.rows(token))
        self.assertEqual((401, "token_revoked"), (error.status_code, error.details["reason"]))

    async def test_sessions_are_trusted_through_a_control_plane_outage_for_the_grace_then_not(self) -> None:
        await self.warm()
        await self.rows(self.reader)
        self.cp.down = True
        self.harness.clock.advance(30)

        self.assertEqual("2", (await self.rows(self.reader))["headers"]["X-Tail-Rows"])
        self.harness.clock.advance(40)
        error = await self.refused(self.rows(self.reader))
        self.assertEqual((503, "control_plane_unreachable"), (error.status_code, error.details["reason"]))
        self.cp.down = False
        self.assertEqual("2", (await self.rows(self.reader))["headers"]["X-Tail-Rows"], "and back once it answers")

    async def test_a_forged_forwarding_header_gains_nothing(self) -> None:
        await self.warm()
        # To the owner: the bearer is checked as for any request.
        error = await self.refused(self.rows(None, forged="materializer-tail-app-9"))
        self.assertEqual(401, error.status_code)
        elsewhere = self.cp.token("tok-other", reader_on(OTHER_DATASET_ID))
        error = await self.refused(self.rows(elsewhere, forged="materializer-tail-app-9"))
        self.assertEqual(403, error.status_code)

    async def test_path_parameters_are_names_never_paths(self) -> None:
        await self.warm()
        for materialization_id, table in (("..", "orders"), ("mat-1", "../orders"), ("mat-1/x", "orders"), ("mat-1", "Orders"), ("mat-1", "o" * 64)):
            with self.subTest(materialization_id=materialization_id, table=table):
                error = await self.refused(tail_rows_view(self.harness.request(token=self.reader, after={}), materialization_id, table, worker=self.worker))
                self.assertEqual(400, error.status_code)
        error = await self.refused(tail_rows_view(self.harness.request(token=self.reader, after={}), "mat-1", "nothing_here", worker=self.worker))
        self.assertEqual((404, "live_tail_off"), (error.status_code, error.details["reason"]))
        self.assertNotIn("orders", error.message, "an unknown table's answer names no other")

    async def test_bad_queries_are_refused_with_the_field(self) -> None:
        await self.warm()
        for query, field_name in (({"after": "not json"}, "after"), ({"after": '{"0": "x"}'}, "after"), ({"after": '{"0": -2}'}, "after"), ({"after": "[1]"}, "after"), ({"after": "{}", "max_rows": "0"}, "max_rows"), ({"after": "{}", "format": "csv"}, "format")):
            with self.subTest(query=query):
                request = self.harness.request(token=self.reader)
                request.query = dict(query)
                error = await self.refused(tail_rows_view(request, "mat-1", "orders", worker=self.worker))
                self.assertEqual((400, field_name), (error.status_code, error.details["field"]))
        request = self.harness.request(token=self.reader)
        request.query = {}
        self.assertEqual("after", (await self.refused(tail_rows_view(request, "mat-1", "orders", worker=self.worker))).details["field"])

    async def test_a_peek_needs_the_datasets_query_permission_too(self) -> None:
        await self.warm()
        elsewhere = self.cp.token("tok-other", reader_on(OTHER_DATASET_ID))
        error = await self.refused(tail_head_view(self.harness.request(token=elsewhere, rows=5), "mat-1", "orders", worker=self.worker))
        self.assertEqual(403, error.status_code)

    async def test_no_row_content_reaches_a_log_line(self) -> None:
        self.harness.deliver(tick(self.target, protocol_logging_enabled=True))
        with self.assertLogs("materializer_control_plane", level=logging.DEBUG) as logged:
            await self.warm()
            await self.rows(self.reader)
            await tail_head_view(self.harness.request(token=self.reader, rows=5), "mat-1", "orders", worker=self.worker)
            logging.getLogger("materializer_control_plane.tail_app").info("marker")
        text = "\n".join(logged.output)
        self.assertIn("tail_served", text)
        self.assertNotIn("secret-value-1234", text)
        self.assertNotIn(self.reader, text, "nor a token")


if __name__ == "__main__":
    unittest.main()
