"""Datasets in the directory: creating and sharing them, the table names claimed in them, and
authorizing what a person may do with each one."""

from __future__ import annotations

import os
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
import sys
import unittest
from uuid import uuid4

sys.path.insert(0, str(Path(__file__).resolve().parent))

from test_identity import (  # noqa: E402
    CONFIG,
    NOW,
    NOW_EPOCH,
    SECRETS,
    SHOP,
    FakeTable,
    apply,
    directory,
    http_request,
    make_user,
    request,
    session_token,
    store_dataset,
    store_user,
)
from table_records import build_table_record, table_spec  # noqa: E402

from materializer_control_plane import config as control_plane_config
from materializer_control_plane.handlers.identity import handle_identity_request  # noqa: E402
from materializer_control_plane.identity.authorization import authorize  # noqa: E402
from materializer_control_plane.identity.principals import clear_grants_cache  # noqa: E402
from materializer_control_plane.models import Materialization, MaterializationWorkEvent, PendingTopicChange, TopicChangePlan  # noqa: E402
from materializer_control_plane.models.identity import AuditEntry, Group, Role, TableClaim  # noqa: E402
from materializer_control_plane.services.identity import apply_identity_request  # noqa: E402
from materializer_control_plane.services.table_claims import claim_request, release_request  # noqa: E402
from materializer_control_plane.storage import save_materialization, save_materialization_table  # noqa: E402
from materializer_control_plane.storage.identity import (  # noqa: E402
    find_dataset_by_name,
    get_claim_release,
    get_dataset,
    get_dataset_grant,
    get_directory_state,
    get_request_outcome,
    get_table_claim,
    get_user,
    iter_dataset_grants,
    iter_table_claims,
    save_claim_release,
    save_group,
    save_role,
    save_table_claim,
)
from materializer_control_plane.storage.keys import materialization_routing_key  # noqa: E402
from materializer_control_plane.web.errors import WebError  # noqa: E402

FINANCE = "ds-000000000002"


def outcome_of(table: FakeTable, audit: FakeTable, action: str, actor: str, payload: dict, **kwargs) -> dict:
    """Apply a request of its own - never taken for a redelivered one - and return its outcome."""
    result = apply(table, audit, action, actor, payload, request_id=f"idr-{uuid4().hex[:12]}", **kwargs)
    return result.to_dict()


def audit_actions(audit: FakeTable) -> list[str]:
    return [AuditEntry.from_dict(value["payload"]).action for _, value in audit.prefix_scan("audit:")]


class DatasetDirectoryTests(unittest.TestCase):
    """Creating, changing, sharing and deleting datasets, as the identity agent applies them."""

    def setUp(self) -> None:
        clear_grants_cache()
        self.table, self.audit = directory()

    def create(self, actor: str = "manager", name: str = "sales", dataset_id: str = "ds-0000000000aa", **payload) -> dict:
        return outcome_of(self.table, self.audit, "create_dataset", actor, {"dataset_id": dataset_id, "name": name, **payload})

    def test_creating_a_dataset_makes_its_creator_owner_and_everyone_signed_in_a_reader(self) -> None:
        revision = get_directory_state(self.table).revision

        outcome = self.create(description="Orders, refunds")

        self.assertEqual(("applied", "dataset", "ds-0000000000aa"), (outcome["status"], outcome["entity_type"], outcome["entity_id"]))
        dataset = find_dataset_by_name(self.table, "sales")
        self.assertEqual(("ds-0000000000aa", "Orders, refunds", "manager@example.com"), (dataset.dataset_id, dataset.description, dataset.created_by))
        grants = {(grant.principal_type, grant.principal_id): grant.role_ids for grant in iter_dataset_grants(self.table, dataset.dataset_id)}
        self.assertEqual({("user", "manager"): ["dataset-owner"], ("group", "all-users"): ["dataset-reader"]}, grants)
        self.assertEqual(revision + 1, get_directory_state(self.table).revision, "grants changed what people may do")
        self.assertIn("dataset.created", audit_actions(self.audit))

    def test_creating_a_dataset_needs_permission(self) -> None:
        outcome = self.create(actor="viewer")

        self.assertEqual(("rejected", "forbidden"), (outcome["status"], outcome["reason"]))
        self.assertIsNone(find_dataset_by_name(self.table, "sales"))

    def test_a_dataset_name_is_one_sql_reads_and_is_taken_once(self) -> None:
        for name, fragment in (("Sales", "lowercase only"), ("main", "DuckDB"), ("order", "reserved SQL word"), ("sales-eu", "cannot name a dataset"), ("", "required")):
            with self.subTest(name=name):
                outcome = self.create(name=name)
                self.assertEqual(("rejected", "invalid", "name"), (outcome["status"], outcome["reason"], outcome["field"]))
                self.assertIn(fragment, outcome["message"])
        self.assertEqual("applied", self.create()["status"])
        again = self.create(dataset_id="ds-0000000000bb")
        self.assertEqual(("rejected", "already_exists", "name"), (again["status"], again["reason"], again["field"]))
        # The existing `shop` is taken too.
        self.assertEqual("already_exists", self.create(name="shop", dataset_id="ds-0000000000cc")["reason"])

    def test_owners_change_a_dataset_but_never_its_name(self) -> None:
        self.create()
        dataset_id = "ds-0000000000aa"

        changed = outcome_of(self.table, self.audit, "update_dataset", "manager", {"dataset_id": dataset_id, "description": "Everything sold"})
        self.assertEqual("applied", changed["status"])
        self.assertEqual(("Everything sold", 2), (get_dataset(self.table, dataset_id).description, get_dataset(self.table, dataset_id).revision))
        # A reader (everyone signed in) may not.
        refused = outcome_of(self.table, self.audit, "update_dataset", "viewer", {"dataset_id": dataset_id, "description": "x"})
        self.assertEqual("forbidden", refused["reason"])
        renamed = outcome_of(self.table, self.audit, "update_dataset", "manager", {"dataset_id": dataset_id, "name": "revenue"})
        self.assertEqual(("invalid", "name"), (renamed["reason"], renamed["field"]))
        stale = outcome_of(self.table, self.audit, "update_dataset", "manager", {"dataset_id": dataset_id, "description": "y"}, base_revision=1)
        self.assertEqual("changed", stale["reason"])

    def test_a_dataset_is_deleted_only_once_it_holds_no_tables(self) -> None:
        self.create()
        dataset_id = "ds-0000000000aa"
        save_table_claim(self.table, TableClaim(dataset_id, "orders", "mat-1", "orders", NOW, "a", "idr-1"))

        refused = outcome_of(self.table, self.audit, "delete_dataset", "manager", {"dataset_id": dataset_id})
        self.assertEqual("dataset_not_empty", refused["reason"])
        self.assertEqual(["orders"], refused["details"]["tables"])

        apply_identity_request(self.table, self.audit, release_request(request_id="idr-r", materialization_id="mat-1", everything=True, reason="deleted", actor_id=None, actor_email=None, submitted_at=NOW), config=CONFIG, secrets=SECRETS, applied_at=NOW)
        deleted = outcome_of(self.table, self.audit, "delete_dataset", "manager", {"dataset_id": dataset_id})

        self.assertEqual("applied", deleted["status"])
        self.assertIsNone(get_dataset(self.table, dataset_id))
        self.assertIsNone(find_dataset_by_name(self.table, "sales"))
        self.assertEqual([], iter_dataset_grants(self.table, dataset_id))
        # The name can be taken again, by a dataset that has nothing of the old one's.
        self.assertEqual("applied", self.create(actor="admin", dataset_id="ds-0000000000dd")["status"])
        self.assertIsNone(get_dataset_grant(self.table, "ds-0000000000dd", "user", "manager"))

    def grant(self, actor: str, principal_type: str, principal_id: str, role_ids: list[str], dataset_id: str = SHOP) -> dict:
        return outcome_of(
            self.table,
            self.audit,
            "set_dataset_grant",
            actor,
            {"dataset_id": dataset_id, "principal_type": principal_type, "principal_id": principal_id, "role_ids": role_ids},
        )

    def test_an_owner_shares_a_dataset_with_people_and_groups(self) -> None:
        self.assertEqual("applied", self.grant("admin", "user", "manager", ["dataset-writer"])["status"])
        self.assertEqual(["dataset-writer"], get_dataset_grant(self.table, SHOP, "user", "manager").role_ids)
        self.assertEqual("applied", self.grant("admin", "group", "grp-managers", ["dataset-reader"])["status"])
        # No roles removes the grant.
        self.assertEqual("applied", self.grant("admin", "user", "manager", [])["status"])
        self.assertIsNone(get_dataset_grant(self.table, SHOP, "user", "manager"))
        self.assertIn("dataset.access_changed", audit_actions(self.audit))

    def test_what_a_dataset_cannot_be_shared_with_or_as(self) -> None:
        for principal_type, principal_id, role_ids, reason in (
            ("group", "guest", ["dataset-reader"], "invalid"),
            ("user", "usr-nobody", ["dataset-reader"], "not_found"),
            ("group", "grp-nothing", ["dataset-reader"], "not_found"),
            ("user", "manager", ["operator"], "invalid"),
            ("user", "manager", ["administrator"], "invalid"),
            ("user", "manager", ["rol-nothing"], "not_found"),
            ("robot", "manager", ["dataset-reader"], "invalid"),
        ):
            with self.subTest(principal=principal_id, roles=role_ids):
                self.assertEqual(reason, self.grant("admin", principal_type, principal_id, role_ids)["reason"])

    def test_only_who_manages_a_dataset_shares_it(self) -> None:
        # The viewer reads shop, through the Viewer group; reading is not managing.
        self.assertEqual("forbidden", self.grant("viewer", "user", "viewer", ["dataset-owner"])["reason"])

    def test_nobody_grants_on_a_dataset_access_they_do_not_hold_there(self) -> None:
        curator = Role(role_id="rol-curator", name="Curator", permissions=["dataset.manage"], created_at=NOW, created_by="a", updated_at=NOW, updated_by="a")
        save_role(self.table, curator)
        self.grant("admin", "user", "viewer", ["rol-curator"])

        refused = self.grant("viewer", "user", "manager", ["dataset-writer"])

        self.assertEqual("forbidden", refused["reason"])
        self.assertIn("Add and drop tables on dataset shop", refused["message"])
        # What they hold there - reading, and managing - they may hand out.
        self.assertEqual("applied", self.grant("viewer", "user", "manager", ["dataset-reader"])["status"])

    def test_deleting_a_person_group_or_role_takes_their_dataset_grants_with_them(self) -> None:
        group = Group(group_id="grp-ops", name="Ops", role_ids=[], created_at=NOW, created_by="a", updated_at=NOW, updated_by="a")
        save_group(self.table, group)
        curator = Role(role_id="rol-curator", name="Curator", permissions=["dataset.view_schema"], created_at=NOW, created_by="a", updated_at=NOW, updated_by="a")
        save_role(self.table, curator)
        self.grant("admin", "group", "grp-ops", ["dataset-reader"])
        self.grant("admin", "user", "manager", ["rol-curator", "dataset-reader"])
        store_user(self.table, make_user("leaver", "leaver@example.com", status="deactivated"))
        self.grant("admin", "user", "leaver", ["dataset-reader"])

        self.assertEqual("applied", outcome_of(self.table, self.audit, "delete_group", "admin", {"group_id": "grp-ops"})["status"])
        self.assertEqual("applied", outcome_of(self.table, self.audit, "delete_role", "admin", {"role_id": "rol-curator"})["status"])
        self.assertEqual("applied", outcome_of(self.table, self.audit, "delete_user", "admin", {"user_id": "leaver"})["status"])

        holders = {grant.principal_id: grant.role_ids for grant in iter_dataset_grants(self.table, SHOP)}
        self.assertNotIn("grp-ops", holders)
        self.assertNotIn("leaver", holders)
        self.assertEqual(["dataset-reader"], holders["manager"])

    def test_adding_someone_to_a_group_needs_the_access_the_group_has_on_datasets(self) -> None:
        """The user manager holds nothing on finance; the Ops group owns it."""
        store_dataset(self.table, FINANCE, name="finance", grants={"grp-ops": ("group", "dataset-owner")})
        save_group(self.table, Group(group_id="grp-ops", name="Ops", role_ids=[], created_at=NOW, created_by="a", updated_at=NOW, updated_by="a"))

        refused = outcome_of(self.table, self.audit, "update_group", "manager", {"group_id": "grp-ops", "add_member_ids": ["viewer"]})

        self.assertEqual("forbidden", refused["reason"])
        self.assertIn("on dataset finance", refused["message"])
        self.assertEqual("applied", outcome_of(self.table, self.audit, "update_group", "admin", {"group_id": "grp-ops", "add_member_ids": ["viewer"]})["status"])


def claim_tables(materialization_id: str, *specs, actor: str = "manager", request_id: str = "idr-claim-1", purpose: str = "create", release=()):
    return claim_request(
        request_id=request_id,
        materialization_id=materialization_id,
        purpose=purpose,
        materialization_request_id="req-1",
        tables=specs,
        release=release,
        actor_id=actor,
        actor_email=f"{actor}@example.com",
        submitted_at=NOW,
    )


class TableClaimTests(unittest.TestCase):
    """`<dataset>.<table>` names, taken before any table exists - all of a request's, or none."""

    def setUp(self) -> None:
        clear_grants_cache()
        self.table, self.audit = directory()
        # The manager writes to shop; finance they may only read.
        store_dataset(self.table, FINANCE, name="finance", grants={"all-users": ("group", "dataset-reader")})
        outcome_of(self.table, self.audit, "set_dataset_grant", "admin", {"dataset_id": SHOP, "principal_type": "user", "principal_id": "manager", "role_ids": ["dataset-writer"]})

    def apply(self, identity_request):
        return apply_identity_request(self.table, self.audit, identity_request, config=CONFIG, secrets=SECRETS, applied_at=NOW)

    def test_every_name_is_claimed_or_none(self) -> None:
        save_table_claim(self.table, TableClaim(SHOP, "payments", "mat-0", "payments", NOW, "a", "idr-0"))

        refused = self.apply(claim_tables("mat-1", table_spec("orders", "orders"), table_spec("payments", "payments")))

        self.assertEqual(("rejected", "table_name_taken", "tables.1.name"), (refused.status, refused.reason, refused.field))
        self.assertEqual(
            {"index": 1, "dataset": "shop", "dataset_id": SHOP, "table": "payments", "source_topic": "payments", "materialization_id": "mat-0"},
            refused.details,
        )
        self.assertIsNone(get_table_claim(self.table, SHOP, "orders"), "nothing of a refused claim is taken")

        claimed = self.apply(claim_tables("mat-1", table_spec("orders", "orders"), request_id="idr-claim-2"))
        self.assertEqual("applied", claimed.status)
        claim = get_table_claim(self.table, SHOP, "orders")
        self.assertEqual(("mat-1", "orders", "manager@example.com", "idr-claim-2"), (claim.materialization_id, claim.source_topic, claim.claimed_by, claim.request_id))
        self.assertIn("dataset.table_claimed", audit_actions(self.audit))

    def test_a_dataset_that_is_gone_or_not_writable_is_refused(self) -> None:
        missing = self.apply(claim_tables("mat-1", table_spec("orders", "orders", dataset="marketing", dataset_id="ds-00000000ffff")))
        self.assertEqual(("dataset_not_found", "tables.0.dataset"), (missing.reason, missing.field))

        reader = self.apply(claim_tables("mat-1", table_spec("invoices", "invoices", dataset="finance", dataset_id=FINANCE), request_id="idr-claim-3"))
        self.assertEqual(("forbidden", "tables.0.dataset"), (reader.reason, reader.field))
        self.assertIn("Add and drop tables", reader.message)

        gone = self.apply(claim_tables("mat-1", table_spec("orders", "orders"), actor="usr-nobody", request_id="idr-claim-4"))
        self.assertEqual("forbidden", gone.reason)

    def test_a_materialization_may_claim_its_own_name_again(self) -> None:
        self.apply(claim_tables("mat-1", table_spec("orders", "orders")))

        again = self.apply(claim_tables("mat-1", table_spec("orders", "orders"), request_id="idr-claim-again"))

        self.assertEqual("applied", again.status)
        self.assertEqual(1, len(iter_table_claims(self.table)))

    def test_a_redelivered_claim_answers_as_it_did(self) -> None:
        first = self.apply(claim_tables("mat-1", table_spec("orders", "orders")))
        save_table_claim(self.table, TableClaim(SHOP, "orders", "mat-0", "orders", NOW, "a", "idr-0"))

        self.assertEqual(first, self.apply(claim_tables("mat-1", table_spec("orders", "orders"))))

    def test_releases_free_only_the_materializations_own_names(self) -> None:
        self.apply(claim_tables("mat-1", table_spec("orders", "orders"), table_spec("payments", "payments")))
        save_table_claim(self.table, TableClaim(FINANCE, "orders", "mat-2", "orders", NOW, "a", "idr-x"))

        self.apply(release_request(request_id="idr-r1", materialization_id="mat-1", tables=[table_spec("payments", "payments"), table_spec("orders", "orders", dataset="finance", dataset_id=FINANCE)], reason="dropped", actor_id="manager", actor_email="m", submitted_at=NOW))

        self.assertIsNotNone(get_table_claim(self.table, SHOP, "orders"))
        self.assertIsNone(get_table_claim(self.table, SHOP, "payments"))
        self.assertIsNotNone(get_table_claim(self.table, FINANCE, "orders"), "another materialization's name is never freed")
        self.assertIn("dataset.table_released", audit_actions(self.audit))

    def test_a_deleted_materialization_frees_everything_and_claims_nothing_after(self) -> None:
        self.apply(claim_tables("mat-1", table_spec("orders", "orders")))

        self.apply(release_request(request_id="idr-r2", materialization_id="mat-1", everything=True, reason="deleted", actor_id=None, actor_email=None, submitted_at=NOW))

        self.assertEqual([], iter_table_claims(self.table))
        self.assertIsNotNone(get_claim_release(self.table, "mat-1"))
        late = self.apply(claim_tables("mat-1", table_spec("refunds", "refunds"), request_id="idr-late"))
        self.assertEqual("materialization_deleted", late.reason)

    def test_a_change_of_topics_claims_and_frees_in_one_step(self) -> None:
        self.apply(claim_tables("mat-1", table_spec("orders", "orders"), table_spec("payments", "payments")))
        save_table_claim(self.table, TableClaim(SHOP, "refunds", "mat-0", "refunds", NOW, "a", "idr-0"))

        refused = self.apply(claim_tables("mat-1", table_spec("refunds", "refunds"), release=[table_spec("payments", "payments")], purpose="update", request_id="idr-u1"))
        self.assertEqual("table_name_taken", refused.reason)
        self.assertIsNotNone(get_table_claim(self.table, SHOP, "payments"), "a refused change keeps the names it would drop")

        applied = self.apply(claim_tables("mat-1", table_spec("refunds_2", "refunds"), release=[table_spec("payments", "payments")], purpose="update", request_id="idr-u2"))
        self.assertEqual("applied", applied.status)
        self.assertIsNone(get_table_claim(self.table, SHOP, "payments"))
        self.assertEqual("mat-1", get_table_claim(self.table, SHOP, "refunds_2").materialization_id)

    def test_the_directory_answers_the_claiming_materializations_workflow(self) -> None:
        value = claim_tables("mat-1", table_spec("orders", "orders")).to_dict()

        output = handle_identity_request(value, self.table, self.audit, None, config=CONFIG)

        self.assertEqual(("work", materialization_routing_key("mat-1")), (output["route"], output["key"]))
        event = MaterializationWorkEvent.from_dict(output["value"])
        self.assertEqual(("tables_claimed", "mat-1", "req-1"), (event.step, event.materialization_id, event.caused_by.id))
        self.assertEqual(("idr-claim-1", "create", "applied"), (event.claim.request_id, event.claim.purpose, event.claim.status))
        # Delivered again, it answers again, the same.
        self.assertEqual(output, handle_identity_request(value, self.table, self.audit, None, config=CONFIG))
        # A refusal answers too.
        refused = handle_identity_request(claim_tables("mat-2", table_spec("orders", "orders"), request_id="idr-claim-2").to_dict(), self.table, self.audit, None, config=CONFIG)
        refusal = MaterializationWorkEvent.from_dict(refused["value"])
        self.assertEqual(("claim_rejected", "table_name_taken"), (refusal.step, refusal.claim.reason))
        self.assertEqual("orders", refusal.claim.details["table"])
        # Nothing else answers anyone.
        self.assertIsNone(handle_identity_request(request("maintenance", "system").to_dict(), self.table, self.audit, None, config=CONFIG))

    def test_whoever_asked_may_follow_their_claim(self) -> None:
        from materializer_control_plane.web.identity import identity_request_view

        self.apply(claim_tables("mat-1", table_spec("orders", "orders")))
        manager = authorize(http_request(session_token(get_user(self.table, "manager"))), self.table, None, secrets=SECRETS, now=NOW_EPOCH)

        self.assertEqual("applied", identity_request_view(self.table, manager, "idr-claim-1")["status"])
        self.assertEqual("applied", get_request_outcome(self.table, "idr-claim-1").status)


def build_materialization(materialization_id: str, *specs, desired_phase: str = "running", updated_at: str = "2026-10-13T10:00:00Z") -> Materialization:
    return Materialization(
        materialization_id=materialization_id,
        namespace="midstream",
        name="Shop",
        tables=specs,
        destination_uri="gs://lake",
        desired_phase=desired_phase,
        created_at=updated_at,
        updated_at=updated_at,
    )


class ClaimSweepTests(unittest.TestCase):
    """The hourly maintenance releases claims whose table is gone, and repairs a lost one."""

    LATER = "2026-10-13T14:00:00Z"

    def setUp(self) -> None:
        clear_grants_cache()
        self.table, self.audit = directory()
        self.state = FakeTable()

    def sweep(self, at: str | None = None) -> None:
        apply_identity_request(
            self.table,
            self.audit,
            request("maintenance", "system", request_id=f"idr-maintenance-{at}"),
            config=CONFIG,
            secrets=SECRETS,
            applied_at=at or self.LATER,
            control_plane_table=self.state,
        )

    def claim(self, table: str, materialization_id: str, claimed_at: str = "2026-10-13T10:00:00Z") -> None:
        save_table_claim(self.table, TableClaim(SHOP, table, materialization_id, table, claimed_at, "a", "idr-0"))

    def test_claims_of_a_materialization_gone_or_going_are_released(self) -> None:
        save_materialization(self.state, build_materialization("mat-deleting", table_spec("going", "going"), desired_phase="deleting"))
        self.claim("gone", "mat-purged")
        self.claim("going", "mat-deleting")
        # Too recent to be sure of: the materialization may not have reached this replica yet.
        self.claim("fresh", "mat-new", claimed_at="2026-10-13T13:30:00Z")

        self.sweep()

        self.assertEqual(["fresh"], [claim.table for claim in iter_table_claims(self.table)])

    def test_the_tables_a_materialization_has_or_waits_for_keep_their_names(self) -> None:
        orders, payments, refunds = table_spec("orders", "orders"), table_spec("payments", "payments"), table_spec("refunds", "refunds")
        plan = TopicChangePlan(tables=(orders, refunds), retired_tables=(payments,), added=(refunds,), resumed=(), removed=(), retired=(), dropped=())
        materialization = replace(
            build_materialization("mat-1", orders),
            retired_tables=(payments,),
            pending_topic_change=PendingTopicChange(request_id="req-2", claim_request_id="idr-2", submitted_at=NOW, plan=plan),
        )
        save_materialization(self.state, materialization)
        for spec in (orders, payments):
            save_materialization_table(self.state, build_table_record(materialization, table=spec.name, state="active" if spec is orders else "retired"))
        for name in ("orders", "payments", "refunds", "dropped_long_ago"):
            self.claim(name, "mat-1")

        self.sweep()

        self.assertEqual(["orders", "payments", "refunds"], [claim.table for claim in iter_table_claims(self.table)])

    def test_a_live_table_that_lost_its_claim_takes_its_name_again(self) -> None:
        orders = table_spec("orders", "orders")
        materialization = build_materialization("mat-1", orders)
        save_materialization(self.state, materialization)
        save_materialization_table(self.state, build_table_record(materialization, table="orders"))

        self.sweep()

        claim = get_table_claim(self.table, SHOP, "orders")
        self.assertEqual(("mat-1", "system"), (claim.materialization_id, claim.claimed_by))

    def test_a_materialization_changed_a_moment_ago_is_left_alone(self) -> None:
        save_materialization(self.state, build_materialization("mat-1", table_spec("orders", "orders"), updated_at="2026-10-13T13:59:00Z"))
        self.claim("stale", "mat-1")

        self.sweep()

        self.assertEqual(["stale"], [claim.table for claim in iter_table_claims(self.table)])

    def test_old_releases_are_forgotten(self) -> None:
        from materializer_control_plane.models.identity import ClaimRelease

        save_claim_release(self.table, ClaimRelease("mat-old", "2026-10-01T00:00:00Z"))
        save_claim_release(self.table, ClaimRelease("mat-recent", "2026-10-12T00:00:00Z"))

        self.sweep()

        self.assertIsNone(get_claim_release(self.table, "mat-old"))
        self.assertIsNotNone(get_claim_release(self.table, "mat-recent"))


class DatasetAuthorizationTests(unittest.TestCase):
    """What a person, or their token, may do on each dataset."""

    def setUp(self) -> None:
        clear_grants_cache()
        self.table, self.audit = directory()
        store_dataset(self.table, FINANCE, name="finance", grants={"manager": ("user", "dataset-owner")})

    def token_for(self, user_id: str) -> str:
        return session_token(get_user(self.table, user_id))

    def authorize(self, user_id: str, permission: str, **kwargs):
        clear_grants_cache()
        return authorize(http_request(self.token_for(user_id)), self.table, permission, secrets=SECRETS, now=NOW_EPOCH, **kwargs)

    def test_a_permission_on_a_dataset_holds_there_and_nowhere_else(self) -> None:
        principal = self.authorize("viewer", "data.query", dataset="shop")
        self.assertTrue(principal.can("data.query", SHOP))
        self.assertFalse(principal.can("data.query", FINANCE))
        self.assertFalse(principal.can("data.query"), "not across the deployment")
        with self.assertRaises(WebError) as caught:
            self.authorize("viewer", "data.query", dataset="finance")
        self.assertEqual((403, "data.query", "finance"), (caught.exception.status_code, caught.exception.details["permission"], caught.exception.details["dataset"]))
        self.assertIn("on dataset finance", caught.exception.message)

    def test_some_dataset_is_enough_when_any_dataset_is_asked_for(self) -> None:
        self.assertEqual("viewer", self.authorize("viewer", "catalog.read", any_dataset=True).user_id)
        store_user(self.table, make_user("nobody", "nobody@example.com"))
        outcome_of(self.table, self.audit, "update_group", "admin", {"group_id": "viewer", "role_ids": ["viewer"]})
        with self.assertRaises(WebError):
            self.authorize("nobody", "catalog.write", any_dataset=True)

    def test_an_administrator_holds_everything_on_every_dataset(self) -> None:
        principal = self.authorize("admin", "dataset.manage", dataset="finance")
        self.assertTrue(principal.can("catalog.write", FINANCE))

    def test_a_dataset_that_does_not_exist_is_404_to_someone_known(self) -> None:
        with self.assertRaises(WebError) as caught:
            self.authorize("admin", "dataset.manage", dataset="nothing")
        self.assertEqual(404, caught.exception.status_code)
        with self.assertRaises(WebError) as caught:
            authorize(http_request(), self.table, "dataset.manage", dataset="nothing", secrets=SECRETS, now=NOW_EPOCH)
        self.assertEqual(404, caught.exception.status_code)

    def test_only_dataset_permissions_are_asked_for_on_a_dataset(self) -> None:
        with self.assertRaises(ValueError):
            self.authorize("admin", "users.manage", dataset="shop")

    def test_a_token_limited_to_datasets_reads_only_those(self) -> None:
        from materializer_control_plane.web.tokens import create_access_token_request

        manager = self.authorize("manager", None)
        acceptance = create_access_token_request(manager, {"name": "BI", "datasets": ["finance"]}, request=http_request(), directory_table=self.table, now=datetime(2026, 10, 13, 12, tzinfo=UTC))
        self.assertEqual([{"dataset_id": FINANCE, "name": "finance"}], acceptance.response["datasets"])
        self.assertEqual("applied", apply_identity_request(self.table, self.audit, acceptance.request_event, config=CONFIG, secrets=SECRETS, applied_at=NOW).status)
        clear_grants_cache()
        token = authorize(http_request(acceptance.response["token"]), self.table, "catalog.read", any_dataset=True, secrets=SECRETS, now=NOW_EPOCH)

        self.assertTrue(token.can("catalog.read", FINANCE))
        self.assertFalse(token.can("catalog.read", SHOP), "the manager reads shop, the token does not")
        self.assertFalse(token.can("catalog.read"))
        self.assertEqual([FINANCE], token.describe()["dataset_limit"])

        # Its owner losing access to the dataset narrows it at once.
        outcome_of(self.table, self.audit, "set_dataset_grant", "admin", {"dataset_id": FINANCE, "principal_type": "user", "principal_id": "manager", "role_ids": []})
        clear_grants_cache()
        with self.assertRaises(WebError):
            authorize(http_request(acceptance.response["token"]), self.table, "catalog.read", any_dataset=True, secrets=SECRETS, now=NOW_EPOCH)

    def test_a_token_is_limited_only_to_datasets_its_owner_may_use_it_on(self) -> None:
        from materializer_control_plane.web.tokens import create_access_token_request

        viewer = self.authorize("viewer", None)
        with self.assertRaises(WebError) as caught:
            create_access_token_request(viewer, {"name": "BI", "datasets": ["finance"]}, request=http_request(), directory_table=self.table)
        self.assertEqual(403, caught.exception.status_code)
        with self.assertRaises(WebError) as caught:
            create_access_token_request(viewer, {"name": "BI", "datasets": ["nothing"]}, request=http_request(), directory_table=self.table)
        self.assertEqual(404, caught.exception.status_code)
        # The agent checks again.
        refused = outcome_of(self.table, self.audit, "create_access_token", "viewer", {"token_id": "b" * 12, "name": "x", "permissions": ["catalog.read"], "secret_hash": "sha256:" + "0" * 64, "datasets": [FINANCE]})
        self.assertEqual(("forbidden", "datasets"), (refused["reason"], refused["field"]))



class DatasetViewTests(unittest.TestCase):
    """The Datasets API: what everyone with datasets.view sees, and what only owners change."""

    def setUp(self) -> None:
        clear_grants_cache()
        self.table, self.audit = directory()
        self.state = FakeTable()
        store_dataset(self.table, FINANCE, name="finance", grants={"admin": ("user", "dataset-owner")})
        from materializer_control_plane.models import Dataset
        from materializer_control_plane.storage.identity import save_dataset

        save_dataset(self.table, replace(get_dataset(self.table, SHOP), description="Orders and payments"))
        materialization = build_materialization("mat-1", table_spec("orders", "orders"), table_spec("payments", "payments"))
        save_materialization(self.state, replace(materialization, name="Shop orders"))
        save_materialization_table(self.state, build_table_record(materialization, table="orders"))
        save_table_claim(self.table, TableClaim(SHOP, "orders", "mat-1", "orders", NOW, "a", "idr-1"))
        # Claimed, not created yet.
        save_table_claim(self.table, TableClaim(SHOP, "payments", "mat-1", "payments", NOW, "a", "idr-1"))
        self.Dataset = Dataset

    def principal(self, user_id: str):
        clear_grants_cache()
        return authorize(http_request(session_token(get_user(self.table, user_id))), self.table, None, secrets=SECRETS, now=NOW_EPOCH)

    def test_everyone_sees_every_dataset_and_which_they_cannot_read(self) -> None:
        from materializer_control_plane.web.datasets import list_datasets_view

        listed = list_datasets_view(self.table, self.principal("viewer"))

        by_name = {item["name"]: item for item in listed["datasets"]}
        self.assertEqual(["finance", "shop"], [item["name"] for item in listed["datasets"]])
        self.assertFalse(by_name["shop"]["locked"])
        self.assertTrue(by_name["finance"]["locked"], "listed, locked")
        self.assertEqual(["orders", "payments"], by_name["shop"]["tables"])
        self.assertEqual(2, by_name["shop"]["table_count"])
        self.assertEqual([{"type": "user", "id": "admin", "name": "Admin Person", "email": "admin@example.com"}], by_name["shop"]["owners"])
        self.assertEqual(["catalog.read", "data.export", "data.query", "dataset.view_schema"], by_name["shop"]["permissions"])
        self.assertEqual([], by_name["finance"]["permissions"])
        self.assertFalse(listed["can_create"])
        self.assertTrue(list_datasets_view(self.table, self.principal("manager"))["can_create"])

    def test_datasets_are_found_by_name_description_or_table(self) -> None:
        from materializer_control_plane.web.datasets import list_datasets_view

        viewer = self.principal("viewer")
        for query, expected in (("fin", ["finance"]), ("PAYMENTS", ["shop"]), ("orders and", ["shop"]), ("nothing", [])):
            with self.subTest(query=query):
                self.assertEqual(expected, [item["name"] for item in list_datasets_view(self.table, viewer, {"q": query})["datasets"]])

    def test_a_dataset_lists_its_tables_with_their_materializations(self) -> None:
        from materializer_control_plane.web.datasets import get_dataset_view

        shop = get_dataset_view(self.table, self.state, self.principal("viewer"), "shop")

        self.assertEqual("Orders and payments", shop["description"])
        self.assertEqual(
            [("orders", "active", "mat-1", "Shop orders"), ("payments", "pending", "mat-1", "Shop orders")],
            [(table["name"], table["state"], table["materialization_id"], table["materialization_name"]) for table in shop["tables"]],
        )
        with self.assertRaises(WebError) as caught:
            get_dataset_view(self.table, self.state, self.principal("viewer"), "nothing")
        self.assertEqual(404, caught.exception.status_code)

    def test_a_table_resolves_by_its_sql_name_with_where_it_is_for_readers_only(self) -> None:
        from materializer_control_plane.web.datasets import get_dataset_table_view

        readable = get_dataset_table_view(self.table, self.state, self.principal("viewer"), "shop", "orders")
        self.assertEqual(("shop.orders", "mat-1", "Shop orders", True), (readable["qualified_name"], readable["materialization_id"], readable["materialization_name"], readable["readable"]))
        self.assertTrue(readable["location"].endswith("/materializer/mat-1/orders"))
        store_user(self.table, make_user("outsider", "outsider@example.com"))
        locked = get_dataset_table_view(self.table, self.state, self.principal("outsider"), "shop", "orders")
        self.assertFalse(locked["readable"])
        self.assertNotIn("location", locked)
        with self.assertRaises(WebError) as caught:
            get_dataset_table_view(self.table, self.state, self.principal("viewer"), "shop", "customers")
        self.assertEqual(404, caught.exception.status_code)

    def test_creating_a_dataset_is_validated_and_previewed(self) -> None:
        from materializer_control_plane.web.datasets import create_dataset_request

        manager = self.principal("manager")
        accepted = create_dataset_request(self.table, manager, {"name": "sales", "description": " All of it "}, request=http_request())
        self.assertEqual(("create_dataset", "dataset", "sales"), (accepted.response["action"], accepted.response["entity_type"], accepted.response["name"]))
        self.assertTrue(accepted.response["entity_id"].startswith("ds-"))
        self.assertEqual("All of it", accepted.request_event.payload["description"])
        for body, status in (({"name": "Sales"}, 400), ({"name": "shop"}, 409), ({"name": "x", "color": "red"}, 400), ({"description": "no name"}, 400)):
            with self.subTest(body=body), self.assertRaises(WebError) as caught:
                create_dataset_request(self.table, manager, body, request=http_request())
            self.assertEqual(status, caught.exception.status_code)

    def test_only_an_owner_changes_or_deletes_a_dataset_and_only_once_it_is_empty(self) -> None:
        from materializer_control_plane.web.datasets import delete_dataset_request, update_dataset_request

        admin = self.principal("admin")
        accepted = update_dataset_request(self.table, admin, "shop", {"description": "Sales", "base_revision": 1}, request=http_request())
        self.assertEqual({"dataset_id": SHOP, "description": "Sales"}, accepted.request_event.payload)
        with self.assertRaises(WebError) as caught:
            update_dataset_request(self.table, admin, "shop", {"name": "sales"}, request=http_request())
        self.assertEqual(400, caught.exception.status_code)
        with self.assertRaises(WebError) as caught:
            delete_dataset_request(self.table, self.state, admin, "shop", request=http_request())
        self.assertEqual((409, "dataset_not_empty"), (caught.exception.status_code, caught.exception.details["reason"]))
        self.assertEqual(["orders", "payments"], caught.exception.details["tables"])
        # A table being dropped holds a dataset too, though it freed its name.
        dropping = build_table_record(build_materialization("mat-2", table_spec("ledger", "ledger", dataset="finance", dataset_id=FINANCE)), state="dropping")
        save_materialization_table(self.state, dropping)
        with self.assertRaises(WebError) as caught:
            delete_dataset_request(self.table, self.state, admin, "finance", request=http_request())
        self.assertEqual((409, "dataset_not_empty"), (caught.exception.status_code, caught.exception.details["reason"]))

    def test_owners_see_and_change_who_may_use_a_dataset(self) -> None:
        from materializer_control_plane.web.datasets import list_dataset_grants_view, set_dataset_grant_request

        admin = self.principal("admin")
        listed = list_dataset_grants_view(self.table, admin, "shop")
        self.assertEqual(
            [("group", "editor"), ("group", "viewer"), ("user", "admin")],
            [(grant["principal_type"], grant["principal_id"]) for grant in listed["grants"]],
        )
        self.assertEqual({"dataset-reader", "dataset-writer", "dataset-owner"}, {role["role_id"] for role in listed["roles"]})
        self.assertTrue(all(role["grantable"] for role in listed["roles"]))
        # Whom an owner can share it with, whether or not they may browse people and groups.
        self.assertIn({"user_id": "manager", "name": "Manager Person", "email": "manager@example.com"}, listed["candidates"]["users"])
        self.assertNotIn("guest", [group["group_id"] for group in listed["candidates"]["groups"]])
        self.assertIn("all-users", [group["group_id"] for group in listed["candidates"]["groups"]])

        accepted = set_dataset_grant_request(self.table, admin, "shop", "manager", {"role_ids": ["dataset-writer"]}, request=http_request())
        self.assertEqual(
            {"dataset_id": SHOP, "principal_type": "user", "principal_id": "manager", "role_ids": ["dataset-writer"]},
            accepted.request_event.payload,
        )
        group = set_dataset_grant_request(self.table, admin, "shop", "grp-managers", {"role_ids": []}, request=http_request())
        self.assertEqual("group", group.request_event.payload["principal_type"])
        for principal_id, body, status in (
            ("usr-nobody", {"role_ids": ["dataset-reader"]}, 404),
            ("manager", {"role_ids": ["operator"]}, 400),
            ("guest", {"role_ids": ["dataset-reader"]}, 400),
            ("manager", {"role_ids": "dataset-reader"}, 400),
        ):
            with self.subTest(principal=principal_id, body=body), self.assertRaises(WebError) as caught:
                set_dataset_grant_request(self.table, admin, "shop", principal_id, body, request=http_request())
            self.assertEqual(status, caught.exception.status_code)

    def test_the_views_authorize_on_the_dataset_they_name(self) -> None:
        with self.assertRaises(WebError) as caught:
            authorize(http_request(session_token(get_user(self.table, "viewer"))), self.table, "dataset.manage", dataset="shop", secrets=SECRETS, now=NOW_EPOCH)
        self.assertEqual(403, caught.exception.status_code)
        self.assertEqual("admin", authorize(http_request(session_token(get_user(self.table, "admin"))), self.table, "dataset.manage", dataset="shop", secrets=SECRETS, now=NOW_EPOCH).user_id)



def setUpModule() -> None:
    # A deployment always names the namespace its runtimes run in (handbook 02 §2.3); these tests
    # reach the configuration without choosing one, so the environment names it for them.
    os.environ.setdefault("MATERIALIZER_CONTROL_PLANE_DEFAULT_NAMESPACE", "midstream")
    control_plane_config.get_config.cache_clear()


if __name__ == "__main__":
    unittest.main()
