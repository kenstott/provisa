# Copyright (c) 2026 Kenneth Stott
# Canary: 4e8a1c6d-3f57-4b92-a0e3-7d5c9b2f1a86
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Access reviews (REQ-2266): on demand and on a schedule, each member's grant with its
entitlements and last use, grouped by the domain whose owner confirms or revokes it; every
decision recorded with who, when and why; a revocation takes effect at once; an export."""

# Requirements: REQ-2266

from __future__ import annotations

import csv
import io
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from sqlalchemy import insert, select

from provisa.api.admin import access_review_router as router
from provisa.api.errors import ApiError
from provisa.auth.middleware import AuthMiddleware
from provisa.auth.models import AuthIdentity
from provisa.core.schema_org import (
    access_review_rows,
    admin_audit_log,
    domains,
    roles,
    user_directory,
    user_role_assignments,
)
from provisa.entitlements import access_review as ar
from provisa.entitlements.lists import HeldEntitlements, HeldList
from provisa.entitlements.runtime import EntitlementRequest, bound_request_scope, reaches_domain

pytestmark = pytest.mark.unit

_NOW = datetime.now(UTC)


@pytest.fixture
async def org(control_plane, monkeypatch):
    db = control_plane.org()
    async with db.acquire() as conn:
        for role_id in ("reader", "clerk"):
            await conn.execute_core(
                insert(roles).values(
                    id=role_id, description=role_id, capabilities=[], domain_access=["*"]
                )
            )
        await conn.execute_core(insert(domains).values(id="sales", description="Sales"))
        await conn.execute_core(
            insert(domains).values(id="sales_emea", description="EMEA", parent_domain_id="sales")
        )
        await conn.execute_core(insert(domains).values(id="hr", description="HR"))
        await conn.execute_core(
            insert(user_directory).values(user_id="bo", display_name="Bo", email="bo@x.test")
        )
    monkeypatch.setattr(
        "provisa.api.app.state",
        SimpleNamespace(model_db=db, held_entitlements={}),
        raising=False,
    )
    return db


async def _grant(db, user: str, role: str, domain: str, **cols) -> None:
    async with db.acquire() as conn:
        await conn.execute_core(
            insert(user_role_assignments).values(
                user_id=user, role_id=role, domain_id=domain, **cols
            )
        )


async def _start(db, held=None) -> int:
    async with db.acquire() as conn:
        return await ar.start(conn, "ana", held or {})


async def _rows(db, review_id: int) -> dict[tuple[str, str, str], dict]:
    async with db.acquire() as conn:
        rows = await ar.rows_of(conn, review_id)
    return {(r["user_id"], r["role_id"], r["domain_id"]): r for r in rows}


async def test_a_review_lists_each_grant_in_force_with_its_entitlements_and_last_use(org):
    used = _NOW - timedelta(days=3)
    await _grant(org, "bo", "reader", "sales", last_used_at=used)
    await _grant(org, "cy", "clerk", "hr")
    await _grant(org, "di", "reader", "hr", expires_at=_NOW - timedelta(minutes=1))
    held = {
        "bo": HeldEntitlements(
            lists=(
                HeldList("reader", "tables", ("sales.orders",)),
                HeldList("clerk", "tables", ("hr.people",)),
            )
        )
    }
    rows = await _rows(org, await _start(org, held))
    assert set(rows) == {("bo", "reader", "sales"), ("cy", "clerk", "hr")}
    bo = rows[("bo", "reader", "sales")]
    assert bo["entitlements"] == [
        {"kind": "tables", "capability": None, "identifiers": ["sales.orders"]}
    ]
    assert bo["last_used_at"] == used
    assert (bo["display_name"], bo["email"]) == ("Bo", "bo@x.test")
    assert bo["decision"] is None


async def test_a_confirmation_records_who_when_and_why_and_is_final(org):
    await _grant(org, "bo", "reader", "sales")
    review_id = await _start(org)
    row_id = (await _rows(org, review_id))[("bo", "reader", "sales")]["id"]
    async with org.acquire() as conn:
        decided = await ar.decide(conn, row_id, ar.CONFIRMED, "ana", "  still on the team ")
        assert (decided["decision"], decided["decided_by"], decided["reason"]) == (
            "confirmed",
            "ana",
            "still on the team",
        )
        assert decided["decided_at"] is not None
        assert decided["revoked_grant"] is False
        with pytest.raises(ar.ReviewRefused) as again:
            await ar.decide(conn, row_id, ar.REVOKED, "ana", "changed my mind")
    assert again.value.code == "access_review.already_decided"
    async with org.acquire() as conn:
        held = (await conn.execute_core(select(user_role_assignments))).fetchall()
    assert [(g.user_id, g.role_id) for g in held] == [("bo", "reader")]


async def test_a_decision_needs_a_reason_and_a_known_decision(org):
    await _grant(org, "bo", "reader", "sales")
    row_id = (await _rows(org, await _start(org)))[("bo", "reader", "sales")]["id"]
    async with org.acquire() as conn:
        with pytest.raises(ar.ReviewRefused) as blank:
            await ar.decide(conn, row_id, ar.CONFIRMED, "ana", "   ")
        with pytest.raises(ar.ReviewRefused) as unknown:
            await ar.decide(conn, row_id, "maybe", "ana", "why not")
        with pytest.raises(ar.ReviewRefused) as missing:
            await ar.decide(conn, row_id + 999, ar.CONFIRMED, "ana", "why")
    assert blank.value.code == "access_review.reason_required"
    assert unknown.value.code == "access_review.decision_invalid"
    assert missing.value.code == "access_review.row_not_found"


async def test_a_revocation_removes_the_grant_at_once(org):
    await _grant(org, "bo", "reader", "sales")
    await _grant(org, "bo", "clerk", "hr")
    row_id = (await _rows(org, await _start(org)))[("bo", "reader", "sales")]["id"]
    async with org.acquire() as conn:
        decided = await ar.decide(conn, row_id, ar.REVOKED, "ana", "left the sales team")
    assert decided["revoked_grant"] is True
    identity = AuthIdentity(user_id="bo", email=None, display_name=None, roles=[], raw_claims={})
    reader = SimpleNamespace(_db_pool=org, _default_assignments=[])
    held = await AuthMiddleware._read_assignments(reader, identity)  # type: ignore[arg-type]
    assert [a.role_id for a in held] == ["clerk"]


async def test_the_export_carries_every_row_and_its_decision(org):
    await _grant(org, "bo", "reader", "sales")
    await _grant(org, "cy", "clerk", "hr")
    review_id = await _start(org)
    rows = await _rows(org, review_id)
    async with org.acquire() as conn:
        await ar.decide(conn, rows[("bo", "reader", "sales")]["id"], ar.REVOKED, "ana", "left")
        exported = ar.export_csv(await ar.rows_of(conn, review_id))
    lines = list(csv.DictReader(io.StringIO(exported)))
    assert [(r["domain_id"], r["user_id"], r["decision"], r["reason"]) for r in lines] == [
        ("hr", "cy", "", ""),
        ("sales", "bo", "revoked", "left"),
    ]
    assert lines[1]["decided_by"] == "ana"


def test_a_schedule_is_due_once_a_cron_time_has_passed_since_the_last_review():
    cron = "0 6 23 * *"  # the 23rd of each month, 06:00 UTC
    the_23rd = datetime(2026, 11, 23, 6, 5, tzinfo=UTC)
    assert ar.due(cron, datetime(2026, 10, 23, 6, 1, tzinfo=UTC), the_23rd)
    assert not ar.due(cron, datetime(2026, 11, 23, 6, 1, tzinfo=UTC), the_23rd)
    # With no scheduled review yet, only a cron time within the last check interval is due.
    assert ar.due(cron, None, the_23rd)
    assert not ar.due(cron, None, the_23rd + timedelta(hours=1))


def test_a_schedule_must_be_a_five_field_cron():
    assert ar.check_cron(" 0 6 * * 1 ") == "0 6 * * 1"
    with pytest.raises(ar.ReviewRefused) as bad:
        ar.check_cron("every monday")
    assert bad.value.code == "access_review.cron_invalid"


async def test_the_scheduled_review_starts_when_due_and_not_twice(org):
    await _grant(org, "bo", "reader", "sales")
    overrides = {ar.SETTINGS_KEY: {ar.CRON_FIELD: "*/5 * * * *"}}
    now = datetime.now(UTC)
    assert await ar.run_scheduled(org, {}, now) is None
    first = await ar.run_scheduled(org, overrides, now)
    assert first is not None
    assert await ar.run_scheduled(org, overrides, now) is None
    async with org.acquire() as conn:
        listed = await ar.reviews(conn)
    assert [(r.id, r.started_by, r.rows, r.decided) for r in listed] == [(first, "schedule", 1, 0)]


async def test_a_domain_is_spelled_with_its_ancestors_and_the_whole_org_as_star(org):
    async with org.acquire() as conn:
        spellings = await ar.domain_spellings(conn)
    assert "sales" in spellings["sales_emea"]
    assert "sales_emea" in spellings["sales_emea"]
    assert "sales" not in spellings["hr"]
    assert spellings["*"] == ("*",)


def _steward_state(identifiers: tuple[str, ...]) -> SimpleNamespace:
    role = {"capabilities": ["domain_steward"], "domain_access": ["*"]}
    held = HeldEntitlements(
        lists=(HeldList("steward", "domains", identifiers, capability="domain_steward"),)
    )
    return SimpleNamespace(roles={"steward": role}, held_entitlements={"ana": held})


def test_a_domains_owner_is_whom_domain_steward_reaches_it_for():
    state = _steward_state(("sales",))
    with bound_request_scope(EntitlementRequest("ana", {}, "", ("steward",))):
        assert reaches_domain(state, ("sales",), {"domain_steward"})
        assert reaches_domain(state, ("sales_emea", "sales"), {"domain_steward"})
        assert not reaches_domain(state, ("hr",), {"domain_steward"})
        # A grant over every domain is reviewed only by an owner of every domain.
        assert not reaches_domain(state, ("*",), {"domain_steward"})
    with bound_request_scope(EntitlementRequest("ana", {}, "", ("steward",))):
        assert reaches_domain(_steward_state(("*",)), ("*",), {"domain_steward"})


def _request(user_id: str) -> SimpleNamespace:
    return SimpleNamespace(
        state=SimpleNamespace(identity=SimpleNamespace(user_id=user_id), active_org_id="default")
    )


@pytest.fixture
def gates(monkeypatch):
    """The router's capability gates open; who owns which domain is set per test."""
    monkeypatch.setattr(router, "require_capability_request", lambda _r, _c: None)
    monkeypatch.setattr(router, "has_capability_request", lambda _r, _c: False)
    monkeypatch.setattr(router, "require_roles_removable", lambda _r, _ids: None)

    async def _no_effective(_conn, _state, _user):
        return None

    monkeypatch.setattr(
        "provisa.entitlements.effective_store.write_person", _no_effective, raising=True
    )
    owned: set[str] = set()
    monkeypatch.setattr(
        "provisa.entitlements.runtime.reaches_domain",
        lambda _s, spellings, _c: any(s in owned for s in spellings),
    )
    return owned


async def test_only_the_domains_owner_decides_and_never_on_their_own_access(org, gates):
    await _grant(org, "bo", "reader", "sales")
    await _grant(org, "ana", "reader", "sales")
    await _grant(org, "cy", "clerk", "hr")
    review_id = await _start(org)
    rows = await _rows(org, review_id)
    gates.add("sales")

    # An owner reads the rows of the domains they own, and may decide all but their own.
    seen = (await router.get_review(review_id, _request("ana")))["rows"]  # type: ignore[arg-type]
    assert {(r["user_id"], r["can_decide"]) for r in seen} == {("bo", True), ("ana", False)}

    body = router.DecisionBody(decision="revoked", reason="left")
    with pytest.raises(ApiError) as not_owner:
        await router.decide_row(rows[("cy", "clerk", "hr")]["id"], body, _request("ana"))  # type: ignore[arg-type]
    assert not_owner.value.code == "access_review.not_owner"
    with pytest.raises(ApiError) as own:
        await router.decide_row(rows[("ana", "reader", "sales")]["id"], body, _request("ana"))  # type: ignore[arg-type]
    assert own.value.code == "access_review.own_access"

    decided = await router.decide_row(rows[("bo", "reader", "sales")]["id"], body, _request("ana"))  # type: ignore[arg-type]
    assert (decided["decision"], decided["decided_by"], decided["revoked_grant"]) == (
        "revoked",
        "ana",
        True,
    )
    async with org.acquire() as conn:
        trail = (await conn.execute_core(select(admin_audit_log))).fetchall()
        left = (await conn.execute_core(select(access_review_rows.c.decision))).fetchall()
    assert [(t.action, t.actor_id, t.subject_id, t.detail["reason"]) for t in trail] == [
        ("access_review.revoked", "ana", "bo", "left")
    ]
    assert sorted(r.decision or "" for r in left) == ["", "", "revoked"]
