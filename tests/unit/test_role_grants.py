# Copyright (c) 2026 Kenneth Stott
# Canary: decf0aaf-9fcc-4a2e-b7c7-bbce3d25cd1b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Time-limited role grants and the removal of unused ones (REQ-2077).

A grant held by Provisa with an expiry is absent from the held roles the moment it expires; one
of a role with ``inactivity_days`` goes once unused that long. A request uses the roles it
depended on, recorded once a day. The sweep deletes expired and unused grants into the org's
trail and finds the ones to give notice of; the one expiry run tells of them
(test_entitlement_expiry_run.py). A grant given again keeps the latest of its expiries, and a rule
ending the whole grant sets or shortens the grant's own expiry (REQ-2077, amended 2026-11-11)."""

# Requirements: REQ-2077

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from sqlalchemy import insert, select, update

from provisa.auth import role_grants
from provisa.auth.middleware import AuthMiddleware
from provisa.auth.models import AuthIdentity
from provisa.core.schema_org import admin_audit_log, roles, user_role_assignments

pytestmark = pytest.mark.unit

_t = user_role_assignments
_NOW = datetime.now(timezone.utc)


@pytest.fixture
async def org(control_plane):
    db = control_plane.org()
    async with db.acquire() as conn:
        for role_id, days in (("reader", None), ("checker", None), ("contractor", 30)):
            await conn.execute_core(
                insert(roles).values(
                    id=role_id, capabilities=[], domain_access=["*"], inactivity_days=days
                )
            )
    return db


async def _grant(db, user: str, role: str, **cols) -> None:
    async with db.acquire() as conn:
        await conn.execute_core(
            insert(_t).values(user_id=user, role_id=role, domain_id="*", **cols)
        )


async def _grants(db) -> dict[tuple[str, str], dict]:
    async with db.acquire() as conn:
        rows = (await conn.execute_core(select(_t))).fetchall()
    return {(r.user_id, r.role_id): dict(r._mapping) for r in rows}


async def _trail(db) -> list[tuple[str, str, str]]:
    async with db.acquire() as conn:
        rows = (await conn.execute_core(select(admin_audit_log))).fetchall()
    return sorted((r.action, r.subject_id, r.detail["role_id"]) for r in rows)


def _identity(user: str) -> AuthIdentity:
    return AuthIdentity(user_id=user, email=None, display_name=None, roles=[], raw_claims={})


async def test_an_expired_grant_is_not_held_from_the_instant_it_expires(org):
    await _grant(org, "ada", "reader", expires_at=_NOW - timedelta(seconds=1))
    await _grant(org, "ada", "checker", expires_at=_NOW + timedelta(days=1))
    await _grant(org, "ada", "contractor")
    reader = SimpleNamespace(_db_pool=org, _default_assignments=[])
    held = await AuthMiddleware._read_assignments(reader, _identity("ada"))  # type: ignore[arg-type]
    assert sorted(a.role_id for a in held) == ["checker", "contractor"]


async def test_an_expired_org_admin_grant_is_no_admin(org):
    from provisa.core.org_membership import org_admin_user_ids

    await _grant(org, "ada", "org_admin", expires_at=_NOW - timedelta(minutes=1))
    await _grant(org, "bo", "org_admin")
    assert await org_admin_user_ids(org) == {"bo"}


def _state(members: dict[str, set[int]]) -> SimpleNamespace:
    surfaces = {
        m: SimpleNamespace(
            context=SimpleNamespace(tables={str(t): SimpleNamespace(table_id=t) for t in tables})
        )
        for m, tables in members.items()
    }
    return SimpleNamespace(surfaces=surfaces, meta_roles={"meta:all": tuple(members)})


def test_a_single_acting_role_that_served_the_request_is_used():
    assert role_grants.used_roles("reader", [], _state({})) == ("reader",)


def test_acting_as_all_uses_only_the_roles_granted_what_the_statement_read():
    state = _state({"reader": {1, 2}, "checker": {3}, "contractor": {2}})
    assert role_grants.used_roles("meta:all", [2], state) == ("reader", "contractor")
    assert role_grants.used_roles("meta:all", [], state) == ()


async def test_a_grants_use_is_recorded_at_most_once_a_day(org):
    yesterday = _NOW - timedelta(days=1)
    await _grant(org, "ada", "reader", last_used_at=yesterday)
    await _grant(org, "ada", "checker")
    await _grant(org, "ada", "contractor", expires_at=_NOW - timedelta(minutes=1))
    await role_grants.mark_used(org, "ada", ["reader", "checker", "contractor"])
    first = await _grants(org)
    assert first["ada", "reader"]["last_used_at"] > yesterday
    assert first["ada", "checker"]["last_used_at"] is not None
    assert first["ada", "contractor"]["last_used_at"] is None  # no longer held
    await role_grants.mark_used(org, "ada", ["reader", "checker"])
    again = await _grants(org)
    assert again["ada", "reader"]["last_used_at"] == first["ada", "reader"]["last_used_at"]
    assert again["ada", "checker"]["last_used_at"] == first["ada", "checker"]["last_used_at"]


async def test_the_sweep_removes_expired_and_unused_grants_into_the_trail(org):
    await _grant(org, "ada", "reader", expires_at=_NOW - timedelta(minutes=1))
    await _grant(org, "ada", "checker", expires_at=_NOW + timedelta(days=30))
    long_ago = _NOW - timedelta(days=60)
    await _grant(
        org, "bo", "contractor", created_at=long_ago, last_used_at=_NOW - timedelta(days=31)
    )
    await _grant(org, "cy", "contractor", created_at=_NOW - timedelta(days=31))
    await _grant(
        org, "di", "contractor", created_at=long_ago, last_used_at=_NOW - timedelta(days=2)
    )
    await _grant(org, "ed", "reader", created_at=_NOW - timedelta(days=400))
    outcome = await role_grants.sweep(org, notify=False)
    assert [(g.user_id, g.role_id) for g in outcome.expired] == [("ada", "reader")]
    assert sorted(g.user_id for g in outcome.inactive) == ["bo", "cy"]
    assert sorted(await _grants(org)) == [
        ("ada", "checker"),
        ("di", "contractor"),
        ("ed", "reader"),
    ]
    assert await _trail(org) == [
        ("role.expired", "ada", "reader"),
        ("role.removed_inactive", "bo", "contractor"),
        ("role.removed_inactive", "cy", "contractor"),
    ]
    assert outcome.expiring == [] and outcome.going_unused == []


async def test_notice_is_given_once_before_expiry_and_again_after_each_use(org, monkeypatch):
    monkeypatch.setattr(role_grants, "notice_window", lambda: timedelta(days=7))
    await _grant(org, "ada", "checker", expires_at=_NOW + timedelta(days=3))
    await _grant(org, "ada", "reader", expires_at=_NOW + timedelta(days=30))
    long_ago = _NOW - timedelta(days=60)
    await _grant(
        org, "bo", "contractor", created_at=long_ago, last_used_at=_NOW - timedelta(days=25)
    )
    await _grant(
        org, "cy", "contractor", created_at=long_ago, last_used_at=_NOW - timedelta(days=2)
    )
    first = await role_grants.sweep(org, notify=True)
    assert [(g.user_id, g.role_id) for g in first.expiring] == [("ada", "checker")]
    assert [g.user_id for g in first.going_unused] == ["bo"]
    second = await role_grants.sweep(org, notify=True)
    assert second.expiring == [] and second.going_unused == []
    async with org.acquire() as conn:  # bo uses the grant again, and later falls idle again
        await conn.execute_core(
            update(_t)
            .where(_t.c.user_id == "bo")
            .values(
                last_used_at=_NOW - timedelta(days=24),
                inactive_notified_at=_NOW - timedelta(days=25),
            )
        )
    third = await role_grants.sweep(org, notify=True)
    assert [g.user_id for g in third.going_unused] == ["bo"]


async def _regrant(db, user: str, role: str, expires_at) -> dict:
    async with db.acquire() as conn:
        await role_grants.regrant(conn, user, role, "*", expires_at)
    return (await _grants(db))[user, role]


async def test_a_grant_given_again_keeps_the_latest_of_its_expiries(org):
    first = _NOW + timedelta(days=10)
    assert (await _regrant(org, "ada", "reader", first))["expires_at"] == first
    assert (await _regrant(org, "ada", "reader", _NOW + timedelta(days=2)))["expires_at"] == first
    later = _NOW + timedelta(days=20)
    assert (await _regrant(org, "ada", "reader", later))["expires_at"] == later


async def test_no_expiry_is_later_than_any_date(org):
    await _regrant(org, "ada", "reader", _NOW + timedelta(days=10))
    assert (await _regrant(org, "ada", "reader", None))["expires_at"] is None
    assert (await _regrant(org, "ada", "reader", _NOW + timedelta(days=99)))["expires_at"] is None


async def test_a_grant_given_again_is_told_again_only_of_an_end_it_moved(org):
    await _grant(
        org,
        "ada",
        "reader",
        expires_at=_NOW + timedelta(days=3),
        expiry_notified_at=_NOW - timedelta(hours=1),
    )
    sooner = await _regrant(org, "ada", "reader", _NOW + timedelta(days=1))
    assert sooner["expiry_notified_at"] is not None
    moved = await _regrant(org, "ada", "reader", _NOW + timedelta(days=30))
    assert moved["expiry_notified_at"] is None


async def test_a_grant_wide_rule_sets_or_shortens_the_grants_own_expiry(org):
    await _grant(org, "ada", "reader")
    ends = _NOW + timedelta(days=10)
    async with org.acquire() as conn:
        assert await role_grants.end_grant(conn, "ada", "reader", ends) == 1
        assert (await _grants(org))["ada", "reader"]["expires_at"] == ends
        await role_grants.end_grant(conn, "ada", "reader", _NOW + timedelta(days=20))
        assert (await _grants(org))["ada", "reader"]["expires_at"] == ends  # never extends
        sooner = _NOW + timedelta(days=5)
        await role_grants.end_grant(conn, "ada", "reader", sooner)
        assert (await _grants(org))["ada", "reader"]["expires_at"] == sooner


async def test_a_grant_wide_rule_on_a_role_provisa_does_not_grant_ends_nothing(org):
    await _grant(org, "ada", "reader")
    async with org.acquire() as conn:
        assert await role_grants.end_grant(conn, "ada", "checker", _NOW + timedelta(days=1)) == 0
        assert await role_grants.end_grant(conn, "bo", "reader", _NOW + timedelta(days=1)) == 0


async def test_a_grant_ended_by_a_grant_wide_rule_is_not_held_from_that_instant(org):
    await _grant(org, "ada", "reader")
    await _grant(org, "ada", "checker")
    async with org.acquire() as conn:
        await role_grants.end_grant(conn, "ada", "reader", _NOW - timedelta(seconds=1))
        assert await role_grants.grant_ended(conn, "ada", "reader")
        assert not await role_grants.grant_ended(conn, "ada", "checker")
        assert [g.role_id for g in await role_grants.held_grants(conn, "ada")] == ["checker"]
    reader = SimpleNamespace(_db_pool=org, _default_assignments=[])
    held = await AuthMiddleware._read_assignments(reader, _identity("ada"))  # type: ignore[arg-type]
    assert [a.role_id for a in held] == ["checker"]


def test_a_role_ends_at_the_latest_of_its_grants():
    soon, late = _NOW + timedelta(days=1), _NOW + timedelta(days=9)
    grants = [
        role_grants.HeldGrant("reader", "sales", soon),
        role_grants.HeldGrant("reader", "finance", late),
        role_grants.HeldGrant("checker", "*", soon),
        role_grants.HeldGrant("checker", "sales", None),
    ]
    assert role_grants.grant_end(grants, "reader") == (True, late)
    assert role_grants.grant_end(grants, "checker") == (True, None)
    assert role_grants.grant_end(grants, "contractor") == (False, None)


async def test_a_roles_inactivity_days_is_kept_and_served(org):
    from provisa.api.admin._row_mappers import _role_from_row
    from provisa.core.models import Role
    from provisa.core.repositories import role as role_repo

    async with org.acquire() as conn:
        await role_repo.upsert(
            conn,
            Role(
                id="temp",
                description="Short engagements.",
                capabilities=[],
                domain_access=["*"],
                inactivity_days=14,
            ),
            org_id="acme",
        )
        row = await role_repo.get(conn, "temp")
    assert row is not None and row["inactivity_days"] == 14
    assert (
        _role_from_row(
            row, detail=True, effective={"capabilities": [], "domains": [], "regions": []}
        ).inactivity_days
        == 14
    )
    with pytest.raises(ValueError):
        Role(id="t", description="d", capabilities=[], domain_access=["*"], inactivity_days=0)
