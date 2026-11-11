# Copyright (c) 2026 Kenneth Stott
# Canary: 0b6a2f4e-91c3-4d7a-8e15-c2d9f3a07b46
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2219, EXPIRY RULES END EVERY CAPABILITY THEY NAME: the scheduled entitlement expiry run.
Ahead of each expiry rule, by the expiring-soon event's own setting, it emits an
entitlements.expiring_soon event; once the date has passed, an entitlements.expired event. Each is
an entry in the org's administrative trail, once per rule and date, and where mail is configured
the person and the org's admins are told."""

# Requirements: REQ-2219

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from sqlalchemy import insert, select

from provisa.core import settings_catalog
from provisa.core.schema_org import admin_audit_log, user_directory, user_role_assignments
from provisa.entitlements import expiry_run, store
from provisa.entitlements.lists import ExpiryRule, HeldEntitlements

pytestmark = pytest.mark.unit

_NOW = datetime.now(UTC)


async def _rule(db, principal, ends_at, ends=None, role="viewer") -> str:
    async with db.acquire() as conn:
        return await store.add_expiry(
            conn,
            principal,
            role,
            ends_at,
            {"read": ("finance",)} if ends is None else ends,
            set_by="admin",
            reason="contract ends",
        )


async def _trail(db) -> list[tuple[str, str, str]]:
    async with db.acquire() as conn:
        rows = (await conn.execute_core(select(admin_audit_log))).fetchall()
    return sorted((r.action, r.subject_id, r.detail["rule_key"]) for r in rows)


@pytest.fixture
def threshold(monkeypatch):
    days = {"value": 7}
    monkeypatch.setattr(expiry_run, "expiring_soon_days", lambda: days["value"])
    return days


def test_the_threshold_is_a_declared_setting_of_its_own():
    [setting] = [s for s in settings_catalog.DECLARED if s.key == expiry_run.SETTING]
    assert (setting.type, setting.unit, setting.req) == ("int", "days", "REQ-2219")
    assert setting.default == 7 and setting.min == 1


def test_a_rule_is_due_ahead_of_its_date_by_the_threshold_and_once_its_date_has_passed():
    soon = ExpiryRule("viewer", _NOW + timedelta(days=3), rule_key="soon")
    later = ExpiryRule("viewer", _NOW + timedelta(days=30), rule_key="later")
    passed = ExpiryRule("viewer", _NOW - timedelta(minutes=1), rule_key="passed")
    held = {"alice": HeldEntitlements(expiries=(soon, later, passed))}
    run = expiry_run.due(held, set(), _NOW, timedelta(days=7))
    assert [d.rule.rule_key for d in run.expiring] == ["soon"]
    assert [d.rule.rule_key for d in run.expired] == ["passed"]


def test_an_event_already_emitted_for_a_rule_and_date_is_not_due_again():
    soon = ExpiryRule("viewer", _NOW + timedelta(days=3), rule_key="soon")
    held = {"alice": HeldEntitlements(expiries=(soon,))}
    emitted = {(expiry_run.EXPIRING_SOON, "soon", soon.ends_at.isoformat())}
    assert expiry_run.due(held, emitted, _NOW, timedelta(days=7)).expiring == []
    moved = {(expiry_run.EXPIRING_SOON, "soon", (soon.ends_at - timedelta(days=1)).isoformat())}
    assert len(expiry_run.due(held, moved, _NOW, timedelta(days=7)).expiring) == 1


async def test_the_run_emits_each_event_into_the_trail_once(control_plane, threshold):
    db = control_plane.org()
    soon = await _rule(db, "alice", _NOW + timedelta(days=3))
    passed = await _rule(db, "bob", _NOW - timedelta(hours=1))
    await _rule(db, "carol", _NOW + timedelta(days=30))
    first = await expiry_run.run(db, _NOW)
    assert (len(first.expiring), len(first.expired)) == (1, 1)
    assert await _trail(db) == sorted(
        [(expiry_run.EXPIRING_SOON, "alice", soon), (expiry_run.EXPIRED, "bob", passed)]
    )
    again = await expiry_run.run(db, _NOW)
    assert (again.expiring, again.expired) == ([], [])
    assert len(await _trail(db)) == 2


async def test_the_events_own_setting_is_the_threshold(control_plane, threshold):
    db = control_plane.org()
    await _rule(db, "alice", _NOW + timedelta(days=10))
    assert (await expiry_run.run(db, _NOW)).expiring == []
    threshold["value"] = 14
    assert len((await expiry_run.run(db, _NOW)).expiring) == 1


async def test_a_rule_that_passes_after_its_expiring_soon_event_is_then_expired(
    control_plane, threshold
):
    db = control_plane.org()
    key = await _rule(db, "alice", _NOW + timedelta(days=1))
    await expiry_run.run(db, _NOW)
    await expiry_run.run(db, _NOW + timedelta(days=2))
    assert await _trail(db) == sorted(
        [(expiry_run.EXPIRING_SOON, "alice", key), (expiry_run.EXPIRED, "alice", key)]
    )


async def test_a_withdrawn_rule_emits_nothing(control_plane, threshold):
    db = control_plane.org()
    await _rule(db, "alice", _NOW + timedelta(days=1))
    async with db.acquire() as conn:
        [rule] = (await store.load_held(conn))["alice"].expiries
        await store.withdraw_expiry(conn, rule, "alice", set_by="admin", reason="renewed")
    run = await expiry_run.run(db, _NOW + timedelta(days=2))
    assert (run.expiring, run.expired) == ([], [])


async def test_the_person_and_the_admins_are_told_and_the_admins_of_an_end(
    control_plane, monkeypatch
):
    db = control_plane.org()
    sent: list[tuple[str, str, str]] = []

    async def _send(org_id, to, subject, body, *, kind):
        sent.append((to, subject, kind))

    monkeypatch.setattr("provisa.auth.role_grants.send_notice", _send)
    async with db.acquire() as conn:
        await conn.execute_core(
            insert(user_role_assignments).values(user_id="boss", role_id="org_admin", domain_id="*")
        )
        await conn.execute_core(
            insert(user_directory).values(
                [
                    {"user_id": "boss", "email": "boss@example.com"},
                    {"user_id": "ada", "email": "ada@example.com"},
                ]
            )
        )
    soon = expiry_run.Due("ada", ExpiryRule("viewer", _NOW + timedelta(days=3), {}))
    gone = expiry_run.Due("ada", ExpiryRule("analyst", _NOW, {"read": ("finance",)}))
    await expiry_run._tell(db, "acme", expiry_run.Run([soon], [gone]))
    when = expiry_run._when(soon.rule.ends_at)
    assert sorted(sent) == sorted(
        [
            ("ada@example.com", f"Your access in acme ends {when}", "entitlement_expiry"),
            ("boss@example.com", f"ada's access in acme ends {when}", "entitlement_expiry"),
            ("boss@example.com", "ada's access in acme ended", "entitlement_expiry"),
        ]
    )


@pytest.mark.parametrize(("provider", "told"), [("smtp", True), ("", False)])
async def test_notices_go_wherever_mail_is_configured(monkeypatch, provider, told):
    from provisa.api import app as app_module

    calls: list[str] = []

    async def _run(db, now):
        calls.append("run")
        return expiry_run.Run([], [])

    async def _tell(db, org_id, outcome):
        calls.append("told")

    monkeypatch.setattr(expiry_run, "run", _run)
    monkeypatch.setattr(expiry_run, "_tell", _tell)
    state = app_module.state
    monkeypatch.setattr(type(state), "model_db", property(lambda s: object()))
    config = SimpleNamespace(multitenancy=False, mail=SimpleNamespace(provider=provider))
    monkeypatch.setattr(state, "config", config, raising=False)
    await expiry_run.run_org("acme")
    assert calls == (["run", "told"] if told else ["run"])


def test_each_org_runs_on_the_scheduler_once():
    added: list[str] = []

    class _Scheduler:
        def get_job(self, job_id):
            return job_id if job_id in added else None

        def add_job(self, func, **kwargs):
            assert func is expiry_run.run_org
            added.append(kwargs["id"])

    scheduler = _Scheduler()
    expiry_run.register_org(scheduler, "acme")  # pyright: ignore[reportArgumentType]
    expiry_run.register_org(scheduler, "acme")  # pyright: ignore[reportArgumentType]
    assert added == ["entitlements:expiry_run:org_acme"]
