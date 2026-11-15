# Copyright (c) 2026 Kenneth Stott
# Canary: 0b6a2f4e-91c3-4d7a-8e15-c2d9f3a07b46
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2077, REQ-2219, EXPIRY IS ON THE GRANT AND ON ITS ENTRIES: each org's one expiry run. It
sweeps the role grants (test_role_grants.py), and ahead of each entry's expiry rule, by the one
expiring-soon setting, it emits an entitlements.expiring_soon event; once the date has passed, an
entitlements.expired event. Each is an entry in the org's administrative trail, once per rule and
date, and where mail is configured one notice per person and role names the role when it is
ending and each entry that is."""

# Requirements: REQ-2077, REQ-2219

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from sqlalchemy import insert, select

from provisa.auth import role_grants
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
    monkeypatch.setattr(expiry_run, "notice_window", lambda: timedelta(days=days["value"]))
    return days


def test_one_expiring_soon_setting_covers_the_grant_and_its_entries():
    keys = {s.key for s in settings_catalog.DECLARED}
    assert role_grants.NOTICE_SETTING == "auth.role_grant_notice_days"
    assert role_grants.NOTICE_SETTING in keys
    assert "entitlements.expiring_soon_days" not in keys
    assert expiry_run.notice_window is role_grants.notice_window
    [setting] = [s for s in settings_catalog.DECLARED if s.key == role_grants.NOTICE_SETTING]
    assert (setting.type, setting.unit) == ("int", "days")
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


async def test_the_one_setting_is_the_entries_threshold(control_plane, threshold):
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


async def _people(db) -> None:
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


@pytest.fixture
def sent(monkeypatch) -> list[tuple[str, str, str, str]]:
    out: list[tuple[str, str, str, str]] = []

    async def _send(org_id, to, subject, body, *, kind):
        out.append((to, subject, body, kind))

    monkeypatch.setattr("provisa.auth.role_grants.send_notice", _send)
    return out


def _outcome(grants=None, entries=None) -> expiry_run.Outcome:
    return expiry_run.Outcome(
        grants or role_grants.Sweep([], [], [], []), entries or expiry_run.Run([], [])
    )


async def test_one_notice_names_the_ending_role_and_each_ending_entry(control_plane, sent):
    db = control_plane.org()
    await _people(db)
    role_end = _NOW + timedelta(days=5)
    first = ExpiryRule("viewer", _NOW + timedelta(days=2), {"read": ("finance",)})
    second = ExpiryRule("viewer", _NOW + timedelta(days=4), {"*": ("sales.orders",)})
    grant = role_grants.Grant("ada", "viewer", "*", role_end)
    entries = expiry_run.Run([expiry_run.Due("ada", second), expiry_run.Due("ada", first)], [])
    await expiry_run._tell(db, "acme", _outcome(role_grants.Sweep([], [], [grant], []), entries))
    when = expiry_run._when
    body = "\n".join(
        [
            f"The role viewer in acme ends {when(role_end)}.",
            f"read: finance in the role viewer ends {when(first.ends_at)}.",
            f"*: sales.orders in the role viewer ends {when(second.ends_at)}.",
        ]
    )
    subject = f"role viewer in acme ends {when(role_end)}"
    assert sorted(sent) == sorted(
        [
            ("ada@example.com", f"Your {subject}", body, "role_grant"),
            ("boss@example.com", f"ada's {subject}", body, "role_grant"),
        ]
    )


async def test_entries_ending_under_a_grant_that_carries_on_name_only_the_entries(
    control_plane, sent
):
    db = control_plane.org()
    await _people(db)
    rule = ExpiryRule("viewer", _NOW + timedelta(days=3), {"read": ("finance",)})
    await expiry_run._tell(
        db, "acme", _outcome(entries=expiry_run.Run([expiry_run.Due("ada", rule)], []))
    )
    when = expiry_run._when(rule.ends_at)
    body = f"read: finance in the role viewer ends {when}."
    assert sorted(sent) == sorted(
        [
            (
                "ada@example.com",
                f"Your access in the role viewer in acme ends {when}",
                body,
                "role_grant",
            ),
            (
                "boss@example.com",
                f"ada's access in the role viewer in acme ends {when}",
                body,
                "role_grant",
            ),
        ]
    )


async def test_the_admins_are_told_of_what_ended_in_one_notice(control_plane, sent):
    db = control_plane.org()
    await _people(db)
    gone = role_grants.Grant("ada", "analyst", "*", _NOW)
    ended = ExpiryRule("analyst", _NOW, {"read": ("finance",)})
    unused = role_grants.Grant("ada", "reader", "sales", _NOW)
    outcome = _outcome(
        role_grants.Sweep([gone], [unused], [], []),
        expiry_run.Run([], [expiry_run.Due("ada", ended)]),
    )
    await expiry_run._tell(db, "acme", outcome)
    when = expiry_run._when(_NOW)
    assert sorted(sent) == sorted(
        [
            (
                "boss@example.com",
                "ada's role analyst in acme was removed",
                "The role analyst in acme was removed: the grant expired.\n"
                f"read: finance in the role analyst ended {when}.",
                "role_grant",
            ),
            (
                "boss@example.com",
                "ada's role reader in acme was removed",
                "The role reader in the domain sales in acme was removed: the grant went unused.",
                "role_grant",
            ),
        ]
    )


async def test_the_one_run_sweeps_the_grants_and_records_the_entries(control_plane, monkeypatch):
    from provisa.api import app as app_module

    db = control_plane.org()
    async with db.acquire() as conn:
        await conn.execute_core(
            insert(user_role_assignments).values(
                user_id="ada",
                role_id="org_admin",
                domain_id="*",
                expires_at=_NOW - timedelta(minutes=1),
            )
        )
    entry = await _rule(db, "bob", _NOW - timedelta(hours=1))
    state = app_module.state
    monkeypatch.setattr(type(state), "model_db", property(lambda s: db))
    config = SimpleNamespace(multitenancy=False, mail=SimpleNamespace(provider=""))
    monkeypatch.setattr(state, "config", config, raising=False)
    outcome = await expiry_run.run_org("acme")
    assert [(g.user_id, g.role_id) for g in outcome.grants.expired] == [("ada", "org_admin")]
    assert [d.rule.rule_key for d in outcome.entries.expired] == [entry]
    async with db.acquire() as conn:
        rows = (await conn.execute_core(select(admin_audit_log))).fetchall()
    assert sorted((r.action, r.subject_id) for r in rows) == [
        (expiry_run.EXPIRED, "bob"),
        ("role.expired", "ada"),
    ]


@pytest.mark.parametrize(("provider", "told"), [("smtp", True), ("", False)])
async def test_notices_go_wherever_mail_is_configured_single_org_included(
    monkeypatch, provider, told
):
    from provisa.api import app as app_module

    calls: list[str] = []

    async def _sweep(db, *, notify):
        calls.append(f"sweep notify={notify}")
        return role_grants.Sweep([], [], [], [])

    async def _run(db, now):
        calls.append("run")
        return expiry_run.Run([], [])

    async def _tell(db, org_id, outcome):
        calls.append("told")

    monkeypatch.setattr(expiry_run, "sweep", _sweep)
    monkeypatch.setattr(expiry_run, "run", _run)
    monkeypatch.setattr(expiry_run, "_tell", _tell)
    state = app_module.state
    monkeypatch.setattr(type(state), "model_db", property(lambda s: object()))
    config = SimpleNamespace(multitenancy=False, mail=SimpleNamespace(provider=provider))
    monkeypatch.setattr(state, "config", config, raising=False)
    await expiry_run.run_org("acme")
    expected = [f"sweep notify={told}", "run"]
    assert calls == ([*expected, "told"] if told else expected)


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
