# Copyright (c) 2026 Kenneth Stott
# Canary: f96945b0-1762-4264-bfe7-ea6b40af4ba4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1268/REQ-1269: org join-policy primitives.

``email_matches_rule`` is the predicate the auto-join gate applies (REQ-1572: invitation
redemption does not consult it — an invitation is the admission decision); its truth table — NULL rule admits everything, a rule demands a non-empty
matching email, a corrupt stored rule fails CLOSED. ``_validate_org_policy`` is the
write-time guard: only compilable rules are ever stored (which is why the read-side
re.error guard is a hand-edit backstop, not a code path), and auto_join without a
default role is rejected — membership without a role would grant nothing.
"""

from __future__ import annotations

import pytest
from fastapi import HTTPException

from provisa.api.admin.orgs_router import _validate_org_policy
from provisa.core.org_membership import email_matches_rule

# ---------------------------------------------------------------------------
# email_matches_rule truth table
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("email", "rule", "expected"),
    [
        # NULL rule imposes no restriction — even a missing email passes.
        ("alice@acme.com", None, True),
        (None, None, True),
        ("", None, True),
        # Anchored domain rule.
        ("alice@acme.com", r"@acme\.com$", True),
        ("alice@example.com", r"@acme\.com$", False),
        # Unanchored substring would over-match without the anchor — the rule text decides.
        ("alice@acme.com.evil.org", r"@acme\.com$", False),
        ("alice@acme.com.evil.org", r"@acme\.com", True),
        # A non-NULL rule requires a non-empty email.
        (None, r"@acme\.com$", False),
        ("", r"@acme\.com$", False),
        # Corrupt stored rule (hand-edited DB) fails closed: no match, never an exception.
        ("alice@acme.com", r"[unclosed", False),
    ],
)
def test_email_matches_rule(email, rule, expected):
    assert email_matches_rule(email, rule) is expected


# ---------------------------------------------------------------------------
# _validate_org_policy write-time guard
# ---------------------------------------------------------------------------


def test_policy_accepts_valid_rule_and_no_auto_join():
    _validate_org_policy(r"@acme\.com$", False, None)


def test_policy_accepts_auto_join_with_role():
    _validate_org_policy(r"@acme\.com$", True, "analyst")


def test_policy_accepts_all_defaults():
    _validate_org_policy(None, False, None)


def test_policy_rejects_uncompilable_rule():
    with pytest.raises(HTTPException) as exc:
        _validate_org_policy(r"[unclosed", False, None)
    assert exc.value.status_code == 400
    assert "Invalid email rule" in exc.value.detail


def test_policy_rejects_auto_join_without_role():
    with pytest.raises(HTTPException) as exc:
        _validate_org_policy(None, True, None)
    assert exc.value.status_code == 400
    assert "auto_join_role" in exc.value.detail


# ---------------------------------------------------------------------------
# REQ-1477: auto-join may not rest on a consumer mailbox domain
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "rule",
    [
        r"@gmail\.com$",
        r"@outlook\.com$",
        # Unanchored, so it admits every address that merely contains the domain.
        r"gmail",
        # A rule broad enough to admit anything admits consumer mailboxes too.
        r"@",
        # No rule at all: auto_join would take every address that ever signs in.
        None,
    ],
)
def test_policy_rejects_auto_join_on_a_public_domain(rule):
    with pytest.raises(HTTPException) as exc:
        _validate_org_policy(rule, True, "analyst")
    assert exc.value.status_code == 400
    assert exc.value.code == "orgs.auto_join_public_domain"


def test_a_public_domain_rule_is_allowed_when_auto_join_is_off():
    """The rule alone only bounds who may redeem an invite — an invite is still a human decision."""
    _validate_org_policy(r"@gmail\.com$", False, None)


def test_a_corporate_domain_that_merely_contains_a_public_name_is_allowed():
    _validate_org_policy(r"@gmail-partners\.com$", True, "analyst")


@pytest.mark.asyncio
async def test_a_stored_public_domain_rule_never_auto_joins(monkeypatch):
    """Rows predating the guard, or edited straight into the table, are refused at read time."""
    from provisa.core import org_membership as mod

    class _Result:
        def __init__(self, rows):
            self._rows = rows

        def fetchall(self):
            return self._rows

    class _Row:
        def __init__(self, mapping):
            self._mapping = mapping

        def __getitem__(self, i):
            return list(self._mapping.values())[i]

    class _Conn:
        def __init__(self):
            self.calls = 0

        async def execute_core(self, stmt):
            self.calls += 1
            if self.calls == 1:
                return _Result(
                    [
                        _Row(
                            {
                                "id": "openhouse",
                                "email_rule": r"@gmail\.com$",
                                "auto_join_role": "analyst",
                            }
                        ),
                        _Row(
                            {
                                "id": "acme",
                                "email_rule": r"@acme\.com$",
                                "auto_join_role": "analyst",
                            }
                        ),
                    ]
                )
            return _Result([])

    class _Db:
        def __init__(self):
            self.conn = _Conn()

        def acquire(self):
            conn = self.conn

            class _Ctx:
                async def __aenter__(self_inner):
                    return conn

                async def __aexit__(self_inner, *exc):
                    return False

            return _Ctx()

    assert await mod.resolve_auto_join_orgs(_Db(), "mallory@gmail.com", "mallory") == []
    assert await mod.resolve_auto_join_orgs(_Db(), "alice@acme.com", "alice") == [
        ("acme", "analyst")
    ]


# ---------------------------------------------------------------------------
# REQ-1567: how far past its own domains an auto-join rule reaches
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("rule", "reason"),
    [
        # Names no domain: it matches by coincidence, at any host in the world.
        (r"acme", "unbounded_no_domain"),
        # Not anchored at the end: acme.com.example.net is registered by whoever wants it.
        (r"@acme\.com", "unbounded_suffix"),
    ],
)
def test_policy_refuses_a_rule_no_membership_could_be(rule, reason):
    with pytest.raises(HTTPException) as exc:
        _validate_org_policy(rule, True, "analyst")
    assert exc.value.status_code == 400
    assert exc.value.code == "orgs.auto_join_rule_unbounded"


def test_an_unbounded_rule_is_refused_even_when_the_risk_is_accepted():
    """Consent is offered for breadth, not for a rule whose reach has no edge to describe."""
    with pytest.raises(HTTPException) as exc:
        _validate_org_policy(r"@acme\.com", True, "analyst", True)
    assert exc.value.code == "orgs.auto_join_rule_unbounded"


def test_a_rule_reaching_past_its_domain_is_shown_to_its_author_first():
    # No "@" boundary, so notacme.com — a domain a stranger can register — is admitted too.
    with pytest.raises(HTTPException) as exc:
        _validate_org_policy(r"acme\.com$", True, "analyst")
    assert exc.value.status_code == 400
    assert exc.value.code == "orgs.auto_join_breadth_unacknowledged"
    assert "someone@notacme.com" in exc.value.detail
    assert "outside your organization" in exc.value.detail


def test_the_same_rule_saves_once_the_author_accepts_the_risk():
    """The subdivision case: only the author knows a-division.acme.com is theirs."""
    _validate_org_policy(r"@([a-z-]+\.)?acme\.com$", True, "analyst", True)


def test_one_exact_domain_is_never_asked_to_accept_anything():
    _validate_org_policy(r"@acme\.com$", True, "analyst")


def test_breadth_is_not_measured_when_auto_join_is_off():
    """With auto-join off the rule bounds invite redemption, and an invite is a human decision."""
    _validate_org_policy(r"acme", False, None)


# ---------------------------------------------------------------------------
# REQ-1567: an auto-join rule is an exclusive claim
# ---------------------------------------------------------------------------


class _FakePool:
    """An admin pool answering one SELECT with ``row``."""

    def __init__(self, row):
        self.row = row
        self.statements = []

    def acquire(pool_self):
        class _Ctx:
            async def __aenter__(self):
                class _Conn:
                    async def execute_core(_, stmt):
                        pool_self.statements.append(str(stmt))

                        class _Result:
                            def fetchone(self):
                                return pool_self.row

                        return _Result()

                return _Conn()

            async def __aexit__(self, *exc):
                return False

        return _Ctx()


@pytest.mark.asyncio
async def test_a_rule_another_org_already_auto_joins_on_is_refused(monkeypatch):
    from provisa.api.admin import orgs_router

    pool = _FakePool(("acme", "Acme"))
    monkeypatch.setattr(orgs_router, "_admin_pool", lambda: pool)
    with pytest.raises(HTTPException) as exc:
        await orgs_router._reject_duplicate_auto_join(r"@acme\.com$", True)
    assert exc.value.status_code == 409
    assert exc.value.code == "orgs.auto_join_rule_taken"
    assert "Acme" in exc.value.detail


@pytest.mark.asyncio
async def test_an_unclaimed_rule_passes(monkeypatch):
    from provisa.api.admin import orgs_router

    monkeypatch.setattr(orgs_router, "_admin_pool", lambda: _FakePool(None))
    await orgs_router._reject_duplicate_auto_join(r"@acme\.com$", True)


@pytest.mark.asyncio
async def test_an_org_does_not_collide_with_itself_on_an_edit(monkeypatch):
    """Saving the settings page unchanged must not report the org's own rule as taken."""
    from provisa.api.admin import orgs_router

    pool = _FakePool(None)
    monkeypatch.setattr(orgs_router, "_admin_pool", lambda: pool)
    await orgs_router._reject_duplicate_auto_join(r"@acme\.com$", True, exclude_org_id="acme")
    assert "id !=" in pool.statements[0]


@pytest.mark.asyncio
async def test_two_orgs_may_share_a_rule_that_only_bounds_invites(monkeypatch):
    """Exclusivity is about the automatic claim; an invite filter takes nobody by itself."""
    from provisa.api.admin import orgs_router

    pool = _FakePool(("acme", "Acme"))
    monkeypatch.setattr(orgs_router, "_admin_pool", lambda: pool)
    await orgs_router._reject_duplicate_auto_join(r"@acme\.com$", False)
    assert pool.statements == []
