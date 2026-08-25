# Copyright (c) 2026 Kenneth Stott
# Canary: 6b1d0e77-4c39-4a52-91ec-8f0a2d3b5e64
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1572: an invitation IS the admission decision; the org email rule does not gate it.

The rule decides who may join on their own initiative. An invitation is an org admin naming a
person — that same decision, already made, single-use and expiring. Gating redemption on the rule
refused exactly the people an admin deliberately reached outside their own domain, with an error
the invitee could do nothing about. Both halves are asserted here: redemption ignores the rule,
and auto-join still enforces it, because dropping the gate from redemption must not drop it from
the path it actually governs.
"""

# Requirements: REQ-516, REQ-1268, REQ-1269, REQ-1572

from __future__ import annotations

import datetime
import types

from datetime import timezone

import pytest

# The org admits only @acme.com on its own initiative.
ACME_RULE = r"@acme\.com$"
OUTSIDER = "bob@rival.com"


class _Row:
    def __init__(self, mapping):
        self._mapping = mapping

    def __getitem__(self, i):
        return list(self._mapping.values())[i]


class _Result:
    def __init__(self, rows):
        self._rows = rows

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return self._rows


class _Conn:
    """Records what redemption did to the platform plane."""

    def __init__(self, plane):
        self._plane = plane

    async def execute_core(self, stmt):
        text = str(stmt)
        self._plane.statements.append(text)
        if text.lstrip().upper().startswith("SELECT"):
            return _Result([_Row(self._plane.invite)])
        return _Result([])

    async def upsert(self, table, values, **kwargs):
        self._plane.memberships.append(values)


class _Db:
    def __init__(self, plane):
        self._plane = plane

    def acquire(self):
        conn = _Conn(self._plane)

        class _Ctx:
            async def __aenter__(self_inner):
                return conn

            async def __aexit__(self_inner, *exc):
                return False

        return _Ctx()


@pytest.fixture
def plane(monkeypatch):
    """An org restricted to @acme.com holding a live invitation for an outsider."""
    state = types.SimpleNamespace(
        statements=[],
        memberships=[],
        roles=[],
        invite={
            "org_id": "acme",
            "role_id": "analyst",
            "expires_at": datetime.datetime(2099, 1, 1, tzinfo=timezone.utc),
            "used_at": None,
        },
    )
    admin_db = _Db(state)
    monkeypatch.setattr(
        "provisa.api.app.state", types.SimpleNamespace(admin_db=admin_db), raising=False
    )

    async def _resolve_invite_role(org_id, role_id):
        return role_id

    async def _ensure_org_runtime(org_id):
        state.bound_org = org_id
        return types.SimpleNamespace(tenant_db=object())

    async def _grant_org_role(tenant_db, user_id, role_id):
        state.roles.append((user_id, role_id))

    async def _bind_member_to_org_trial(db, org_id, email):
        return None

    monkeypatch.setattr(
        "provisa.api.admin.invites_router.resolve_invite_role", _resolve_invite_role
    )
    monkeypatch.setattr("provisa.api.app.ensure_org_runtime", _ensure_org_runtime, raising=False)
    monkeypatch.setattr("provisa.core.org_membership.grant_org_role", _grant_org_role)
    monkeypatch.setattr("provisa.core.commerce.bind_member_to_org_trial", _bind_member_to_org_trial)
    return state


def _request(email: str, user_id: str = "bob"):
    return types.SimpleNamespace(
        state=types.SimpleNamespace(identity=types.SimpleNamespace(user_id=user_id, email=email))
    )


@pytest.mark.asyncio
async def test_an_address_the_org_rule_excludes_still_redeems_its_invitation(plane):
    """bob@rival.com is exactly who the rule keeps out and exactly who the admin invited."""
    from provisa.api.auth_router import RedeemInviteRequest, redeem_invite

    body = await redeem_invite(RedeemInviteRequest(token="tok-1"), _request(OUTSIDER))

    assert body == {"user_id": "bob", "org_id": "acme", "role_id": "analyst"}


@pytest.mark.asyncio
async def test_redemption_grants_the_invited_role_in_the_invited_org(plane):
    from provisa.api.auth_router import RedeemInviteRequest, redeem_invite

    await redeem_invite(RedeemInviteRequest(token="tok-1"), _request(OUTSIDER))

    assert plane.memberships and plane.memberships[0]["org_id"] == "acme"
    assert plane.roles == [("bob", "analyst")]
    # The role lands in the INVITED org's schema, not the request's default org.
    assert plane.bound_org == "acme"


@pytest.mark.asyncio
async def test_the_invitation_is_burned_so_it_admits_exactly_one_person(plane):
    from provisa.api.auth_router import RedeemInviteRequest, redeem_invite

    await redeem_invite(RedeemInviteRequest(token="tok-1"), _request(OUTSIDER))

    assert any("UPDATE org_invites" in s and "used_at" in s for s in plane.statements)


@pytest.mark.asyncio
async def test_the_org_email_rule_is_never_read_during_redemption(plane):
    """The gate is absent, not merely satisfied: redemption reads the invite, nothing about orgs."""
    from provisa.api.auth_router import RedeemInviteRequest, redeem_invite

    await redeem_invite(RedeemInviteRequest(token="tok-1"), _request(OUTSIDER))

    assert not any("email_rule" in s for s in plane.statements)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "used_at,expires_at",
    [
        (
            datetime.datetime(2026, 1, 1, tzinfo=timezone.utc),
            datetime.datetime(2099, 1, 1, tzinfo=timezone.utc),
        ),
        (None, datetime.datetime(2020, 1, 1, tzinfo=timezone.utc)),
    ],
    ids=["already-used", "expired"],
)
async def test_a_spent_or_expired_invitation_admits_nobody(plane, used_at, expires_at):
    """Single-use and expiring is what makes the invitation safe to trust over the rule."""
    from provisa.api.auth_router import RedeemInviteRequest, redeem_invite
    from provisa.api.errors import ApiError

    plane.invite["used_at"] = used_at
    plane.invite["expires_at"] = expires_at

    with pytest.raises(ApiError) as exc:
        await redeem_invite(RedeemInviteRequest(token="tok-1"), _request(OUTSIDER))

    assert exc.value.status_code == 400
    assert plane.memberships == []


class _RuleConn:
    def __init__(self, rows):
        self._rows = rows
        self._call = 0

    async def execute_core(self, stmt):
        self._call += 1
        # First read is the auto-join orgs, second the caller's opt-outs.
        return _Result(self._rows if self._call == 1 else [])


class _RuleDb:
    def __init__(self, rows):
        self._rows = rows

    def acquire(self):
        conn = _RuleConn(self._rows)

        class _Ctx:
            async def __aenter__(self_inner):
                return conn

            async def __aexit__(self_inner, *exc):
                return False

        return _Ctx()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "email,admitted",
    [(OUTSIDER, False), ("dana@acme.com", True)],
    ids=["outsider-refused", "insider-admitted"],
)
async def test_the_rule_still_governs_joining_on_your_own_initiative(email, admitted):
    """REQ-1268/1269 are untouched: what REQ-1572 removed from redemption stays on auto-join."""
    from provisa.core.org_membership import resolve_auto_join_orgs

    db = _RuleDb([_Row({"id": "acme", "email_rule": ACME_RULE, "auto_join_role": "analyst"})])

    joined = await resolve_auto_join_orgs(db, email, "bob")

    assert (joined == [("acme", "analyst")]) is admitted
