# Copyright (c) 2026 Kenneth Stott
# Canary: c17b9d3f-2e84-40a6-9b5d-71fce0a28d64
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1266: authz truth table for issuing/managing org invites.

_require_org_admin gates create/list/revoke of invites. It must let a platform admin act in
any org, confine an org_admin to exactly the org they are currently acting in (active_org_id)
AND backed by an admin-plane membership row, allow dev/no-auth, and reject everything else.
A too-lax branch here lets any authenticated user mint an invite for any org (the hole this
requirement closes)."""

from __future__ import annotations

import types
from typing import cast

import pytest
from fastapi import HTTPException, Request

import provisa.api.admin.invites_router as inv


class _FakeResult:
    def __init__(self, row):
        self._row = row

    def fetchone(self):
        return self._row


class _FakeConn:
    def __init__(self, row):
        self._row = row

    async def execute_core(self, _stmt):
        return _FakeResult(self._row)


class _FakeAcquire:
    def __init__(self, row):
        self._row = row

    async def __aenter__(self):
        return _FakeConn(self._row)

    async def __aexit__(self, *exc):
        return False


class _FakePool:
    def __init__(self, membership_row):
        self._row = membership_row

    def acquire(self):
        return _FakeAcquire(self._row)


def _request(*, identity, active_org) -> Request:
    st = types.SimpleNamespace(identity=identity, active_org_id=active_org)
    return cast(Request, types.SimpleNamespace(state=st))


def _identity(user_id, roles):
    return types.SimpleNamespace(user_id=user_id, roles=roles)


@pytest.fixture
def patch(monkeypatch):
    """Control resolved caps and the membership-row lookup independently of a live DB."""

    def _apply(*, caps, membership_row):
        monkeypatch.setattr(
            "provisa.api.admin.capabilities._resolved_capabilities",
            lambda _identity, _state: caps,
        )
        monkeypatch.setattr(inv, "_pool", lambda _request: _FakePool(membership_row))

    return _apply


@pytest.mark.asyncio
async def test_anonymous_dev_mode_allowed(patch):
    patch(caps=set(), membership_row=None)
    # Anonymous identity == dev/no-auth bypass (matches _require_superadmin).
    await inv._require_org_admin(_request(identity=None, active_org=None), "acme")


@pytest.mark.asyncio
async def test_superadmin_allowed_any_org(patch):
    patch(caps={"superadmin"}, membership_row=None)
    await inv._require_org_admin(
        _request(identity=_identity("root", ["superadmin"]), active_org="other"), "acme"
    )


@pytest.mark.asyncio
async def test_platform_admin_allowed_any_org(patch):
    patch(caps={"admin"}, membership_row=None)
    await inv._require_org_admin(
        _request(identity=_identity("p", ["admin"]), active_org=None), "acme"
    )


@pytest.mark.asyncio
async def test_org_admin_of_active_org_and_member_allowed(patch):
    # REQ-1337: what allows this is the user_management RIGHT, not the "org_admin" role name.
    patch(caps={"user_management"}, membership_row=("acme",))
    await inv._require_org_admin(
        _request(identity=_identity("alice", ["org_admin:acme"]), active_org="acme"), "acme"
    )


@pytest.mark.asyncio
async def test_org_admin_of_different_active_org_rejected(patch):
    # org_admin acting in acme cannot mint an invite for beta: user_management without cross_org is
    # confined to the org being acted in (REQ-1337).
    patch(caps={"user_management"}, membership_row=("beta",))
    with pytest.raises(HTTPException) as ei:
        await inv._require_org_admin(
            _request(identity=_identity("alice", ["org_admin:acme"]), active_org="acme"), "beta"
        )
    assert ei.value.status_code == 403


@pytest.mark.asyncio
async def test_org_admin_role_but_no_membership_rejected(patch):
    # user_management held and active org matches, but no admin-plane membership row → reject.
    patch(caps={"user_management"}, membership_row=None)
    with pytest.raises(HTTPException) as ei:
        await inv._require_org_admin(
            _request(identity=_identity("alice", ["org_admin:acme"]), active_org="acme"), "acme"
        )
    assert ei.value.status_code == 403


@pytest.mark.asyncio
async def test_plain_authenticated_user_rejected(patch):
    # The closed hole: an authenticated user holding no user_management right cannot invite for any
    # org, whatever roles they name.
    patch(caps={"query_development"}, membership_row=("acme",))
    with pytest.raises(HTTPException) as ei:
        await inv._require_org_admin(
            _request(identity=_identity("bob", ["viewer"]), active_org="acme"), "acme"
        )
    assert ei.value.status_code == 403


@pytest.mark.asyncio
async def test_allow_cross_org_false_rejects_platform_admin_with_no_membership(patch):
    # REQ-1605: platform_admin may still ACT (create invites, default allow_cross_org=True), but a
    # caller reading an org's members/settings/branding/config is withheld the cross_org bypass —
    # holding only the control-plane right with no admin-plane membership row in this org is a 403.
    patch(caps={"admin"}, membership_row=None)
    with pytest.raises(HTTPException) as ei:
        await inv._require_org_admin(
            _request(identity=_identity("p", ["admin"]), active_org="acme"),
            "acme",
            allow_cross_org=False,
        )
    assert ei.value.status_code == 403


@pytest.mark.asyncio
async def test_allow_cross_org_false_allows_platform_admin_with_actual_membership(patch):
    # A platform_admin who also holds a real org_admin assignment in this org (seeded, REQ-1599's
    # sandbox, or a REQ-1303 recovery grant) reads it the same as any other org_admin would.
    patch(caps={"admin", "user_management"}, membership_row=("acme",))
    await inv._require_org_admin(
        _request(identity=_identity("p", ["admin", "org_admin:acme"]), active_org="acme"),
        "acme",
        allow_cross_org=False,
    )
