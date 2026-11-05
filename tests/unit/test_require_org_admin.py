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
any org only where the route asks for it (an org's lifecycle and the audited grant_org_admin
recovery, REQ-1327), confine an org_admin to exactly the org they are currently acting in (active_org_id)
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
@pytest.mark.parametrize("retired", ["superadmin", "admin"])
async def test_a_retired_wildcard_string_reaches_no_org(patch, retired):
    # REQ-1327: no capability string means "every right". Acting in an org one does not administer
    # is the cross_org right and nothing else.
    patch(caps={retired}, membership_row=None)
    with pytest.raises(HTTPException) as err:
        await inv._require_org_admin(
            _request(identity=_identity("root", [retired]), active_org="other"), "acme"
        )
    assert err.value.status_code == 403


@pytest.mark.asyncio
async def test_cross_org_refused_by_default(patch):
    """REQ-1327 (amended 2026-11-05): this test used to assert that cross_org passed the gate in
    any org. The platform admin now keeps only an org's lifecycle and never edits its members,
    join settings or branding, so the bypass is withheld unless a lifecycle route asks for it.
    The refusal names the right the org's own administrator holds."""
    patch(caps={"platform_settings", "cross_org"}, membership_row=None)
    with pytest.raises(HTTPException) as ei:
        await inv._require_org_admin(
            _request(identity=_identity("p", ["platform_admin"]), active_org=None), "acme"
        )
    assert ei.value.status_code == 403
    assert getattr(ei.value, "code") == "invites.user_management_in_org_required"
    assert "user_management" in str(ei.value.detail)


@pytest.mark.asyncio
async def test_cross_org_allowed_any_org_when_the_route_is_lifecycle(patch):
    # Deleting an org and the audited grant_org_admin recovery stay the platform's (REQ-1327).
    patch(caps={"platform_settings", "cross_org"}, membership_row=None)
    await inv._require_org_admin(
        _request(identity=_identity("p", ["platform_admin"]), active_org=None),
        "acme",
        allow_cross_org=True,
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
    # REQ-1605, REQ-1327: reading an org's members/settings/branding/config, like editing them, is
    # withheld the cross_org bypass — holding only the control-plane right with no admin-plane
    # membership row in this org is a 403.
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


# ── REQ-1327 (amended 2026-11-05): which routes keep the platform's cross_org reach ──────────────
#
# The gate is left real and wrapped: a route that refuses the platform admin raises the gate's own
# 403; a route that admits it reaches the wrapper's _Passed, which stops the handler before any
# store is touched.


class _Passed(Exception):
    pass


@pytest.fixture
def platform_admin_at_the_gate(patch, monkeypatch):
    """A platform admin acting from root (cross_org, no membership row in acme)."""
    patch(caps={"platform_settings", "cross_org"}, membership_row=None)
    real_gate = inv._require_org_admin

    async def _gate(request, org_id, **kwargs):
        await real_gate(request, org_id, **kwargs)
        raise _Passed

    monkeypatch.setattr(inv, "_require_org_admin", _gate)
    return _request(identity=_identity("p", ["platform_admin"]), active_org="root")


def _org_routes():
    import provisa.api.admin.orgs_router as orgs

    policy = orgs.OrgPolicyBody.model_construct()
    branding = orgs.OrgBrandingBody.model_construct()
    member = orgs.AddMemberBody.model_construct()
    invite = inv.CreateInviteBody.model_construct(org_id="acme", role_id=None)
    analyst_invite = inv.CreateInviteBody.model_construct(org_id="acme", role_id="analyst")
    return {
        "PATCH /orgs/{id}/settings": lambda r: orgs.update_org_settings("acme", policy, r),
        "PATCH /orgs/{id}/branding": lambda r: orgs.update_org_branding("acme", branding, r),
        "POST /orgs/{id}/branding/preview-invite": lambda r: orgs.preview_invite_message(
            "acme", branding, r
        ),
        "PUT /orgs/{id}/branding/logo": lambda r: orgs.upload_org_logo("acme", r),
        "DELETE /orgs/{id}/branding/logo": lambda r: orgs.delete_org_logo("acme", r),
        "POST /orgs/{id}/members": lambda r: orgs.add_member("acme", member, r),
        "DELETE /orgs/{id}/members/{user}": lambda r: orgs.remove_member("acme", "u9", r),
        "POST /invites": lambda r: inv.create_invite(invite, r),
        "POST /invites (analyst)": lambda r: inv.create_invite(analyst_invite, r),
        "DELETE /orgs/{id}": lambda r: orgs.delete_org("acme", r, confirm="acme"),
        "POST /orgs/{id}/admins/{user}": lambda r: orgs.grant_org_admin_role("acme", "u9", r),
        "DELETE /orgs/{id}/admins/{user}": lambda r: orgs.revoke_org_admin_role("acme", "u9", r),
    }


_ORG_LAYER = [
    "PATCH /orgs/{id}/settings",
    "PATCH /orgs/{id}/branding",
    "POST /orgs/{id}/branding/preview-invite",
    "PUT /orgs/{id}/branding/logo",
    "DELETE /orgs/{id}/branding/logo",
    "POST /orgs/{id}/members",
    "DELETE /orgs/{id}/members/{user}",
    "POST /invites",
    "POST /invites (analyst)",
]
_LIFECYCLE_AND_RECOVERY = [
    "DELETE /orgs/{id}",
    "POST /orgs/{id}/admins/{user}",
    "DELETE /orgs/{id}/admins/{user}",
]


@pytest.mark.asyncio
@pytest.mark.parametrize("route", _ORG_LAYER)
async def test_platform_admin_is_refused_the_orgs_own_layer(platform_admin_at_the_gate, route):
    # Members, join settings / auto-join role, branding and invitations are the org admin's
    # (REQ-1327): the platform admin is refused, and told the right the org's admin holds.
    with pytest.raises(HTTPException) as ei:
        await _org_routes()[route](platform_admin_at_the_gate)
    assert ei.value.status_code == 403
    assert getattr(ei.value, "code") == "invites.user_management_in_org_required"
    assert "user_management in acme" in str(ei.value.detail)


@pytest.mark.asyncio
@pytest.mark.parametrize("route", _LIFECYCLE_AND_RECOVERY)
async def test_platform_admin_keeps_lifecycle_and_recovery(platform_admin_at_the_gate, route):
    # Deleting an org is its lifecycle; the platform appoints, replaces and removes an org's admins
    # through grant_org_admin and its revocation (REQ-1327). These stay the platform admin's.
    with pytest.raises(_Passed):
        await _org_routes()[route](platform_admin_at_the_gate)


@pytest.mark.asyncio
@pytest.mark.parametrize("route", _ORG_LAYER)
async def test_the_orgs_own_admin_passes_its_layer(patch, monkeypatch, route):
    patch(caps={"user_management", "org_settings"}, membership_row=("acme",))
    real_gate = inv._require_org_admin

    async def _gate(request, org_id, **kwargs):
        await real_gate(request, org_id, **kwargs)
        raise _Passed

    monkeypatch.setattr(inv, "_require_org_admin", _gate)
    with pytest.raises(_Passed):
        await _org_routes()[route](
            _request(identity=_identity("alice", ["org_admin"]), active_org="acme")
        )
