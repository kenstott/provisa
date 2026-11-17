# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1327 (amended 2026-11-16): deleting an organization and granting org_admin need org_admin.

user_management alone allows neither. Holding org_admin is read as rights (REQ-1337): every right
the org_admin role carries. The platform administrator (cross_org) keeps an org's lifecycle and the
appointment of its admins. Every door to the two acts is asked here: the org delete and the
org_admin grant routes, the users surface, invitations, data product offers and the auto-join role.
"""

from __future__ import annotations

import types
from typing import cast

import pytest
from fastapi import Request

import provisa.api.app as appmod
from provisa.api.admin import data_product_offers_router as offers_router
from provisa.api.admin import invites_router as inv
from provisa.api.admin import local_users_router as users_router
from provisa.api.admin import orgs_router
from provisa.api.errors import ApiError
from provisa.core.db import STANDARD_ROLE_RIGHTS
from provisa.security.rights import (
    DEPLOYMENT_GRANTER,
    ORG_ADMIN_ROLE,
    check_role_grant,
)

ORG = "acme"
ORG_ADMIN_RIGHTS = sorted(STANDARD_ROLE_RIGHTS[ORG_ADMIN_ROLE])

_ROLES = {
    ORG_ADMIN_ROLE: {"capabilities": ORG_ADMIN_RIGHTS, "domain_access": ["*"]},
    "people_admin": {"capabilities": ["user_management"], "domain_access": ["*"]},
    "platform_admin": {"capabilities": ["platform_settings", "cross_org"], "domain_access": []},
}


def _request(user_id: str, *roles: str, active_org: str | None = ORG) -> Request:
    identity = types.SimpleNamespace(user_id=user_id, roles=list(roles), acting_roles=list(roles))
    return cast(
        Request,
        types.SimpleNamespace(
            state=types.SimpleNamespace(identity=identity, active_org_id=active_org)
        ),
    )


class _Membership:
    """A member of ORG: the admin-plane membership row _require_org_admin reads."""

    def acquire(self):
        return _MembershipCtx()


class _MembershipConn:
    async def execute_core(self, _stmt):
        return types.SimpleNamespace(fetchone=lambda: (ORG,))


class _MembershipCtx:
    async def __aenter__(self):
        return _MembershipConn()

    async def __aexit__(self, *exc):
        return False


def _stop(*_a, **_k):
    raise RuntimeError("past the gate")


@pytest.fixture(autouse=True)
def _state(monkeypatch):
    monkeypatch.setattr(appmod.state, "roles", _ROLES, raising=False)
    monkeypatch.setattr(inv, "_pool", lambda _request: _Membership())
    monkeypatch.setattr(orgs_router, "_admin_pool", _stop)
    monkeypatch.setattr(users_router, "_admin_pool", _stop)
    monkeypatch.setattr(users_router, "_pool", _stop)


def _refused_for_org_admin(
    err: pytest.ExceptionInfo[ApiError], *, granter_held_user_management: bool = True
) -> None:
    assert err.value.status_code == 403
    assert err.value.code == "platform.right_required"
    # What the caller lacks of org_admin is named; user_management only when they lack it too.
    assert ("user_management" in str(err.value.detail)) is not granter_held_user_management


# --- the rule every grant path asks ---------------------------------------------------------


def test_user_management_alone_cannot_grant_org_admin():
    from provisa.security.rights import OrgAdminGrantError

    with pytest.raises(OrgAdminGrantError) as err:
        check_role_grant(ORG_ADMIN_ROLE, ORG_ADMIN_RIGHTS, {"user_management"})
    assert "user_management" not in err.value.missing
    assert err.value.missing


def test_an_org_admin_grants_org_admin():
    check_role_grant(ORG_ADMIN_ROLE, ORG_ADMIN_RIGHTS, set(ORG_ADMIN_RIGHTS))


def test_the_platform_appoints_org_admins():
    check_role_grant(ORG_ADMIN_ROLE, ORG_ADMIN_RIGHTS, DEPLOYMENT_GRANTER)


def test_user_management_still_grants_other_roles():
    check_role_grant("analyst", sorted(STANDARD_ROLE_RIGHTS["analyst"]), {"user_management"})


def test_the_rule_reads_rights_not_the_role_name():
    # REQ-1337: an org's own role carrying org_admin's rights is an org administrator's role too.
    from provisa.security.rights import OrgAdminGrantError

    with pytest.raises(OrgAdminGrantError):
        check_role_grant("chief", ORG_ADMIN_RIGHTS, {"user_management"})
    # A role carrying no more than the granter holds is theirs to confer.
    check_role_grant("people_admin", ["user_management"], {"user_management"})


# --- DELETE /admin/orgs/{org_id} -------------------------------------------------------------


async def test_user_management_alone_cannot_delete_the_org():
    with pytest.raises(ApiError) as err:
        await orgs_router.delete_org(ORG, _request("alice", "people_admin"), confirm=ORG)
    _refused_for_org_admin(err)


async def test_an_org_admin_deletes_the_org():
    with pytest.raises(RuntimeError, match="past the gate"):
        await orgs_router.delete_org(ORG, _request("alice", ORG_ADMIN_ROLE), confirm=ORG)


async def test_the_platform_deletes_an_org():
    with pytest.raises(RuntimeError, match="past the gate"):
        await orgs_router.delete_org(
            ORG, _request("p", "platform_admin", active_org=None), confirm=ORG
        )


# --- POST /admin/orgs/{org_id}/admins/{user_id} ----------------------------------------------


async def test_user_management_alone_cannot_grant_org_admin_by_the_route():
    with pytest.raises(ApiError) as err:
        await orgs_router.grant_org_admin_role(ORG, "bob", _request("alice", "people_admin"))
    _refused_for_org_admin(err)


async def test_an_org_admin_grants_org_admin_by_the_route():
    with pytest.raises(RuntimeError, match="past the gate"):
        await orgs_router.grant_org_admin_role(ORG, "bob", _request("alice", ORG_ADMIN_ROLE))


# --- the users surface: create, update, add an assignment ------------------------------------


async def test_user_management_alone_cannot_grant_org_admin_on_the_users_surface():
    req = _request("alice", "people_admin")
    calls = [
        users_router.add_assignment(
            "u1", users_router.AssignmentBody(role_id=ORG_ADMIN_ROLE, domain_id="*"), req
        ),
        users_router.update_user("u1", users_router.UpdateUserBody(roles=[ORG_ADMIN_ROLE]), req),
        users_router.create_user(
            users_router.CreateUserBody(username="x", password="y", roles=[ORG_ADMIN_ROLE]), req
        ),
    ]
    for call in calls:
        with pytest.raises(ApiError) as err:
            await call
        _refused_for_org_admin(err)


async def test_an_org_admin_grants_org_admin_on_the_users_surface():
    with pytest.raises(RuntimeError, match="past the gate"):
        await users_router.add_assignment(
            "u1",
            users_router.AssignmentBody(role_id=ORG_ADMIN_ROLE, domain_id="*"),
            _request("alice", ORG_ADMIN_ROLE),
        )


# --- an invitation naming org_admin -----------------------------------------------------------


@pytest.fixture
def org_roles(monkeypatch):
    async def _rows(_org_id):
        return [
            {"id": rid, "capabilities": r["capabilities"], "parents": []}
            for rid, r in _ROLES.items()
        ]

    monkeypatch.setattr(inv, "_org_role_rows", _rows)
    monkeypatch.setattr(appmod.state, "org_id", "root", raising=False)


async def test_user_management_alone_cannot_invite_an_org_admin(org_roles):
    with pytest.raises(ApiError) as err:
        await inv.resolve_invite_role(ORG, ORG_ADMIN_ROLE, granter_capabilities={"user_management"})
    _refused_for_org_admin(err)


async def test_an_org_admin_invites_an_org_admin(org_roles):
    resolved = await inv.resolve_invite_role(
        ORG, ORG_ADMIN_ROLE, granter_capabilities=set(ORG_ADMIN_RIGHTS)
    )
    assert resolved == ORG_ADMIN_ROLE


# --- a data product offer conferring org_admin -----------------------------------------------


def test_user_management_alone_cannot_offer_org_admin(monkeypatch):
    monkeypatch.setattr(offers_router, "reaches_role", lambda _s, _r, _c: True)
    refusal = offers_router._role_grant_refusal(
        appmod.state, ORG_ADMIN_ROLE, frozenset({"user_management"})
    )
    assert refusal is not None and "org_admin" in refusal


def test_an_org_admin_offers_org_admin(monkeypatch):
    monkeypatch.setattr(offers_router, "reaches_role", lambda _s, _r, _c: True)
    assert (
        offers_router._role_grant_refusal(appmod.state, ORG_ADMIN_ROLE, frozenset(ORG_ADMIN_RIGHTS))
        is None
    )


# --- the auto-join role -----------------------------------------------------------------------


def test_org_admin_is_never_an_auto_join_role(monkeypatch):
    monkeypatch.setattr(appmod, "state", types.SimpleNamespace(platform_roles=_ROLES))
    # An auto-join grants with no rights at all (auto_join.join_org_automatically), so it can never
    # confer org_admin; the setting is refused where it is written.
    with pytest.raises(ApiError) as err:
        orgs_router._validate_org_policy(r".*@acme\.example$", True, ORG_ADMIN_ROLE)
    _refused_for_org_admin(err, granter_held_user_management=False)
