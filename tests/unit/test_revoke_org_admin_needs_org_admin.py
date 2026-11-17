# Copyright (c) 2026 Kenneth Stott
# Canary: 5b0f7a2e-6c1d-4e8a-9f3b-2d7c41e90a68
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1327 (amended 2026-11-16): revoking org_admin needs org_admin, as granting it does.

A holder of user_management alone cannot remove an organization's administrators; the platform
administrator keeps the audited recovery. Every door that takes org_admin away is asked here: the
revoke route, the users surface (an assignment removed, the roles list rewritten, the person
removed from the org), the org's member removal, and an entitlement rule ending the grant.
"""

from __future__ import annotations

import types
from datetime import UTC, datetime, timedelta
from typing import cast

import pytest
from fastapi import Request

import provisa.api.app as appmod
import provisa.core.org_membership as membership
from provisa.api.admin import invites_router as inv
from provisa.api.admin import local_users_router as users_router
from provisa.api.admin import orgs_router
from provisa.api.errors import ApiError
from provisa.core.db import STANDARD_ROLE_RIGHTS
from provisa.security.rights import ORG_ADMIN_ROLE, Capability
from tests.unit.test_entitlements_api import _hold, _identity
from tests.unit.test_entitlements_api import api as api  # noqa: F401  (fixture)

ORG = "acme"
ORG_ADMIN_RIGHTS = sorted(STANDARD_ROLE_RIGHTS[ORG_ADMIN_ROLE])

_ROLES = {
    ORG_ADMIN_ROLE: {"capabilities": ORG_ADMIN_RIGHTS, "domain_access": ["*"]},
    "people_admin": {"capabilities": ["user_management"], "domain_access": ["*"]},
    "analyst": {"capabilities": ["query"], "domain_access": ["*"]},
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


class _Db:
    """A database whose first ``len(rows)`` statements answer ``rows`` in turn; any statement
    after them is the act itself, past every gate."""

    def __init__(self, *rows: list[tuple]):
        self._rows = list(rows)

    def acquire(self):
        return _Ctx(self)


class _Ctx:
    def __init__(self, db: _Db):
        self._db = db

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def execute_core(self, _stmt):
        if not self._db._rows:
            raise RuntimeError("past the gate")
        rows = self._db._rows.pop(0)
        return types.SimpleNamespace(
            fetchone=lambda: rows[0] if rows else None, fetchall=lambda: rows
        )


def _stop(*_a, **_k):
    raise RuntimeError("past the gate")


async def _astop(*_a, **_k):
    raise RuntimeError("past the gate")


@pytest.fixture
def org(monkeypatch):
    monkeypatch.setattr(appmod.state, "roles", _ROLES, raising=False)
    monkeypatch.setattr(inv, "_pool", lambda _request: _Db([(ORG,)]))
    monkeypatch.setattr(orgs_router, "_admin_pool", _stop)
    monkeypatch.setattr(users_router, "_admin_pool", _stop)
    monkeypatch.setattr(users_router, "_pool", _stop)
    monkeypatch.setattr(membership, "assert_not_last_org_admin", _astop)


def _model_db(monkeypatch, held: list[str]) -> None:
    """The org's model database: ``held`` are the roles the subject holds there."""

    async def _db(_org_id):
        return _Db([(r,) for r in held])

    monkeypatch.setattr(orgs_router, "_org_model_db", _db)


def _refused_for_org_admin(err: pytest.ExceptionInfo[ApiError]) -> None:
    assert err.value.status_code == 403
    assert err.value.code == "platform.right_required"
    # The rights of org_admin the caller lacks are named; user_management, which they hold, is not.
    named = str(err.value.params["right"])
    assert named and "user_management" not in named


# --- DELETE /admin/orgs/{org_id}/admins/{user_id} --------------------------------------------


async def test_user_management_alone_cannot_revoke_org_admin(org, monkeypatch):
    monkeypatch.setattr(orgs_router, "_org_model_db", _astop)
    with pytest.raises(ApiError) as err:
        await orgs_router.revoke_org_admin_role(ORG, "bob", _request("alice", "people_admin"))
    _refused_for_org_admin(err)


async def test_an_org_admin_revokes_org_admin(org, monkeypatch):
    monkeypatch.setattr(orgs_router, "_org_model_db", _astop)
    with pytest.raises(RuntimeError, match="past the gate"):
        await orgs_router.revoke_org_admin_role(ORG, "bob", _request("alice", ORG_ADMIN_ROLE))


async def test_the_platform_keeps_the_audited_recovery(org, monkeypatch):
    monkeypatch.setattr(orgs_router, "_org_model_db", _astop)
    with pytest.raises(RuntimeError, match="past the gate"):
        await orgs_router.revoke_org_admin_role(
            ORG, "bob", _request("p", "platform_admin", active_org=None)
        )


# --- the users surface: removing an assignment -----------------------------------------------


async def test_user_management_alone_cannot_remove_an_org_admin_assignment(org, monkeypatch):
    monkeypatch.setattr(users_router, "_pool", lambda _r: _Db([(ORG_ADMIN_ROLE,)]))
    with pytest.raises(ApiError) as err:
        await users_router.remove_assignment("bob", 7, _request("alice", "people_admin"))
    _refused_for_org_admin(err)


async def test_an_org_admin_removes_an_org_admin_assignment(org, monkeypatch):
    monkeypatch.setattr(users_router, "_pool", lambda _r: _Db([(ORG_ADMIN_ROLE,)]))
    with pytest.raises(RuntimeError, match="past the gate"):
        await users_router.remove_assignment("bob", 7, _request("alice", ORG_ADMIN_ROLE))


# --- the users surface: rewriting the roles list ---------------------------------------------


async def test_user_management_alone_cannot_drop_org_admin_from_the_roles_list(org, monkeypatch):
    monkeypatch.setattr(
        users_router, "_admin_pool", lambda _r: _Db([([ORG_ADMIN_ROLE, "analyst"],)])
    )
    with pytest.raises(ApiError) as err:
        await users_router.update_user(
            "bob", users_router.UpdateUserBody(roles=["analyst"]), _request("alice", "people_admin")
        )
    _refused_for_org_admin(err)


async def test_user_management_rewrites_a_roles_list_holding_no_org_admin(org, monkeypatch):
    monkeypatch.setattr(users_router, "_admin_pool", lambda _r: _Db([(["analyst"],)]))
    with pytest.raises(RuntimeError, match="past the gate"):
        await users_router.update_user(
            "bob", users_router.UpdateUserBody(roles=[]), _request("alice", "people_admin")
        )


async def test_an_org_admin_drops_org_admin_from_the_roles_list(org, monkeypatch):
    monkeypatch.setattr(
        users_router, "_admin_pool", lambda _r: _Db([([ORG_ADMIN_ROLE, "analyst"],)])
    )
    with pytest.raises(RuntimeError, match="past the gate"):
        await users_router.update_user(
            "bob", users_router.UpdateUserBody(roles=["analyst"]), _request("alice", ORG_ADMIN_ROLE)
        )


# --- removing the person from the org: both surfaces ------------------------------------------


async def test_user_management_alone_cannot_remove_an_org_admin_member(org, monkeypatch):
    _model_db(monkeypatch, [ORG_ADMIN_ROLE, "analyst"])
    with pytest.raises(ApiError) as err:
        await orgs_router.remove_member(ORG, "bob", _request("alice", "people_admin"))
    _refused_for_org_admin(err)


async def test_user_management_removes_a_member_who_is_no_org_admin(org, monkeypatch):
    _model_db(monkeypatch, ["analyst"])
    with pytest.raises(RuntimeError, match="past the gate"):
        await orgs_router.remove_member(ORG, "bob", _request("alice", "people_admin"))


async def test_an_org_admin_removes_an_org_admin_member(org, monkeypatch):
    _model_db(monkeypatch, [ORG_ADMIN_ROLE])
    with pytest.raises(RuntimeError, match="past the gate"):
        await orgs_router.remove_member(ORG, "bob", _request("alice", ORG_ADMIN_ROLE))


async def test_user_management_alone_cannot_remove_an_org_admin_user_from_the_org(org, monkeypatch):
    _model_db(monkeypatch, [ORG_ADMIN_ROLE])
    # delete_user takes the admin pool before it decides; any statement on it is past the gate.
    monkeypatch.setattr(users_router, "_admin_pool", lambda _r: _Db())
    with pytest.raises(ApiError) as err:
        await users_router.delete_user("bob", _request("alice", "people_admin"))
    _refused_for_org_admin(err)


async def test_an_org_admin_removes_an_org_admin_user_from_the_org(org, monkeypatch):
    _model_db(monkeypatch, [ORG_ADMIN_ROLE])
    monkeypatch.setattr(users_router, "_admin_pool", lambda _r: _Db())
    with pytest.raises(RuntimeError, match="past the gate"):
        await users_router.delete_user("bob", _request("alice", ORG_ADMIN_ROLE))


# --- an entitlement rule ending the org_admin grant -------------------------------------------

_ASSIGN = Capability.ASSIGN_ENTITLEMENTS.value
_TOMORROW = (datetime.now(UTC) + timedelta(days=1)).isoformat()


def _org_admin_role(api_, *, granter_rights: list[str]) -> None:
    api_.state.roles[ORG_ADMIN_ROLE] = {
        "id": ORG_ADMIN_ROLE,
        "capabilities": ORG_ADMIN_RIGHTS,
        "parents": [],
        "domain_access": ["*"],
    }
    api_.state.role_chains[ORG_ADMIN_ROLE] = [ORG_ADMIN_ROLE]
    api_.state.roles["granter"]["capabilities"] = granter_rights


def _ends_grant(**over) -> dict:
    return {
        "principal": "bob",
        "role_id": ORG_ADMIN_ROLE,
        "ends_at": _TOMORROW,
        "ends": {},
        "reason": "leaving",
        **over,
    }


async def test_assign_entitlements_alone_cannot_end_an_org_admin_grant(api):
    _org_admin_role(api, granter_rights=[_ASSIGN, "user_management"])
    await _hold(api, "bob", role=ORG_ADMIN_ROLE)
    response = await api.client.post("/admin/entitlements/expiries", json=_ends_grant())
    assert response.status_code == 403, response.text
    assert response.json()["code"] == "platform.right_required"


async def test_an_org_admin_ends_an_org_admin_grant(api):
    _org_admin_role(api, granter_rights=sorted({_ASSIGN, *ORG_ADMIN_RIGHTS}))
    api.caller["identity"] = _identity()
    await _hold(api, "bob", role=ORG_ADMIN_ROLE)
    response = await api.client.post("/admin/entitlements/expiries", json=_ends_grant())
    assert response.status_code == 200, response.text
