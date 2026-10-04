# Copyright (c) 2026 Kenneth Stott
# Canary: 4d1b7a52-0c3e-4f86-9a17-6e2f8b3c5d90
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1313 / REQ-1314: the role an invitation confers is defaulted and validated at issue."""

from __future__ import annotations

import types

import pytest

from provisa.api.admin.invites_router import DEFAULT_INVITE_ROLE, resolve_invite_role
from provisa.api.errors import ApiError
from provisa.security.rights import Capability

ROLES = [
    {"id": "analyst", "capabilities": ["query_development"], "parent_role_id": None},
    {"id": "org_admin", "capabilities": ["user_management"], "parent_role_id": None},
    {
        "id": "platform_admin",
        "capabilities": [Capability.PLATFORM_SETTINGS.value, Capability.CROSS_ORG.value],
        "parent_role_id": None,
    },
]


class _Result:
    def fetchall(self):
        return [types.SimpleNamespace(_mapping=r) for r in ROLES]


class _Conn:
    async def execute_core(self, stmt):
        return _Result()


class _Db:
    def acquire(self):
        class _Ctx:
            async def __aenter__(self_inner):
                return _Conn()

            async def __aexit__(self_inner, *exc):
                return False

        return _Ctx()


@pytest.fixture(autouse=True)
def runtime(monkeypatch):
    async def _ensure_org_runtime(org_id):
        return types.SimpleNamespace(model_db=_Db())

    monkeypatch.setattr("provisa.api.app.ensure_org_runtime", _ensure_org_runtime, raising=False)
    monkeypatch.setattr(
        "provisa.api.app.state", types.SimpleNamespace(org_id="root"), raising=False
    )


async def test_a_role_less_invitation_confers_analyst():  # REQ-1314
    assert DEFAULT_INVITE_ROLE == "analyst"
    assert await resolve_invite_role("acme", None, granter_capabilities=set()) == "analyst"


async def test_a_role_in_the_target_org_is_conferred_as_named():  # REQ-1313
    assert await resolve_invite_role("acme", "org_admin", granter_capabilities=set()) == "org_admin"


async def test_a_role_the_org_does_not_have_is_refused_not_substituted():  # REQ-1313
    with pytest.raises(ApiError) as err:
        await resolve_invite_role("acme", "nonexistent", granter_capabilities=set())
    assert err.value.status_code == 422
    assert err.value.code == "invites.role_not_in_org"


async def test_a_platform_role_is_refused_outside_the_root_org():  # REQ-1313
    with pytest.raises(ApiError) as err:
        await resolve_invite_role(
            "acme",
            "platform_admin",
            granter_capabilities={Capability.PLATFORM_SETTINGS.value, Capability.CROSS_ORG.value},
        )
    assert err.value.status_code == 403
    assert err.value.code == "invites.platform_admin_root_only"


async def test_a_platform_role_is_refused_to_an_inviter_who_does_not_hold_it():  # REQ-1313
    with pytest.raises(ApiError) as err:
        await resolve_invite_role(
            "root", "platform_admin", granter_capabilities={"user_management"}
        )
    assert err.value.status_code == 403
    assert err.value.code == "users.platform_role_requires_platform_admin"
