# Copyright (c) 2026 Kenneth Stott
# Canary: 4e7a2c91-6b05-4f38-a1d9-8c3e5f7b2a64
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A REST create of a role or a local user whose id or username is taken is refused by name
(409), before anything is written. It used to reach the database's unique key and answer a 500.
"""

# Requirements: REQ-042, REQ-124

from __future__ import annotations

import types
from typing import Any

import pytest
from sqlalchemy import func, insert, select

from provisa.api.admin import local_users_router, roles_router
from provisa.api.errors import ApiError
from provisa.core.database import Database
from provisa.core.schema_admin import local_users
from provisa.core.schema_org import roles


def _request() -> Any:
    return types.SimpleNamespace(state=types.SimpleNamespace(identity=None, active_org_id="a"))


async def _count(db: Database, table) -> int:
    async with db.acquire() as conn:
        return (await conn.execute_core(select(func.count()).select_from(table))).scalar_one()


async def test_a_role_id_that_exists_is_refused_by_name(monkeypatch, control_plane):
    plane = control_plane.org()
    async with plane.acquire() as conn:
        await conn.execute_core(
            insert(roles).values(id="auditor", capabilities=[], domain_access=["*"], org_id="a")
        )
    monkeypatch.setattr(roles_router, "require_capability_request", lambda _r, _c: None)
    monkeypatch.setattr(roles_router, "_pool", lambda _r: plane)

    async def _valid(*_args, **_kwargs):
        return None

    # The definition is valid and the caller may grant it (its checks are tested where they are
    # made): what is left to refuse is the id.
    for check in ("_check_parent", "_check_definition", "_require_reach_of_added"):
        monkeypatch.setattr(roles_router, check, _valid)
    monkeypatch.setattr(roles_router.role_repo, "require_residency_grant_saved", _valid)
    before = await _count(plane, roles)
    body = roles_router.CreateRoleBody(
        id="auditor", description="Test role.", capabilities=[], domain_access=["sales"]
    )
    with pytest.raises(ApiError) as err:
        await roles_router.create_role(body, _request())
    assert (err.value.status_code, err.value.code, err.value.params) == (
        409,
        "roles.already_exists",
        {"role": "auditor"},
    )
    assert await _count(plane, roles) == before


async def test_a_username_that_is_taken_is_refused_by_name(monkeypatch, control_plane):
    platform = control_plane.platform
    async with platform.acquire() as conn:
        await conn.execute_core(
            insert(local_users).values(
                id="u1", username="ada", password_hash="x", roles=[], attributes={}
            )
        )
    monkeypatch.setattr(local_users_router, "_require_user_management", lambda _r: None)
    monkeypatch.setattr(local_users_router, "_refuse_platform_role", lambda _r, _roles: None)
    monkeypatch.setattr(local_users_router, "_admin_pool", lambda _r: platform)
    before = await _count(platform, local_users)
    body = local_users_router.CreateUserBody(username="ada", password="secret")
    with pytest.raises(ApiError) as err:
        await local_users_router.create_user(body, _request())
    assert (err.value.status_code, err.value.code, err.value.params) == (
        409,
        "users.username_taken",
        {"username": "ada"},
    )
    assert await _count(platform, local_users) == before
