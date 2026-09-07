# Copyright (c) 2026 Kenneth Stott
# Canary: 5a7c1e9b-2d4f-4a3e-9b6c-8e1f2a5d7c93
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-609/REQ-1634: resolveOwners resolves an owner_role/steward/visible_to ref list to the
individuals it grants to, against a live tenant schema (roles, user_role_assignments,
user_directory)."""

from __future__ import annotations

import os
from types import SimpleNamespace

import pytest
from sqlalchemy import insert

from provisa.core.database import Database, create_engine_from_url
from provisa.core.db import init_schema
from provisa.core.schema_org import roles as roles_t
from provisa.core.schema_org import user_directory as user_directory_t
from provisa.core.schema_org import user_role_assignments as user_role_assignments_t

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

_PG_HOST = os.environ.get("PG_HOST", "localhost")
_PG_PORT = os.environ.get("PG_PORT", "5432")
_ASYNC_URL = f"postgresql+asyncpg://provisa:provisa@{_PG_HOST}:{_PG_PORT}/provisa"

_ORG_ID = "req609resolve"
_SCHEMA = f"org_{_ORG_ID}"

_ORG_ADMIN_CAPS = ["access_config", "user_management"]


@pytest.fixture
async def org_plane(monkeypatch):
    from provisa.api.app import state as app_state

    engine = create_engine_from_url(_ASYNC_URL)
    tenant_db = Database(engine, name="tenant", search_path=_SCHEMA)
    async with tenant_db.acquire() as conn:
        await conn.execute(f"DROP SCHEMA IF EXISTS {_SCHEMA} CASCADE")
        await conn.execute(f"DROP SCHEMA IF EXISTS {_SCHEMA}_mv_cache CASCADE")

    schema_sql = os.path.join(
        os.path.dirname(__file__), "..", "..", "provisa", "core", "schema.sql"
    )
    with open(os.path.abspath(schema_sql), encoding="utf-8") as fh:
        sql = fh.read()
    await init_schema(tenant_db, sql, org_id=_ORG_ID)

    async with tenant_db.acquire() as conn:
        await conn.execute_core(
            insert(roles_t).values(id="data_steward", capabilities=[], domain_access=["*"])
        )
        await conn.execute_core(
            insert(user_directory_t).values(
                user_id="user-alice", display_name="Alice Nguyen", email="alice@example.com"
            )
        )
        await conn.execute_core(
            insert(user_directory_t).values(
                user_id="user-bob", display_name="Bob Ruiz", email="bob@example.com"
            )
        )
        await conn.execute_core(
            insert(user_role_assignments_t).values(
                user_id="user-alice", role_id="data_steward", domain_id="*"
            )
        )
        await conn.execute_core(
            insert(user_role_assignments_t).values(
                user_id="user-bob", role_id="data_steward", domain_id="*"
            )
        )

    monkeypatch.setattr(app_state, "tenant_db", tenant_db, raising=False)
    monkeypatch.setattr(
        app_state,
        "roles",
        {"org_admin": {"capabilities": list(_ORG_ADMIN_CAPS), "domain_access": ["*"]}},
        raising=False,
    )

    yield tenant_db

    async with tenant_db.acquire() as conn:
        await conn.execute(f"DROP SCHEMA IF EXISTS {_SCHEMA} CASCADE")
        await conn.execute(f"DROP SCHEMA IF EXISTS {_SCHEMA}_mv_cache CASCADE")
    await engine.dispose()


def _context(*, active_org_id: str | None, roles: list[str], user_id: str = "member-2"):
    identity = SimpleNamespace(user_id=user_id, roles=roles)
    request = SimpleNamespace(state=SimpleNamespace(active_org_id=active_org_id, identity=identity))
    return {"request": request}


_QUERY = """
query($refs: [String!]!) {
  resolveOwners(refs: $refs) { userId displayName email }
}
"""


async def _run(refs: list[str]) -> dict:
    from provisa.api.admin.schema import admin_schema

    result = await admin_schema.execute(
        _QUERY,
        variable_values={"refs": refs},
        context_value=_context(active_org_id=_ORG_ID, roles=["org_admin:*"]),
    )
    assert result.errors is None, result.errors
    assert result.data is not None
    return result.data


async def test_role_ref_resolves_to_its_members(org_plane):
    data = await _run(["data_steward"])
    by_id = {u["userId"]: u for u in data["resolveOwners"]}
    assert set(by_id) == {"user-alice", "user-bob"}
    assert by_id["user-alice"]["displayName"] == "Alice Nguyen"
    assert by_id["user-alice"]["email"] == "alice@example.com"


async def test_raw_user_id_ref_resolves_directly(org_plane):
    # REQ-609: Domain.steward may be a role or a user id — no role named "user-alice" exists.
    data = await _run(["user-alice"])
    assert [u["userId"] for u in data["resolveOwners"]] == ["user-alice"]


async def test_unknown_ref_is_echoed_back_bare(org_plane):
    data = await _run(["nobody-here"])
    assert data["resolveOwners"] == [{"userId": "nobody-here", "displayName": None, "email": None}]


async def test_refs_are_deduped_across_multiple_roles(org_plane):
    async with org_plane.acquire() as conn:
        await conn.execute_core(
            insert(roles_t).values(id="req609_analyst", capabilities=[], domain_access=["*"])
        )
        await conn.execute_core(
            insert(user_role_assignments_t).values(
                user_id="user-alice", role_id="req609_analyst", domain_id="*"
            )
        )
    data = await _run(["data_steward", "req609_analyst"])
    ids = [u["userId"] for u in data["resolveOwners"]]
    assert ids.count("user-alice") == 1
    assert set(ids) == {"user-alice", "user-bob"}


async def test_empty_refs_returns_empty_list(org_plane):
    data = await _run([])
    assert data["resolveOwners"] == []
