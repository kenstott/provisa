# Copyright (c) 2026 Kenneth Stott
# Canary: 2e8c4a7b-9f31-4d05-b6ac-1d7e83f0c592
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1337: tenancy decides which role HOLDS a right; it never decides what a gate tests.

``apply_tenancy_role_grants`` is the one place the deployment's tenancy mode reaches the role rows.
Every surface downstream — the settings router, the admin nav, the admin routes — asks only whether
the caller holds ``platform_settings`` or ``cross_org``, so what this function writes is what an
org administrator can actually reach.

Driven against a live Postgres schema built by the real ``init_schema``, so the real seed SQL and
the real UPDATE statements run.
"""

from __future__ import annotations

import os

import pytest
from sqlalchemy import text

from provisa.core.database import Database, create_engine_from_url
from provisa.core.db import apply_tenancy_role_grants, init_schema

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

_PG_HOST = os.environ.get("PG_HOST", "localhost")
_PG_PORT = os.environ.get("PG_PORT", "5432")
_ASYNC_URL = f"postgresql+asyncpg://provisa:provisa@{_PG_HOST}:{_PG_PORT}/provisa"

_ORG_ID = "req1337"
_SCHEMA = f"org_{_ORG_ID}"

_SCHEMA_SQL = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "provisa", "core", "schema.sql")
)


@pytest.fixture
async def tenant_db():
    engine = create_engine_from_url(_ASYNC_URL)
    db = Database(engine, name="tenant", search_path=_SCHEMA)
    async with db.acquire() as conn:
        await conn.execute(f"DROP SCHEMA IF EXISTS {_SCHEMA} CASCADE")
        await conn.execute(f"DROP SCHEMA IF EXISTS {_SCHEMA}_mv_cache CASCADE")
    with open(_SCHEMA_SQL, encoding="utf-8") as fh:
        await init_schema(db, fh.read(), org_id=_ORG_ID)
    yield db
    async with db.acquire() as conn:
        await conn.execute(f"DROP SCHEMA IF EXISTS {_SCHEMA} CASCADE")
        await conn.execute(f"DROP SCHEMA IF EXISTS {_SCHEMA}_mv_cache CASCADE")
    await engine.dispose()


async def _caps(db: Database) -> dict[str, set[str]]:
    async with db.acquire() as conn:
        result = await conn.execute_core(
            text("SELECT id, capabilities FROM roles WHERE org_id IS NULL")
        )
        rows = result.fetchall()
    return {r[0]: set(r[1] or []) for r in rows}


async def test_multitenant_withdraws_platform_settings_from_org_admin(tenant_db):
    # The deployment-wide settings surface — federation engine, cache storage, encryption and auth
    # providers — belongs to the platform operator. An org administers its own data plane only.
    await apply_tenancy_role_grants(tenant_db, _ORG_ID, multitenancy=True)

    caps = await _caps(tenant_db)
    assert "platform_settings" not in caps["org_admin"]
    assert "platform_settings" in caps["platform_admin"]


async def test_single_tenant_grants_platform_settings_to_org_admin(tenant_db):
    # In a single-tenant deployment the org administrator IS the deployment operator, so the same
    # gate admits them — because the SEED changed, not because the gate did.
    await apply_tenancy_role_grants(tenant_db, _ORG_ID, multitenancy=False)

    caps = await _caps(tenant_db)
    assert "platform_settings" in caps["org_admin"]
    assert "platform_settings" in caps["platform_admin"]


async def test_flipping_to_multitenant_withdraws_a_stale_grant(tenant_db):
    # A deployment started single-tenant and later flipped must not leave org_admin holding a right
    # the new mode denies. The function asserts the mode's grant rather than adding to it.
    await apply_tenancy_role_grants(tenant_db, _ORG_ID, multitenancy=False)
    assert "platform_settings" in (await _caps(tenant_db))["org_admin"]

    await apply_tenancy_role_grants(tenant_db, _ORG_ID, multitenancy=True)

    assert "platform_settings" not in (await _caps(tenant_db))["org_admin"]


async def test_flipping_to_single_tenant_restores_the_grant(tenant_db):
    await apply_tenancy_role_grants(tenant_db, _ORG_ID, multitenancy=True)

    await apply_tenancy_role_grants(tenant_db, _ORG_ID, multitenancy=False)

    assert "platform_settings" in (await _caps(tenant_db))["org_admin"]


@pytest.mark.parametrize("multitenancy", [True, False])
async def test_only_platform_admin_holds_cross_org_in_either_mode(tenant_db, multitenancy):
    # cross_org is what marks a role CONTROL-PLANE: REQ-1327 keeps such a role off the data plane
    # and REQ-1297 makes it unresolvable in a tenant org. Org authority is confined to the org being
    # acted in, so no tenancy mode ever confers it on org_admin.
    await apply_tenancy_role_grants(tenant_db, _ORG_ID, multitenancy=multitenancy)

    caps = await _caps(tenant_db)
    assert caps["platform_admin"] >= {"cross_org", "platform_settings", "admin", "superadmin"}
    for role_id in ("org_admin", "developer", "analyst"):
        assert "cross_org" not in caps[role_id], role_id


@pytest.mark.parametrize("multitenancy", [True, False])
async def test_a_stale_cross_org_grant_is_withdrawn(tenant_db, multitenancy):
    # A role that acquired cross_org from an older seed or a hand-edited config would silently read
    # as control-plane everywhere. Both modes strip it from every role but platform_admin.
    async with tenant_db.acquire() as conn:
        await conn.execute_core(
            text(
                "UPDATE roles SET capabilities = capabilities || '[\"cross_org\"]'::jsonb "
                "WHERE id = 'org_admin'"
            )
        )

    await apply_tenancy_role_grants(tenant_db, _ORG_ID, multitenancy=multitenancy)

    caps = await _caps(tenant_db)
    assert "cross_org" not in caps["org_admin"]
    # The withdrawal rebuilds the array; nothing else org_admin holds may be lost with it.
    assert {"user_management", "source_registration", "access_config"} <= caps["org_admin"]


@pytest.mark.parametrize("multitenancy", [True, False])
async def test_the_grant_is_idempotent(tenant_db, multitenancy):
    # init_schema calls this on every startup and every org provision.
    await apply_tenancy_role_grants(tenant_db, _ORG_ID, multitenancy=multitenancy)
    once = await _caps(tenant_db)

    await apply_tenancy_role_grants(tenant_db, _ORG_ID, multitenancy=multitenancy)
    await apply_tenancy_role_grants(tenant_db, _ORG_ID, multitenancy=multitenancy)

    assert await _caps(tenant_db) == once


async def test_no_other_role_gains_platform_settings(tenant_db):
    # The single-tenant grant is scoped to org_admin: developer and analyst never reach the
    # deployment-wide settings surface in either mode.
    await apply_tenancy_role_grants(tenant_db, _ORG_ID, multitenancy=False)

    caps = await _caps(tenant_db)
    for role_id in ("developer", "analyst"):
        assert "platform_settings" not in caps[role_id], role_id
