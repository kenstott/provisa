# Copyright (c) 2026 Kenneth Stott
# Canary: 5a41c9e2-77b0-4d16-9c83-2f6e0b1d4a77
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1297: every org schema seeds exactly four system roles, and the retired ids are gone.

The four ids — platform_admin, org_admin, developer, analyst — are the whole role vocabulary.
'admin' and 'superadmin' survive only as CAPABILITY strings; as ROLE ids they are retired, and an
assignment naming one is rewritten to platform_admin the next time the seed runs over the schema.

Driven against a live Postgres schema built by the real ``init_schema`` so the real seed SQL runs.
"""

from __future__ import annotations

import os

import pytest
from sqlalchemy import text

from provisa.core.database import Database, create_engine_from_url
from provisa.core.db import init_schema

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

_PG_HOST = os.environ.get("PG_HOST", "localhost")
_PG_PORT = os.environ.get("PG_PORT", "5432")
_ASYNC_URL = f"postgresql+asyncpg://provisa:provisa@{_PG_HOST}:{_PG_PORT}/provisa"

_ORG_ID = "req1297"
_SCHEMA = f"org_{_ORG_ID}"

_SCHEMA_SQL = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "provisa", "core", "schema.sql")
)


async def _seed(tenant_db: Database) -> None:
    with open(_SCHEMA_SQL, encoding="utf-8") as fh:
        await init_schema(tenant_db, fh.read(), org_id=_ORG_ID)


@pytest.fixture
async def tenant_db():
    engine = create_engine_from_url(_ASYNC_URL)
    db = Database(engine, name="tenant", search_path=_SCHEMA)
    async with db.acquire() as conn:
        await conn.execute(f"DROP SCHEMA IF EXISTS {_SCHEMA} CASCADE")
        await conn.execute(f"DROP SCHEMA IF EXISTS {_SCHEMA}_mv_cache CASCADE")
    await _seed(db)
    yield db
    async with db.acquire() as conn:
        await conn.execute(f"DROP SCHEMA IF EXISTS {_SCHEMA} CASCADE")
        await conn.execute(f"DROP SCHEMA IF EXISTS {_SCHEMA}_mv_cache CASCADE")
    await engine.dispose()


async def _system_roles(db: Database) -> dict[str, list[str]]:
    async with db.acquire() as conn:
        result = await conn.execute_core(
            text("SELECT id, capabilities FROM roles WHERE org_id IS NULL ORDER BY id")
        )
        rows = result.fetchall()
    out: dict[str, list[str]] = {}
    for row in rows:
        caps = row[1]
        out[row[0]] = list(caps) if isinstance(caps, list) else []
    return out


async def test_the_five_system_roles_and_nothing_else_are_seeded(tenant_db):
    seeded = await _system_roles(tenant_db)

    assert set(seeded) == {"platform_admin", "org_admin", "developer", "analyst", "modeler"}


async def test_only_platform_admin_carries_the_bypass_capabilities(tenant_db):
    seeded = await _system_roles(tenant_db)

    assert {"admin", "superadmin"} <= set(seeded["platform_admin"])
    for role_id in ("org_admin", "developer", "analyst", "modeler"):
        assert not ({"admin", "superadmin"} & set(seeded[role_id])), role_id


async def test_modeler_is_the_only_system_role_that_may_join_outside_the_catalog(tenant_db):
    """ignore_relationships is the discovery knob and belongs to exactly one seeded role.

    A role holding it can join relations the approved relationship catalog does not cover — that is
    how a model is determined. Enforcing the model means querying as a role WITHOUT it, so every
    other default (analyst included) must not carry it.
    """
    seeded = await _system_roles(tenant_db)

    holders = {rid for rid, caps in seeded.items() if "ignore_relationships" in caps}
    assert holders == {"modeler"}


async def test_the_tenant_roles_hold_the_authority_the_requirement_names(tenant_db):
    seeded = await _system_roles(tenant_db)

    # org_admin administers one org: members, invites, sources, governance.
    assert {"user_management", "source_registration", "masking_config", "access_config"} <= set(
        seeded["org_admin"]
    )
    # developer builds against the data.
    assert {"query_development", "create_view", "create_relationship", "full_results", "write"} <= (
        set(seeded["developer"])
    )
    # analyst reads: no authoring, no governance, no member management.
    assert set(seeded["analyst"]) == {"usage", "query_development"}
    assert not ({"create_view", "user_management", "write"} & set(seeded["analyst"]))


async def test_retired_role_ids_are_absent_from_a_freshly_seeded_schema(tenant_db):
    async with tenant_db.acquire() as conn:
        result = await conn.execute_core(
            text("SELECT id FROM roles WHERE id IN ('admin', 'superadmin')")
        )
        assert result.fetchall() == []


async def test_an_assignment_naming_a_retired_id_is_rewritten_to_platform_admin(tenant_db):
    """A deployment seeded before REQ-1297 holds roles rows named admin/superadmin and assignments
    pointing at them. Re-running the seed must carry those users over to platform_admin, not orphan
    them — and must not trip the (user_id, role_id, domain_id) uniqueness for a user who already
    holds platform_admin on the same domain."""
    async with tenant_db.acquire() as conn:
        await conn.execute_core(
            text(
                "INSERT INTO roles (id, capabilities, domain_access, org_id) "
                "VALUES ('admin', '[\"admin\"]'::jsonb, '[\"*\"]'::jsonb, NULL), "
                "('superadmin', '[\"superadmin\"]'::jsonb, '[\"*\"]'::jsonb, NULL)"
            )
        )
        await conn.execute_core(
            text(
                "INSERT INTO user_role_assignments (user_id, role_id, domain_id) VALUES "
                "('legacy-admin', 'admin', '*'), "
                "('legacy-super', 'superadmin', 'sales'), "
                "('already-platform', 'admin', '*'), "
                "('already-platform', 'platform_admin', '*')"
            )
        )

    await _seed(tenant_db)

    async with tenant_db.acquire() as conn:
        result = await conn.execute_core(
            text("SELECT user_id, role_id, domain_id FROM user_role_assignments ORDER BY user_id")
        )
        rows = [tuple(r) for r in result.fetchall()]
        remaining = await conn.execute_core(
            text("SELECT id FROM roles WHERE id IN ('admin', 'superadmin')")
        )

    assert rows == [
        ("already-platform", "platform_admin", "*"),
        ("legacy-admin", "platform_admin", "*"),
        ("legacy-super", "platform_admin", "sales"),
    ]
    assert remaining.fetchall() == []


async def test_reseeding_leaves_the_four_roles_untouched(tenant_db):
    """init_schema runs on every startup and on every org provision; the seed is idempotent."""
    before = await _system_roles(tenant_db)

    await _seed(tenant_db)

    assert await _system_roles(tenant_db) == before
