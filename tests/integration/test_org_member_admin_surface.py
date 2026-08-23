# Copyright (c) 2026 Kenneth Stott
# Canary: 9d2c6a41-3f80-4b57-8e19-c05a7b6f2d38
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1293: a self-created org's own org_admin sees that org's admin surface.

Reported failure: after a second user signed in, self-created an org and took the demo,
the app showed "no tables registered" and no ``ops``/``meta`` domains — even though the
org's tenant schema held the full seeded set.

The tenant plane is already isolated BY SCHEMA (``org_<id>``); ``_get_pool()`` returns the
org-routed ``state.tenant_db``, so every row the query can reach already belongs to the
active org. The admin ``domains`` resolver additionally filtered ``domains.org_id ==
active_org_id`` for any caller lacking the platform-bypass ``admin``/``superadmin`` caps —
and ``schema.sql`` stamps ``org_id = 'root'`` on the seeded domain rows of EVERY org schema
it runs in. For an ``org_admin`` of any org but ``root`` that predicate matched zero rows.

This test drives the real resolvers against a live Postgres tenant schema built by the real
``init_schema`` (so the real stamping runs), under an identity holding only ``org_admin``.
"""

from __future__ import annotations

import os
from types import SimpleNamespace

import pytest
from sqlalchemy import insert, select, text

from provisa.core.database import Database, create_engine_from_url
from provisa.core.db import init_schema
from provisa.core.schema_org import domains as domains_t
from provisa.core.schema_org import registered_tables as registered_tables_t
from provisa.core.schema_org import sources as sources_t

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

_PG_HOST = os.environ.get("PG_HOST", "localhost")
_PG_PORT = os.environ.get("PG_PORT", "5432")
_ASYNC_URL = f"postgresql+asyncpg://provisa:provisa@{_PG_HOST}:{_PG_PORT}/provisa"

_ORG_ID = "req1293"
_SCHEMA = f"org_{_ORG_ID}"

# org_admin's caps, as seeded by schema.sql — deliberately WITHOUT admin/superadmin.
_ORG_ADMIN_CAPS = [
    "source_registration",
    "table_registration",
    "create_relationship",
    "create_view",
    "approve_view",
    "approve_relationship",
    "access_config",
    "user_management",
    "masking_config",
    "column_grant",
    "view_governance",
    "query_development",
    "full_results",
    "write",
    "usage",
]


@pytest.fixture
async def org_plane(monkeypatch):
    """A live tenant schema for _ORG_ID, built by the real init_schema, bound as state.tenant_db."""
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

    # A registered table, as the demo seed would leave it.
    async with tenant_db.acquire() as conn:
        await conn.execute_core(
            insert(sources_t).values(id="sales-pg", type="postgres", dialect="postgresql")
        )
        await conn.execute_core(
            insert(registered_tables_t).values(
                source_id="sales-pg",
                schema_name="public",
                table_name="orders",
                domain_id="shelter",
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
    """A GraphQL context whose identity holds exactly `roles` — no platform bypass unless named."""
    identity = SimpleNamespace(user_id=user_id, roles=roles)
    request = SimpleNamespace(state=SimpleNamespace(active_org_id=active_org_id, identity=identity))
    return {"request": request}


_ORG_ADMIN_CTX = dict(active_org_id=_ORG_ID, roles=["org_admin:*"])


async def _run_raw(query: str, **ctx_kwargs):
    from provisa.api.admin.schema import admin_schema

    return await admin_schema.execute(query, context_value=_context(**ctx_kwargs))


async def _run(query: str, **ctx_kwargs) -> dict:
    """Execute and return `data`, failing the test on any GraphQL error."""
    result = await _run_raw(query, **ctx_kwargs)
    assert result.errors is None, result.errors
    assert result.data is not None
    return result.data


async def test_schema_sql_does_not_stamp_a_foreign_org_id(org_plane):
    """schema.sql runs inside every org schema and cannot know which org it is — so it must
    not write an org id at all. The old `SET org_id = 'root'` was wrong in every org but root."""
    async with org_plane.acquire() as conn:
        res = await conn.execute_core(select(domains_t.c.id, domains_t.c.org_id))
        stamped = {r._mapping["id"]: r._mapping["org_id"] for r in res.fetchall()}
    assert stamped["meta"] is None, stamped
    assert stamped["ops"] is None, stamped


async def test_org_admin_sees_its_orgs_domains(org_plane):
    data = await _run("{ domains { id } }", **_ORG_ADMIN_CTX)
    ids = {d["id"] for d in data["domains"]}
    assert "meta" in ids, ids
    assert "ops" in ids, ids
    assert "shelter" in ids, ids


async def test_org_admin_sees_its_orgs_registered_tables(org_plane):
    data = await _run("{ tables { sourceId tableName } }", **_ORG_ADMIN_CTX)
    assert [(t["sourceId"], t["tableName"]) for t in data["tables"]] == [("sales-pg", "orders")]


async def test_org_admin_sees_its_orgs_roles(org_plane):
    data = await _run("{ roles { id } }", **_ORG_ADMIN_CTX)
    assert "org_admin" in {r["id"] for r in data["roles"]}


async def test_platform_admin_still_sees_the_same_tenant_schema(org_plane):
    """The platform-bypass branch and the member branch must agree — one schema, one answer."""
    as_admin = await _run(
        "{ domains { id } }", active_org_id=_ORG_ID, roles=["admin"], user_id="root-user"
    )
    as_member = await _run("{ domains { id } }", **_ORG_ADMIN_CTX)
    assert {d["id"] for d in as_admin["domains"]} == {d["id"] for d in as_member["domains"]}


async def test_unset_active_org_is_still_refused(org_plane):
    """Removing the row filter must not weaken the guard that an org must be bound."""
    result = await _run_raw("{ domains { id } }", active_org_id=None, roles=["org_admin:*"])
    assert result.errors, "an unbound org must not resolve to any org's rows"
    assert "active_org_id" in str(result.errors[0])


async def test_domains_table_left_untouched_by_the_read(org_plane):
    """A read must not repair the stamp — the fix is in the query, not a data rewrite."""
    await _run("{ domains { id } }", **_ORG_ADMIN_CTX)
    async with org_plane.acquire() as conn:
        res = await conn.execute_core(text(f"SELECT count(*) FROM {_SCHEMA}.domains"))
        assert res.fetchone()[0] == 4
