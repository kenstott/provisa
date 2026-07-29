# Copyright (c) 2026 Kenneth Stott
# Canary: 3a6e0c94-77bd-42f1-9c58-b1d4e2f7a806
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1293: a self-created org that asked for demo data can SEE it on the admin surface.

Reported failure: after a second user signed in and created an org with demo data, the app
showed no registered tables and neither the ``ops`` nor ``meta`` domain — the org looked empty.

Two separate things had to hold and only one was covered anywhere: that provisioning writes the
demo rows into ``org_<id>``, and that the admin resolvers hand those rows back to that org's own
``org_admin`` (an identity with no platform-bypass capability). This drives the REAL
``build_org_runtime`` against a live Postgres and then reads through the real GraphQL schema
under an org_admin identity — the same path the browser uses.
"""

from __future__ import annotations

import os
from types import SimpleNamespace

import pytest
from sqlalchemy import select

from provisa.core.schema_org import domains as domains_t
from provisa.core.schema_org import registered_tables as registered_tables_t

_CONFIG = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "fixtures", "org_demo_seed_config.yaml")
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

_ORG_ID = "demoseed"
_SCHEMA = f"org_{_ORG_ID}"


@pytest.fixture(scope="module")
async def demo_org():
    """Provision _ORG_ID with demo data through the real build_org_runtime, bound as the app's org."""
    from _pytest.monkeypatch import MonkeyPatch

    from provisa.api.app import build_org_runtime, create_app, state
    from provisa.api.org_runtime import reset_current_org, set_current_org

    # Scoped to this module's fixture, never at import time: pytest imports every test module
    # before running anything, so an import-time PROVISA_CONFIG would point EVERY other in-process
    # create_app in the session at this fixture config.
    mp = MonkeyPatch()
    mp.setenv("PROVISA_CONFIG", _CONFIG)
    mp.setenv("PG_HOST", os.environ.get("PG_HOST", "localhost"))
    mp.setenv("PG_PORT", os.environ.get("PG_PORT", "5432"))
    mp.setenv("PG_PASSWORD", os.environ.get("PG_PASSWORD", "provisa"))

    try:
        app = create_app()
        async with app.router.lifespan_context(app):
            assert state.tenant_db is not None
            async with state.tenant_db.acquire() as conn:
                await conn.execute(f"DROP SCHEMA IF EXISTS {_SCHEMA} CASCADE")
                await conn.execute(f"DROP SCHEMA IF EXISTS {_SCHEMA}_mv_cache CASCADE")

            rt = await build_org_runtime(_ORG_ID, include_demo=True)
            token = set_current_org(_ORG_ID)
            try:
                yield state, rt
            finally:
                reset_current_org(token)
                assert rt.tenant_db is not None
                async with rt.tenant_db.acquire() as conn:
                    await conn.execute(f"DROP SCHEMA IF EXISTS {_SCHEMA} CASCADE")
                    await conn.execute(f"DROP SCHEMA IF EXISTS {_SCHEMA}_mv_cache CASCADE")
    finally:
        mp.undo()


def _org_admin_context():
    """A request context for an org_admin of _ORG_ID — deliberately no admin/superadmin bypass."""
    identity = SimpleNamespace(user_id="creator", roles=["org_admin:*"])
    request = SimpleNamespace(state=SimpleNamespace(active_org_id=_ORG_ID, identity=identity))
    return {"request": request}


async def _run(query: str) -> dict:
    from provisa.api.admin.schema import admin_schema

    result = await admin_schema.execute(query, context_value=_org_admin_context())
    assert result.errors is None, result.errors
    assert result.data is not None
    return result.data


async def test_provisioning_writes_the_demo_rows_into_the_orgs_own_schema(demo_org):
    _state, rt = demo_org
    assert rt.tenant_db is not None
    async with rt.tenant_db.acquire() as conn:
        tables = (await conn.execute_core(select(registered_tables_t.c.table_name))).fetchall()
        domain_ids = {r[0] for r in (await conn.execute_core(select(domains_t.c.id))).fetchall()}
    assert "orders" in {r[0] for r in tables}
    assert "sales-analytics" in domain_ids


async def test_org_admin_sees_the_demo_tables_on_the_admin_surface(demo_org):
    data = await _run("{ tables { sourceId tableName domainId } }")
    assert "orders" in {t["tableName"] for t in data["tables"]}


async def test_org_admin_sees_the_built_in_domains(demo_org):
    # ops and meta are seeded by schema.sql into EVERY org schema; they went missing because the
    # admin read filtered on an org_id column schema.sql had stamped with another org's id.
    ids = {d["id"] for d in (await _run("{ domains { id } }"))["domains"]}
    assert "ops" in ids, ids
    assert "meta" in ids, ids
    assert "sales-analytics" in ids, ids
