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

from provisa.core.models import DERIVED_SOURCE_ID
from provisa.core.schema_org import domains as domains_t
from provisa.core.schema_org import registered_tables as registered_tables_t
from provisa.core.schema_org import sources as sources_t
from provisa.core.schema_org import table_columns as table_columns_t

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


async def test_provisioning_seeds_the_derived_source_and_its_virtual_views(demo_org):
    # __derived__ is a system source: no config file declares it, _seed_built_in_sources writes one
    # row per org. An org provisioned before that seed existed had no __derived__ row, so every
    # config table pointing at it (the star-schema demo's dim_pet / fact_pet_inquiries) had no FK
    # target and was dropped from the load — the org came up with the demo "missing elements" and
    # its GraphQL schema had no groupBy field for the fact.
    _state, rt = demo_org
    assert rt.tenant_db is not None
    async with rt.tenant_db.acquire() as conn:
        source_ids = {r[0] for r in (await conn.execute_core(select(sources_t.c.id))).fetchall()}
        derived = {
            r[0]
            for r in (
                await conn.execute_core(
                    select(registered_tables_t.c.table_name).where(
                        registered_tables_t.c.source_id == DERIVED_SOURCE_ID
                    )
                )
            ).fetchall()
        }
    assert DERIVED_SOURCE_ID in source_ids, source_ids
    assert "order_totals" in derived, derived


async def test_provisioning_strips_control_plane_roles_from_column_grants(demo_org):
    # REQ-1337: the strip resolved the control-plane roles from the in-memory state.roles map, which
    # build_org_runtime populates AFTER the config load has registered every table — so during a
    # provisioning load it answered "none" and platform_admin stayed on every column grant of the
    # new org. It is resolved from the org's own roles table now, seeded before any registration.
    _state, rt = demo_org
    assert rt.tenant_db is not None
    async with rt.tenant_db.acquire() as conn:
        grants = (
            await conn.execute_core(
                select(table_columns_t.c.column_name, table_columns_t.c.visible_to)
                .select_from(table_columns_t.join(registered_tables_t))
                .where(registered_tables_t.c.table_name == "orders")
            )
        ).fetchall()
    by_col = {r[0]: r[1] for r in grants}
    assert by_col["id"] == ["org_admin"], by_col


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


async def test_compiled_pipeline_forces_engine_route_for_view_backed_query(demo_org):
    """REQ-135/REQ-1163: order_totals' view_sql collapses onto ONE real source (sales-pg) once
    expanded, so the router's normal cost decision would otherwise legitimately pick Route.DIRECT.
    The DIRECT branch's fallback rebuilds physical SQL from the UN-expanded governed SQL, handing
    a literal ``order_totals`` reference to a native driver that has no such table — the same class
    of bug that broke ``fact_pet_inquiries`` (500: no such table views.fact_pet_inquiries) in the
    demo config. _govern_and_route_compiled must force Route.ENGINE for any view-referencing query,
    mirroring the guard _govern_and_route (the raw-SQL path) already has."""
    from provisa.pgwire import _pipeline
    from provisa.transpiler.router import Route

    state, _rt = demo_org
    plan = await _pipeline._govern_and_route_compiled(
        'SELECT * FROM "sales_analytics"."order_totals"', "org_admin", state=state
    )
    assert plan.route == Route.ENGINE, plan.route
    assert plan.exec_sql is not None
    # the view was inline-expanded onto its base table BEFORE routing — the FROM clause must be
    # the view's defining subquery over "public"."orders", not a bare, un-routable "order_totals"
    # table reference (what the DIRECT-branch fallback would hand a native driver).
    exec_sql = plan.exec_sql.lower()
    # normalize_table_refs may add a self-alias ("orders" AS orders) after the physical table ref;
    # match the open of the subquery to tolerate that without weakening the guard (a bare
    # "order_totals" table ref would not contain this pattern).
    assert 'from (select "id", "amount" from "public"."orders"' in exec_sql, exec_sql
