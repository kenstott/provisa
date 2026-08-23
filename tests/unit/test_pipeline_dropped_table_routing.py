# Copyright (c) 2026 Kenneth Stott
# Canary: 7c2e4f91-3a5b-4d6e-9f1a-8b2c5d7e9f0a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Regression test: _optimize_and_route must not raise for an API table that fails
materialization but is NOT inside a UNION (e.g. a single-object lookup keyed by a
required path param, such as an openapi get_pet_by_id endpoint).

ROOT CAUSE: _materialize_api_to_engine_cache appends ANY table that fails
materialization to `dropped_tables`, regardless of whether the SQL actually contains a
UNION referencing it. drop_union_branches_for_table is a no-op for a plain single-source
SELECT, so the table name remains in exec_sql after the "drop" attempt. A guard added to
_optimize_and_route treated that no-op as fatal and raised RuntimeError, breaking every
plain (non-union) query against a non-materializable API table.

FIX: when drop_union_branches_for_table is a no-op, the table was never removed from
exec_sql — it stays untouched and participates in normal source extraction / routing as
a live source, instead of being (a) treated as an error or (b) incorrectly folded into
the "_inlined" set used to reduce the routing source set.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from provisa.api_source.engine_cache import CacheLocation
from provisa.api_source.models import ApiColumn, ApiColumnType, ParamType
from provisa.compiler.sql_gen import CompilationContext
from provisa.compiler.sql_types import TableMeta
from provisa.compiler.stage2 import build_governance_context
from provisa.compiler.rls import RLSContext
from provisa.pgwire._pipeline import _optimize_and_route
from provisa.transpiler.router import Route

SOURCE_ID = "ghsrc"
TABLE_ID = 7


def _ctx() -> CompilationContext:
    ctx = CompilationContext()
    ctx.tables = {
        "pets": TableMeta(
            table_id=TABLE_ID,
            field_name="pets",
            type_name="Pets",
            source_id=SOURCE_ID,
            catalog_name=SOURCE_ID,
            schema_name="public",
            table_name="pets",
            domain_id="petstore",
        )
    }
    return ctx


def _state() -> SimpleNamespace:
    return SimpleNamespace(
        hot_manager=None,
        api_endpoints={},
        graphql_remote_sources={
            "gh": {
                "source_id": SOURCE_ID,
                "url": "https://example.test/graphql",
                "tables": [
                    {
                        "sql_name": "pets",
                        "name": "Pet",
                        "field_name": "pets",
                        "required_args": [{"name": "name"}],
                        "columns": [{"name": "id", "type": "integer"}],
                    }
                ],
            }
        },
        source_types={SOURCE_ID: "graphql_api"},
        source_dialects={SOURCE_ID: None},
        source_dsns={},
        source_pools=SimpleNamespace(source_ids={SOURCE_ID}, has=lambda _: False),
    )


async def test_non_union_unmaterializable_api_table_routes_instead_of_raising():
    """A single-source (non-UNION) query against a table that fails materialization
    must fall through to normal routing, not raise RuntimeError."""
    ctx = _ctx()
    rls = RLSContext.empty()
    gov_ctx = build_governance_context("analyst", rls, {}, ctx, tables=[])
    state = _state()

    exec_sql, decision, default_source, optimized, sources, _opts = await _optimize_and_route(
        "SELECT * FROM pets",
        "SELECT * FROM pets",
        gov_ctx,
        ctx,
        state,
        nf_args={},
    )

    assert "pets" in exec_sql
    assert sources == {SOURCE_ID}
    assert decision.route == Route.API


API_SOURCE_ID = "petstore-api"


def _openapi_ctx() -> CompilationContext:
    ctx = CompilationContext()
    ctx.tables = {
        "get_pet_by_id": TableMeta(
            table_id=TABLE_ID,
            field_name="get_pet_by_id",
            type_name="GetPetById",
            source_id=API_SOURCE_ID,
            catalog_name=API_SOURCE_ID,
            schema_name="public",
            table_name="get_pet_by_id",
            domain_id="petstore",
        )
    }
    return ctx


class _FakeAcquireCtx:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, *exc):
        return False


def _openapi_state() -> SimpleNamespace:
    ep = SimpleNamespace(
        source_id=API_SOURCE_ID,
        table_name="get_pet_by_id",
        path="/pet/{petId}",
        ttl=60,
        columns=[
            ApiColumn(name="id", type=ApiColumnType.string),
            ApiColumn(name="petId", type=ApiColumnType.string, param_type=ParamType.path),
        ],
    )
    # PG hydrate always misses (no pre-seeded row for this specific petId).
    conn = AsyncMock()
    conn.fetch = AsyncMock(side_effect=Exception('relation "default.get_pet_by_id" does not exist'))
    return SimpleNamespace(
        hot_manager=None,
        api_endpoints={"get_pet_by_id": ep},
        graphql_remote_sources={},
        api_sources={},
        org_id="default",
        source_cache={},
        response_cache_default_ttl=300,
        federation_engine=MagicMock(),
        tenant_db=SimpleNamespace(acquire=lambda: _FakeAcquireCtx(conn)),
        # A second, pooled RDBMS source alongside the API source — matches the real demo
        # topology (pet-store-pg is always registered too); the API source alone would have
        # no non-API default_source to fall back to once it's fully inlined.
        source_types={API_SOURCE_ID: "openapi", "pgsrc": "postgresql"},
        source_dialects={API_SOURCE_ID: None, "pgsrc": "postgres"},
        source_dsns={},
        source_pools=SimpleNamespace(
            source_ids={API_SOURCE_ID, "pgsrc"}, has=lambda sid: sid == "pgsrc"
        ),
    )


async def test_openapi_path_param_table_routes_through_engine_cache_not_tenant_db():
    """A required-path-param openapi lookup (get_pet_by_id keyed by _nf_petId) must resolve
    the param and land the REST result as an inlined VALUES CTE, collapsing the query to a
    single live (pooled) source — never leave it on Route.API against the unmaterialized API
    source (which _execute_plan has no branch for and would misroute to state.tenant_db,
    raising "no such table")."""
    ctx = _openapi_ctx()
    rls = RLSContext.empty()
    gov_ctx = build_governance_context("analyst", rls, {}, ctx, tables=[])
    state = _openapi_state()
    loc = CacheLocation("cat", "sch", "relational")
    rest_result = SimpleNamespace(from_cache=False, rows=[{"id": "1"}])

    with (
        patch("provisa.api_source.engine_cache.cache_location", return_value=loc),
        patch("provisa.api_source.engine_cache.cache_table_name", return_value="r_x"),
        patch("provisa.api_source.engine_cache.table_known_live", return_value=False),
        patch("provisa.api_source.engine_cache.ensure_cache_schema"),
        patch("provisa.api_source.engine_cache.table_exists", return_value=False),
        patch(
            "provisa.api_source.router_integration.handle_api_query",
            new=AsyncMock(return_value=rest_result),
        ) as m_handle,
        patch("provisa.api_source.engine_cache.create_and_insert"),
        patch("provisa.api_source.engine_cache.schedule_drop", new=AsyncMock()),
    ):
        exec_sql, decision, default_source, optimized, sources, _opts = await _optimize_and_route(
            "SELECT * FROM get_pet_by_id WHERE \"_nf_petId\" = '1'",
            "SELECT * FROM get_pet_by_id WHERE \"_nf_petId\" = '1'",
            gov_ctx,
            ctx,
            state,
            nf_args={"petId": "1"},
        )

    assert m_handle.await_args.args[1] == {"petId": "1"}
    assert decision.route != Route.API
    assert state.source_pools.has(decision.source_id)
