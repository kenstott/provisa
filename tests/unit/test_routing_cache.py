# Copyright (c) 2026 Kenneth Stott
# Canary: 4f8a2e6c-9d1b-4a3e-8c5f-2b7e9a1d4c6f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1877 routing addendum (2026-09-29): `_optimize_and_route_cached`
(`provisa/pgwire/_pipeline.py`) and its safety pre-check `would_materialize_optimize`
(`provisa/api/data/materialization.py`).

Covers the three invariants the addendum's own design doc requires:
  (a) a cache hit for a repeated exec-SQL shape + role + schema generation skips the routing
      recompute (`_optimize_and_route` itself is not called again)
  (b) a schema/RLS/role mutation (schema_version bump) invalidates a previously cached entry
  (c) a query whose table IS hot/API-cache-eligible is NEVER cached — every call re-runs the live
      check, proven by flipping hot-table state between two calls of the identical query and
      asserting the route differs correctly both times
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import patch

from provisa.api.data.materialization import would_materialize_optimize
from provisa.compiler.compiled_query_cache import CompiledQueryCache
from provisa.compiler.rls import RLSContext
from provisa.compiler.sql_gen import CompilationContext
from provisa.compiler.sql_types import TableMeta
from provisa.compiler.stage2 import build_governance_context
from provisa.pgwire._pipeline import _optimize_and_route, _optimize_and_route_cached
from provisa.transpiler.router import Route

PG_SOURCE_ID = "pgsrc"
API_SOURCE_ID = "petstore-api"


class _FakeHotManager:
    """Minimal stand-in for HotTableManager: `is_hot`/`get_entry` flip on demand."""

    def __init__(self):
        self.hot: set[str] = set()
        self.auto_threshold = 500

    def is_hot(self, table_name: str) -> bool:
        return table_name in self.hot

    def get_entry(self, table_name: str):
        from provisa.cache.hot_tables import HotTableEntry

        if table_name not in self.hot:
            return None
        return HotTableEntry(
            table_name=table_name,
            catalog="cat",
            schema="sch",
            pk_column="id",
            rows=[{"id": 1}],
            column_names=["id"],
            is_api=True,
        )


def _ctx() -> CompilationContext:
    ctx = CompilationContext()
    ctx.tables = {
        "orders": TableMeta(
            table_id=1,
            field_name="orders",
            type_name="Orders",
            source_id=PG_SOURCE_ID,
            catalog_name=PG_SOURCE_ID,
            schema_name="public",
            table_name="orders",
            domain_id="sales",
        )
    }
    return ctx


def _state(hot_manager=None) -> SimpleNamespace:
    return SimpleNamespace(
        hot_manager=hot_manager,
        api_endpoints={},
        graphql_remote_sources={},
        source_types={PG_SOURCE_ID: "postgresql"},
        source_dialects={PG_SOURCE_ID: "postgres"},
        source_dsns={},
        source_pools=SimpleNamespace(
            source_ids={PG_SOURCE_ID}, has=lambda sid: sid == PG_SOURCE_ID
        ),
        tables=[],
        schema_boot_id="boot-1",
        schema_version=1,
        routing_cache=CompiledQueryCache(ttl_seconds=3600),
    )


def _gov_ctx(ctx):
    rls = RLSContext.empty()
    return build_governance_context("analyst", rls, {}, ctx, tables=[])


# --------------------------------------------------------------------------- #
# (a) cache hit skips the routing recompute
# --------------------------------------------------------------------------- #


async def test_cache_hit_skips_optimize_and_route_recompute():
    ctx = _ctx()
    gov_ctx = _gov_ctx(ctx)
    state = _state()
    sql = "SELECT * FROM sales.orders"

    with patch("provisa.pgwire._pipeline._optimize_and_route", wraps=_optimize_and_route) as m_opt:
        r1 = await _optimize_and_route_cached(sql, sql, gov_ctx, ctx, state, "analyst")
        assert m_opt.await_count == 1
        assert r1[1].route == Route.DIRECT

        r2 = await _optimize_and_route_cached(sql, sql, gov_ctx, ctx, state, "analyst")
        # The routing recompute (extract_sources/decide_route inside _optimize_and_route) must
        # not run again — this is what proves the cache actually short-circuited the work,
        # not merely returned an equal-looking result.
        assert m_opt.await_count == 1
        assert r2[1].route == r1[1].route
        assert r2[1].source_id == r1[1].source_id
        assert r2[4] == r1[4]  # sources


async def test_cache_populates_routing_cache_entry():
    ctx = _ctx()
    gov_ctx = _gov_ctx(ctx)
    state = _state()
    sql = "SELECT * FROM sales.orders"
    assert len(state.routing_cache) == 0
    await _optimize_and_route_cached(sql, sql, gov_ctx, ctx, state, "analyst")
    assert len(state.routing_cache) == 1


# --------------------------------------------------------------------------- #
# (b) schema/RLS/role mutation invalidates the cache
# --------------------------------------------------------------------------- #


async def test_schema_version_bump_invalidates_routing_cache():
    ctx = _ctx()
    gov_ctx = _gov_ctx(ctx)
    state = _state()
    sql = "SELECT * FROM sales.orders"

    with patch("provisa.pgwire._pipeline._optimize_and_route", wraps=_optimize_and_route) as m_opt:
        await _optimize_and_route_cached(sql, sql, gov_ctx, ctx, state, "analyst")
        assert m_opt.await_count == 1

        # Simulate a schema/RLS/role mutation: _rebuild_schemas_impl bumps schema_version.
        state.schema_version += 1

        await _optimize_and_route_cached(sql, sql, gov_ctx, ctx, state, "analyst")
        # A new generation's key misses the old entry and must recompute, not serve the stale
        # generation's cached route.
        assert m_opt.await_count == 2


async def test_role_change_uses_a_different_cache_key():
    ctx = _ctx()
    gov_ctx = _gov_ctx(ctx)
    state = _state()
    sql = "SELECT * FROM sales.orders"

    with patch("provisa.pgwire._pipeline._optimize_and_route", wraps=_optimize_and_route) as m_opt:
        await _optimize_and_route_cached(sql, sql, gov_ctx, ctx, state, "analyst")
        await _optimize_and_route_cached(sql, sql, gov_ctx, ctx, state, "modeler")
        assert m_opt.await_count == 2


# --------------------------------------------------------------------------- #
# (c) a query whose table is hot/API-cache-eligible is NEVER cached — every call re-runs the
# live check, and flipping hot state between two calls changes the route both times.
# --------------------------------------------------------------------------- #


async def test_hot_table_query_never_cached_and_route_tracks_live_state():
    ctx = _ctx()
    gov_ctx = _gov_ctx(ctx)
    hot_mgr = _FakeHotManager()
    state = _state(hot_manager=hot_mgr)
    # Table has no PG pool and no API registration either — only the hot-mgr flag drives this
    # test; would_materialize_optimize must return True the instant hot_mgr.is_hot("orders") is
    # True, regardless of anything else.
    sql = "SELECT * FROM sales.orders"

    # Call 1: table is NOT hot -> would_materialize_optimize is False for this call (no live
    # branch reachable) -> eligible for caching.
    assert would_materialize_optimize(sql, state) is False
    r1 = await _optimize_and_route_cached(sql, sql, gov_ctx, ctx, state, "analyst")
    assert len(state.routing_cache) == 1
    assert r1[1].route == Route.DIRECT

    # Flip the table hot BEFORE the second call.
    hot_mgr.hot.add("orders")
    assert would_materialize_optimize(sql, state) is True

    with patch("provisa.pgwire._pipeline._optimize_and_route", wraps=_optimize_and_route) as m_opt:
        r2 = await _optimize_and_route_cached(sql, sql, gov_ctx, ctx, state, "analyst")
        # The live check caught the hot flip: the cache (populated by call 1, still within TTL)
        # must NOT have been consulted — _optimize_and_route ran the full, live path instead.
        assert m_opt.await_count == 1
    # The hot inline collapses "orders" into a VALUES CTE with no live source left, so routing
    # differs from call 1's plain DIRECT-against-pgsrc route.
    assert r2[3] is True  # optimized
    assert r1[3] is False  # optimized
    # The routing cache must still hold only the one entry from call 1 — the hot call was never
    # written into it (would corrupt the no-optimization invariant the cache depends on).
    assert len(state.routing_cache) == 1

    # Flip back to not-hot for a third call: must read the still-valid call-1 cache entry again
    # (proves the live re-check, not just "never cache once a hot table is seen anywhere").
    hot_mgr.hot.discard("orders")
    with patch("provisa.pgwire._pipeline._optimize_and_route", wraps=_optimize_and_route) as m_opt3:
        r3 = await _optimize_and_route_cached(sql, sql, gov_ctx, ctx, state, "analyst")
        assert m_opt3.await_count == 0
    assert r3[1].route == r1[1].route == Route.DIRECT
