# Copyright (c) 2026 Kenneth Stott
# Canary: f3a2b1c0-d4e5-4f6a-8b9c-0d1e2f3a4b5c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration tests: sampling + RLS + caching interactions.

These tests cover multi-concern scenarios not addressed by:
  - tests/unit/test_sampling.py          (apply_sampling unit tests only)
  - tests/unit/test_sampling_rls.py      (unit tests for sampling+RLS individually)
  - tests/integration/test_rls_execution.py  (RLS execution against a real DB)

Focus: correctness of sampling+RLS together, cache key partitioning by role,
and cache invalidation when sample limits change.

Tests that touch a real DB are marked @pytest.mark.integration.
Tests that exercise only the pipeline's compile+governance layer use no marker
and run without infrastructure.
"""

from __future__ import annotations

import json
import os

import pytest
import pytest_asyncio

from provisa.cache.key import cache_key
from provisa.cache.store import CachedResult, NoopCacheStore, RedisCacheStore
from provisa.compiler.rls import RLSContext, inject_rls
from provisa.compiler.sampling import apply_sampling
from provisa.compiler.sql_gen import (
    ColumnRef,
    CompilationContext,
    CompiledQuery,
    TableMeta,
)
from provisa.executor.direct import execute_direct
from provisa.executor.pool import SourcePool

# ---------------------------------------------------------------------------
# Shared constants
# ---------------------------------------------------------------------------

ORDERS_TABLE_ID = 1
CUSTOMERS_TABLE_ID = 2

_SQL_ORDERS = 'SELECT "id", "amount", "region" FROM "public"."orders"'
_SQL_CUSTOMERS = 'SELECT "id", "name", "region" FROM "public"."customers"'


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _orders_meta() -> TableMeta:
    return TableMeta(
        table_id=ORDERS_TABLE_ID,
        field_name="orders",
        type_name="Orders",
        source_id="test-pg",
        catalog_name="test_pg",
        schema_name="public",
        table_name="orders",
    )


def _customers_meta() -> TableMeta:
    return TableMeta(
        table_id=CUSTOMERS_TABLE_ID,
        field_name="customers",
        type_name="Customers",
        source_id="test-pg",
        catalog_name="test_pg",
        schema_name="public",
        table_name="customers",
    )


def _ctx(include_customers: bool = False) -> CompilationContext:
    ctx = CompilationContext()
    ctx.tables["orders"] = _orders_meta()
    if include_customers:
        ctx.tables["customers"] = _customers_meta()
    return ctx


def _orders_compiled(extra_sql: str = "") -> CompiledQuery:
    sql = _SQL_ORDERS + (f" {extra_sql}" if extra_sql else "")
    return CompiledQuery(
        sql=sql,
        params=[],
        root_field="orders",
        columns=[
            ColumnRef(alias=None, column="id", field_name="id", nested_in=None),
            ColumnRef(alias=None, column="amount", field_name="amount", nested_in=None),
            ColumnRef(alias=None, column="region", field_name="region", nested_in=None),
        ],
        sources={"test-pg"},
    )


def _customers_compiled() -> CompiledQuery:
    return CompiledQuery(
        sql=_SQL_CUSTOMERS,
        params=[],
        root_field="customers",
        columns=[
            ColumnRef(alias=None, column="id", field_name="id", nested_in=None),
            ColumnRef(alias=None, column="name", field_name="name", nested_in=None),
            ColumnRef(alias=None, column="region", field_name="region", nested_in=None),
        ],
        sources={"test-pg"},
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture(scope="session")
async def source_pool():
    sp = SourcePool()
    await sp.add(
        "test-pg",
        source_type="postgresql",
        host=os.environ.get("PG_HOST", "localhost"),
        port=int(os.environ.get("PG_PORT", "5432")),
        database=os.environ.get("PG_DATABASE", "provisa"),
        user=os.environ.get("PG_USER", "provisa"),
        password=os.environ.get("PG_PASSWORD", "provisa"),
    )
    yield sp
    await sp.close_all()


# ---------------------------------------------------------------------------
# Unit-level tests (no DB or Redis required)
# ---------------------------------------------------------------------------


class TestSamplingAndRLSCombinedLogic:
    """Verify governance pipeline: RLS filter then sampling limit — no live DB."""

    def test_rls_then_sampling_both_present_in_sql(self):
        """After RLS injection then sampling, SQL must contain WHERE and LIMIT."""
        compiled = _orders_compiled()
        ctx = _ctx()
        rls = RLSContext(rules={ORDERS_TABLE_ID: "region = 'us-east'"})

        with_rls = inject_rls(compiled, ctx, rls)
        final = apply_sampling(with_rls, 50)

        assert "WHERE" in final.sql
        assert "region = 'us-east'" in final.sql
        assert "LIMIT 50" in final.sql

    def test_sampling_does_not_bypass_rls_clause(self):
        """LIMIT is injected after the WHERE clause — RLS cannot be silently dropped."""
        compiled = _orders_compiled()
        ctx = _ctx()
        rls = RLSContext(rules={ORDERS_TABLE_ID: "region = 'eu-west'"})

        with_rls = inject_rls(compiled, ctx, rls)
        # Simulate a large user-requested limit
        large_limit = CompiledQuery(
            sql=with_rls.sql + " LIMIT 10000",
            params=with_rls.params,
            root_field=with_rls.root_field,
            columns=with_rls.columns,
            sources=with_rls.sources,
        )
        final = apply_sampling(large_limit, 100)

        # Sampling capped the limit
        assert "LIMIT 100" in final.sql
        assert "10000" not in final.sql
        # RLS filter still present
        assert "eu-west" in final.sql
        # LIMIT appears after WHERE
        assert final.sql.index("WHERE") < final.sql.index("LIMIT 100")

    def test_rls_filter_no_match_produces_empty_like_sql(self):
        """An RLS filter that would match nothing is still valid SQL.

        We can't execute against a DB here, but we verify the generated SQL
        is structurally correct: WHERE clause present, LIMIT injected.
        """
        compiled = _orders_compiled()
        ctx = _ctx()
        rls = RLSContext(rules={ORDERS_TABLE_ID: "region = 'impossible-region-xyz'"})

        with_rls = inject_rls(compiled, ctx, rls)
        final = apply_sampling(with_rls, 100)

        # SQL is valid structure even when predicate will match nothing
        assert "WHERE" in final.sql
        assert "impossible-region-xyz" in final.sql
        assert "LIMIT 100" in final.sql

    def test_cache_key_differs_by_rls_rules(self):
        """Two roles with different RLS rules must produce different cache keys."""
        sql = 'SELECT "id" FROM "public"."orders"'
        params: list = []

        key_us = cache_key(sql, params, "analyst", {ORDERS_TABLE_ID: "region = 'us-east'"})
        key_eu = cache_key(sql, params, "analyst", {ORDERS_TABLE_ID: "region = 'eu-west'"})

        assert key_us != key_eu

    def test_cache_key_differs_by_role_id(self):
        """Same SQL + same RLS expr but different role_id → different cache key."""
        sql = 'SELECT "id" FROM "public"."orders"'
        params: list = []
        rls = {ORDERS_TABLE_ID: "region = 'us-east'"}

        key_analyst = cache_key(sql, params, "analyst", rls)
        key_viewer = cache_key(sql, params, "viewer", rls)

        assert key_analyst != key_viewer

    def test_cache_key_stable_for_same_inputs(self):
        """Same inputs always produce the same cache key (determinism)."""
        sql = 'SELECT "id" FROM "public"."orders"'
        params: list = ["val"]
        rls = {ORDERS_TABLE_ID: "region = 'us-east'"}

        key1 = cache_key(sql, params, "analyst", rls)
        key2 = cache_key(sql, params, "analyst", rls)

        assert key1 == key2

    def test_cache_key_includes_sample_limit_indirectly_via_sql(self):
        """SQL with different LIMIT values must produce different cache keys.

        When a sampling limit changes, the cached SQL string changes,
        guaranteeing a different key — old cached results are not served.
        """
        sql_100 = 'SELECT "id" FROM "public"."orders" LIMIT 100'
        sql_50 = 'SELECT "id" FROM "public"."orders" LIMIT 50'
        params: list = []
        rls: dict = {}

        key_100 = cache_key(sql_100, params, "analyst", rls)
        key_50 = cache_key(sql_50, params, "analyst", rls)

        assert key_100 != key_50

    def test_cache_key_empty_rls_vs_rls_differ(self):
        """No-RLS and role-with-RLS must get different keys for the same query."""
        sql = 'SELECT "id" FROM "public"."orders"'
        params: list = []

        key_no_rls = cache_key(sql, params, "admin", {})
        key_with_rls = cache_key(sql, params, "admin", {ORDERS_TABLE_ID: "region = 'us-east'"})

        assert key_no_rls != key_with_rls

    def test_cache_key_raises_on_empty_rls_expression(self):
        """An empty RLS filter expression must raise ValueError (security guard)."""
        with pytest.raises(ValueError, match="empty filter expression"):
            cache_key("SELECT 1", [], "analyst", {ORDERS_TABLE_ID: ""})

    def test_cache_key_raises_on_whitespace_only_rls_expression(self):
        """A whitespace-only RLS filter expression must also raise ValueError."""
        with pytest.raises(ValueError, match="empty filter expression"):
            cache_key("SELECT 1", [], "analyst", {ORDERS_TABLE_ID: "   "})

    def test_sampling_preserves_rls_params(self):
        """Parameters from RLS injection must be preserved after sampling."""
        # In current impl RLS injects a literal expression (no params), but
        # we verify the param list on a pre-parameterised compiled query is
        # not lost when sampling is subsequently applied.
        compiled = CompiledQuery(
            sql=_SQL_ORDERS + " WHERE region = $1",
            params=["us-east"],
            root_field="orders",
            columns=[],
            sources={"test-pg"},
        )
        result = apply_sampling(compiled, 100)
        assert result.params == ["us-east"]
        assert "LIMIT 100" in result.sql

    def test_rls_on_two_tables_sampling_applied_once(self):
        """RLS on two root queries, sampling applied to each independently."""
        ctx = _ctx(include_customers=True)

        orders = _orders_compiled()
        customers = _customers_compiled()

        rls_orders = RLSContext(rules={ORDERS_TABLE_ID: "region = 'us-east'"})
        rls_customers = RLSContext(rules={CUSTOMERS_TABLE_ID: "region = 'us-east'"})

        orders_final = apply_sampling(inject_rls(orders, ctx, rls_orders), 50)
        customers_final = apply_sampling(inject_rls(customers, ctx, rls_customers), 50)

        # Each query has its own LIMIT and RLS
        assert "LIMIT 50" in orders_final.sql
        assert "region = 'us-east'" in orders_final.sql
        assert "LIMIT 50" in customers_final.sql
        assert "region = 'us-east'" in customers_final.sql

        # The queries are independent (separate SQL strings)
        assert orders_final.sql != customers_final.sql


# ---------------------------------------------------------------------------
# DB integration tests
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.asyncio(loop_scope="session")
class TestSamplingRLSDBExecution:
    """Verify sampling + RLS together in real DB execution."""

    async def test_sampling_and_rls_both_respected(self, source_pool):
        """Role has both RLS filter AND a sample limit; result honours both."""
        ctx = _ctx()
        compiled = _orders_compiled()

        rls = RLSContext(rules={ORDERS_TABLE_ID: "region = 'us-east'"})
        with_rls = inject_rls(compiled, ctx, rls)
        final = apply_sampling(with_rls, 2)

        result = await execute_direct(
            source_pool, "test-pg", final.sql, final.params
        )

        # At most sample_size rows returned
        assert len(result.rows) <= 2
        # Every row respects the RLS filter
        region_idx = result.column_names.index("region")
        for row in result.rows:
            assert row[region_idx] == "us-east"

    async def test_sampling_does_not_serve_rows_outside_rls(self, source_pool):
        """Even if sample_size > filtered rows, only RLS-matching rows returned."""
        ctx = _ctx()
        # Use a region that may have fewer rows than the sample size
        rls = RLSContext(rules={ORDERS_TABLE_ID: "region = 'us-west'"})
        with_rls = inject_rls(_orders_compiled(), ctx, rls)
        final = apply_sampling(with_rls, 1000)  # large sample — no bypass

        result = await execute_direct(
            source_pool, "test-pg", final.sql, final.params
        )

        region_idx = result.column_names.index("region")
        for row in result.rows:
            assert row[region_idx] == "us-west", (
                f"Sampling bypassed RLS: unexpected region {row[region_idx]!r}"
            )

    async def test_rls_eliminates_all_rows_under_sampling_is_empty(self, source_pool):
        """When RLS eliminates all rows, result is empty — not an error."""
        ctx = _ctx()
        rls = RLSContext(rules={ORDERS_TABLE_ID: "region = 'nonexistent-region-xyz'"})
        with_rls = inject_rls(_orders_compiled(), ctx, rls)
        final = apply_sampling(with_rls, 100)

        result = await execute_direct(
            source_pool, "test-pg", final.sql, final.params
        )

        assert result.rows == [], (
            f"Expected empty result, got {len(result.rows)} rows"
        )
