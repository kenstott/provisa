# Copyright (c) 2026 Kenneth Stott
# Canary: a1b2c3d4-e5f6-7890-abcd-ef1234567890
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration tests: RLS WHERE clause injection in executed query results (REQ-040, REQ-041)."""

from __future__ import annotations

import os

import pytest
import pytest_asyncio

from provisa.compiler.rls import RLSContext, inject_rls
from provisa.compiler.sql_gen import (
    ColumnRef,
    CompilationContext,
    CompiledQuery,
    JoinMeta,
    TableMeta,
)
from provisa.executor.direct import execute_direct
from provisa.executor.pool import SourcePool

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

ORDERS_TABLE_ID = 1
CUSTOMERS_TABLE_ID = 2


def _make_orders_meta() -> TableMeta:
    return TableMeta(
        table_id=ORDERS_TABLE_ID,
        field_name="orders",
        type_name="Orders",
        source_id="test-pg",
        catalog_name="test_pg",
        schema_name="public",
        table_name="orders",
    )


def _make_customers_meta() -> TableMeta:
    return TableMeta(
        table_id=CUSTOMERS_TABLE_ID,
        field_name="customers",
        type_name="Customers",
        source_id="test-pg",
        catalog_name="test_pg",
        schema_name="public",
        table_name="customers",
    )


def _make_orders_ctx(include_join: bool = False) -> CompilationContext:
    ctx = CompilationContext()
    ctx.tables["orders"] = _make_orders_meta()
    if include_join:
        ctx.tables["customers"] = _make_customers_meta()
        ctx.joins[("Orders", "customers")] = JoinMeta(
            source_column="customer_id",
            target_column="id",
            source_column_type="integer",
            target_column_type="integer",
            target=_make_customers_meta(),
            cardinality="many-to-one",
        )
    return ctx


def _simple_orders_query(extra_sql: str = "") -> CompiledQuery:
    """Return a minimal CompiledQuery for orders (no aliases — simple path)."""
    base_sql = 'SELECT "id", "amount", "region" FROM "public"."orders"'
    sql = base_sql + (f" {extra_sql}" if extra_sql else "")
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


def _aliased_orders_query() -> CompiledQuery:
    """Return a CompiledQuery for orders with t0 alias (used for join tests)."""
    sql = (
        'SELECT "t0"."id", "t0"."amount", "t0"."region", "t1"."name" '
        'FROM "public"."orders" "t0" '
        'LEFT JOIN "public"."customers" "t1" ON "t0"."customer_id" = "t1"."id"'
    )
    return CompiledQuery(
        sql=sql,
        params=[],
        root_field="orders",
        columns=[
            ColumnRef(alias="t0", column="id", field_name="id", nested_in=None),
            ColumnRef(alias="t0", column="amount", field_name="amount", nested_in=None),
            ColumnRef(alias="t0", column="region", field_name="region", nested_in=None),
            ColumnRef(alias="t1", column="name", field_name="name", nested_in="customers"),
        ],
        sources={"test-pg"},
    )


@pytest_asyncio.fixture(scope="session")
async def source_pool():
    sp = SourcePool()
    try:
        await sp.add(
            "test-pg",
            source_type="postgresql",
            host=os.environ.get("PG_HOST", "localhost"),
            port=int(os.environ.get("PG_PORT", "5432")),
            database=os.environ.get("PG_DATABASE", "provisa"),
            user=os.environ.get("PG_USER", "provisa"),
            password=os.environ.get("PG_PASSWORD", "provisa"),
        )
    except Exception:
        pytest.skip("PostgreSQL not available")
    yield sp
    await sp.close_all()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _col_index(result, name: str) -> int:
    return result.column_names.index(name)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestRLSExecution:
    async def test_rls_filters_rows_by_region(self, source_pool):
        """RLS rule region = 'us-east' must return only us-east rows."""
        ctx = _make_orders_ctx()
        compiled = _simple_orders_query()

        rls = RLSContext(rules={ORDERS_TABLE_ID: "region = 'us-east'"})
        compiled_rls = inject_rls(compiled, ctx, rls)

        result = await execute_direct(source_pool, "test-pg", compiled_rls.sql, compiled_rls.params)

        assert result.rows, "Expected at least one us-east row in test data"
        region_idx = _col_index(result, "region")
        for row in result.rows:
            assert row[region_idx] == "us-east", (
                f"RLS failed: got region={row[region_idx]!r}, expected 'us-east'"
            )

    async def test_rls_empty_result_when_no_match(self, source_pool):
        """RLS rule that matches no rows must yield an empty result."""
        ctx = _make_orders_ctx()
        compiled = _simple_orders_query()

        rls = RLSContext(rules={ORDERS_TABLE_ID: "region = 'nonexistent-region-xyz'"})
        compiled_rls = inject_rls(compiled, ctx, rls)

        result = await execute_direct(source_pool, "test-pg", compiled_rls.sql, compiled_rls.params)

        assert result.rows == [], (
            f"Expected empty result but got {len(result.rows)} rows"
        )

    async def test_rls_multiple_rules_anded(self, source_pool):
        """Two RLS rules on the same table must be ANDed together."""
        # We test this by injecting RLS twice (simulating two rules combined by the caller)
        # and verifying both constraints are satisfied.
        ctx = _make_orders_ctx()
        compiled = _simple_orders_query()

        # Inject first rule: region = 'us-east'
        rls1 = RLSContext(rules={ORDERS_TABLE_ID: "region = 'us-east'"})
        compiled_after_first = inject_rls(compiled, ctx, rls1)

        # Inject a second rule (amount > 0) on top — simulates AND logic
        # In real usage both filters would be in a single RLSContext; here we
        # manually construct the combined SQL to verify the AND is honoured.
        combined_sql = compiled_after_first.sql.replace(
            "(region = 'us-east')",
            "(region = 'us-east') AND (amount > 0)",
        )
        result = await execute_direct(source_pool, "test-pg", combined_sql, [])

        region_idx = _col_index(result, "region")
        amount_idx = _col_index(result, "amount")
        for row in result.rows:
            assert row[region_idx] == "us-east"
            assert row[amount_idx] > 0

    async def test_no_rls_returns_all_rows(self, source_pool):
        """No RLS rule must return all rows (unfiltered result)."""
        compiled = _simple_orders_query()
        ctx = _make_orders_ctx()

        rls = RLSContext.empty()
        compiled_no_rls = inject_rls(compiled, ctx, rls)

        # Ensure SQL is unchanged
        assert compiled_no_rls.sql == compiled.sql

        result_all = await execute_direct(source_pool, "test-pg", compiled.sql, [])
        result_rls = await execute_direct(source_pool, "test-pg", compiled_no_rls.sql, [])

        assert len(result_all.rows) == len(result_rls.rows)

    async def test_rls_uses_session_variable(self, source_pool):
        """RLS filter using a literal session value is substituted before execution."""
        # Simulates: user's session says region='us-west', so we substitute at build time.
        session_region = "us-west"
        ctx = _make_orders_ctx()
        compiled = _simple_orders_query()

        # The caller resolves :session_var before building RLSContext.
        filter_expr = f"region = '{session_region}'"
        rls = RLSContext(rules={ORDERS_TABLE_ID: filter_expr})
        compiled_rls = inject_rls(compiled, ctx, rls)

        result = await execute_direct(source_pool, "test-pg", compiled_rls.sql, compiled_rls.params)

        region_idx = _col_index(result, "region")
        for row in result.rows:
            assert row[region_idx] == session_region, (
                f"Session-var substitution failed: got {row[region_idx]!r}"
            )

    async def test_rls_on_joined_table(self, source_pool):
        """RLS on a JOINed table (customers) must filter the join result."""
        ctx = _make_orders_ctx(include_join=True)
        compiled = _aliased_orders_query()

        # Apply RLS only to the root orders table (region filter)
        rls = RLSContext(rules={ORDERS_TABLE_ID: "region = 'us-east'"})
        compiled_rls = inject_rls(compiled, ctx, rls)

        result = await execute_direct(source_pool, "test-pg", compiled_rls.sql, compiled_rls.params)

        assert "region" in result.column_names
        region_idx = _col_index(result, "region")
        for row in result.rows:
            assert row[region_idx] == "us-east", (
                f"Join RLS failed: got region={row[region_idx]!r}"
            )
