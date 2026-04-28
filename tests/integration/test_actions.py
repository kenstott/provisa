# Copyright (c) 2026 Kenneth Stott
# Canary: 7c3a2f1e-8b4d-4e9a-b6c5-3d0f7e2a1b8c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration tests for the Actions feature — DB function execution against real PG.

Pure logic tests (SQL generation, webhook dispatch with mocks, mutation field generation)
live in tests/unit/test_actions.py. This file covers only tests that require a live
PostgreSQL connection.
"""

from __future__ import annotations

import os

import pytest
import pytest_asyncio

from provisa.compiler.function_gen import build_function_sql
from provisa.core.models import Function, FunctionArgument
from provisa.executor.pool import SourcePool

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest_asyncio.fixture(scope="module")
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


@pytest_asyncio.fixture(scope="module")
async def pg_func(source_pool):
    """Create a real PG function for testing, drop it after the module."""
    create_sql = """
        CREATE OR REPLACE FUNCTION public.provisa_test_action(p_region TEXT, p_limit INT)
        RETURNS TABLE(id INT, region TEXT, amount NUMERIC)
        LANGUAGE SQL STABLE AS $$
            SELECT id, region, amount
            FROM orders
            WHERE region = p_region
            LIMIT p_limit;
        $$;
    """
    await source_pool.execute("test-pg", create_sql)
    yield
    await source_pool.execute(
        "test-pg", "DROP FUNCTION IF EXISTS public.provisa_test_action(TEXT, INT)"
    )


# ---------------------------------------------------------------------------
# DB Function: real execution
# ---------------------------------------------------------------------------

class TestFunctionExecution:
    async def test_function_executes_and_returns_rows(self, source_pool, pg_func):
        """Calling a tracked DB function returns rows from the function result set."""
        from provisa.executor.direct import execute_direct

        func = Function(
            name="provisa_test_action",
            source_id="test-pg",
            schema="public",
            function_name="provisa_test_action",
            returns="test-pg.public.orders",
            arguments=[
                FunctionArgument(name="p_region", type="String"),
                FunctionArgument(name="p_limit", type="Int"),
            ],
        )
        sql, params = build_function_sql(func, ["us-east", 3])
        result = await execute_direct(source_pool, "test-pg", sql, params)

        assert result.column_names == ["id", "region", "amount"]
        assert len(result.rows) <= 3
        for row in result.rows:
            assert row[1] == "us-east"

    async def test_function_empty_result_on_no_match(self, source_pool, pg_func):
        """Function with no matching rows returns empty result."""
        from provisa.executor.direct import execute_direct

        func = Function(
            name="provisa_test_action",
            source_id="test-pg",
            schema="public",
            function_name="provisa_test_action",
            returns="test-pg.public.orders",
            arguments=[
                FunctionArgument(name="p_region", type="String"),
                FunctionArgument(name="p_limit", type="Int"),
            ],
        )
        sql, params = build_function_sql(func, ["__nonexistent_region__", 10])
        result = await execute_direct(source_pool, "test-pg", sql, params)
        assert result.rows == []

    async def test_function_limit_respected(self, source_pool, pg_func):
        """Function LIMIT arg caps the returned row count."""
        from provisa.executor.direct import execute_direct

        func = Function(
            name="provisa_test_action",
            source_id="test-pg",
            schema="public",
            function_name="provisa_test_action",
            returns="test-pg.public.orders",
            arguments=[
                FunctionArgument(name="p_region", type="String"),
                FunctionArgument(name="p_limit", type="Int"),
            ],
        )
        sql, params = build_function_sql(func, ["us-east", 1])
        result = await execute_direct(source_pool, "test-pg", sql, params)
        assert len(result.rows) <= 1
