# Copyright (c) 2026 Kenneth Stott
# Canary: eb21a5ee-7fac-4d13-9f18-850bb306603f
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for the direct execution layer and trino_write CTAS helpers.

execute_direct() delegates to SourcePool.execute(); these tests verify
that delegation, logging, and return-value handling work correctly using
DuckDB as a real in-process driver.
"""

from __future__ import annotations

import pytest

from provisa.executor.direct import execute_direct
from provisa.executor.pool import SourcePool
from provisa.executor.trino import QueryResult
from provisa.executor.trino_write import (
    RESULTS_BUCKET,
    RESULTS_CATALOG,
    RESULTS_SCHEMA,
    _iceberg_format,
    is_trino_native_format,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _duckdb_pool(source_id: str = "duck") -> SourcePool:
    """Create a SourcePool backed by an in-memory DuckDB."""
    sp = SourcePool()
    await sp.add(
        source_id,
        source_type="duckdb",
        host="",
        port=0,
        database=":memory:",
        user="",
        password="",
    )
    return sp


# ---------------------------------------------------------------------------
# execute_direct — delegation and return value
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
class TestExecuteDirect:
    async def test_returns_query_result(self):
        pool = await _duckdb_pool()
        result = await execute_direct(pool, "duck", "SELECT 1 AS n")
        assert isinstance(result, QueryResult)
        await pool.close_all()

    async def test_simple_select(self):
        pool = await _duckdb_pool()
        result = await execute_direct(pool, "duck", "SELECT 42 AS answer")
        assert result.rows == [(42,)]
        assert result.column_names == ["answer"]
        await pool.close_all()

    async def test_multi_column_select(self):
        pool = await _duckdb_pool()
        result = await execute_direct(pool, "duck", "SELECT 1 AS a, 'x' AS b")
        assert result.rows == [(1, "x")]
        assert result.column_names == ["a", "b"]
        await pool.close_all()

    async def test_multi_row_result(self):
        pool = await _duckdb_pool()
        sql = "SELECT * FROM (VALUES (1), (2), (3)) AS t(n)"
        result = await execute_direct(pool, "duck", sql)
        assert len(result.rows) == 3
        await pool.close_all()

    async def test_execute_with_params(self):
        pool = await _duckdb_pool()
        result = await execute_direct(pool, "duck", "SELECT $1 AS v", params=["hi"])
        assert result.rows == [("hi",)]
        await pool.close_all()

    async def test_execute_with_none_params(self):
        """Passing params=None is equivalent to no parameters."""
        pool = await _duckdb_pool()
        result = await execute_direct(pool, "duck", "SELECT 7 AS n", params=None)
        assert result.rows == [(7,)]
        await pool.close_all()

    async def test_raises_for_unknown_source(self):
        pool = SourcePool()  # empty pool
        with pytest.raises(KeyError):
            await execute_direct(pool, "nonexistent", "SELECT 1")

    async def test_delegates_to_pool_execute(self):
        """execute_direct must call pool.execute() — verified by tracking calls."""
        calls: list[tuple] = []

        class TrackingPool:
            async def execute(self, source_id, sql, params=None):
                calls.append((source_id, sql, params))
                return QueryResult(rows=[(1,)], column_names=["n"])

        result = await execute_direct(TrackingPool(), "src1", "SELECT 1", params=None)
        assert len(calls) == 1
        assert calls[0][0] == "src1"
        assert calls[0][1] == "SELECT 1"
        assert result.rows == [(1,)]

    async def test_delegates_params_to_pool(self):
        calls: list[tuple] = []

        class TrackingPool:
            async def execute(self, source_id, sql, params=None):
                calls.append((source_id, sql, params))
                return QueryResult(rows=[], column_names=[])

        await execute_direct(TrackingPool(), "src", "SELECT $1", params=["val"])
        assert calls[0][2] == ["val"]

    async def test_result_row_count_logged(self):
        """execute_direct must return the full result from pool.execute()."""
        pool = await _duckdb_pool()
        rows_sql = (
            "SELECT * FROM ("
            "VALUES (1,'a'), (2,'b'), (3,'c'), (4,'d'), (5,'e')"
            ") AS t(id, name)"
        )
        result = await execute_direct(pool, "duck", rows_sql)
        assert len(result.rows) == 5
        await pool.close_all()

    async def test_sql_executed_unchanged(self):
        """execute_direct does not modify SQL — driver receives the exact string."""
        received: list[str] = []

        class TrackingPool:
            async def execute(self, source_id, sql, params=None):
                received.append(sql)
                return QueryResult(rows=[], column_names=[])

        original_sql = 'SELECT "id" FROM "public"."orders" WHERE "status" = $1'
        await execute_direct(TrackingPool(), "pg", original_sql, params=["active"])
        assert received[0] == original_sql


# ---------------------------------------------------------------------------
# trino_write constants and helpers
# ---------------------------------------------------------------------------

class TestTrinoWriteConstants:
    def test_results_catalog(self):
        assert RESULTS_CATALOG == "results"

    def test_results_schema(self):
        assert RESULTS_SCHEMA == "provisa_results"

    def test_results_bucket(self):
        assert RESULTS_BUCKET == "provisa-results"


class TestIcebergFormatHelper:
    def test_parquet_to_uppercase(self):
        assert _iceberg_format("parquet") == "PARQUET"

    def test_orc_to_uppercase(self):
        assert _iceberg_format("orc") == "ORC"

    def test_already_uppercase_stays(self):
        assert _iceberg_format("PARQUET") == "PARQUET"


class TestIsTrinoNativeFormat:
    def test_parquet_native(self):
        assert is_trino_native_format("parquet") is True

    def test_orc_native(self):
        assert is_trino_native_format("orc") is True

    def test_json_not_native(self):
        assert is_trino_native_format("json") is False

    def test_csv_not_native(self):
        assert is_trino_native_format("csv") is False

    def test_arrow_not_native(self):
        assert is_trino_native_format("arrow") is False

    def test_ndjson_not_native(self):
        assert is_trino_native_format("ndjson") is False

    def test_empty_string_not_native(self):
        assert is_trino_native_format("") is False

    def test_case_insensitive_parquet(self):
        assert is_trino_native_format("PARQUET") is True

    def test_case_insensitive_orc(self):
        assert is_trino_native_format("ORC") is True
