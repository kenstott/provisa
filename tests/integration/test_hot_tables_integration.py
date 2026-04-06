# Copyright (c) 2026 Kenneth Stott
# Canary: b3c4d5e6-f7a8-9012-cdef-123456789012
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration tests for hot table VALUES CTE injection (REQ-232, Phase AD6)."""

from __future__ import annotations

import os
from unittest.mock import MagicMock, AsyncMock, patch

import pytest
import pytest_asyncio

from provisa.cache.hot_tables import (
    HotTableEntry,
    HotTableManager,
    detect_hot_tables,
)
from provisa.compiler.sql_gen import (
    ColumnRef,
    CompilationContext,
    CompiledQuery,
    TableMeta,
    rewrite_hot_joins,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

# ---------------------------------------------------------------------------
# Helpers / builders
# ---------------------------------------------------------------------------

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379")
REDIS_AVAILABLE = False

try:
    import redis
    REDIS_AVAILABLE = True
except ImportError:
    pass


def _make_hot_manager(auto_threshold: int = 1000) -> HotTableManager:
    return HotTableManager(
        redis_url=REDIS_URL,
        auto_threshold=auto_threshold,
        max_rows=auto_threshold,
    )


def _make_trino_conn_mock(rows: list[tuple], columns: list[str]) -> MagicMock:
    """Return a synchronous Trino connection mock."""
    description = [(c,) for c in columns]
    cur = MagicMock()
    cur.execute = MagicMock()
    cur.fetchall = MagicMock(return_value=rows)
    cur.description = description
    conn = MagicMock()
    conn.cursor = MagicMock(return_value=cur)
    return conn


def _make_compiled(sql: str, root_field: str = "orders") -> CompiledQuery:
    return CompiledQuery(
        sql=sql,
        params=[],
        root_field=root_field,
        columns=[ColumnRef(alias="t0", column="id", field_name="id", nested_in=None)],
        sources={"test-pg"},
    )


def _make_manager_with_entry(table_name: str, rows: list[dict], column_names: list[str]) -> HotTableManager:
    """Build a HotTableManager with a pre-loaded in-memory entry (no Redis)."""
    mgr = _make_hot_manager()
    entry = HotTableEntry(
        table_name=table_name,
        catalog="test_pg",
        schema="public",
        pk_column="id",
        rows=rows,
        column_names=column_names,
    )
    mgr._hot_tables[table_name] = entry
    return mgr


# ---------------------------------------------------------------------------
# Tests: loading from Trino (mocked)
# ---------------------------------------------------------------------------

class TestHotTableLoading:
    async def test_hot_table_loaded_from_trino(self):
        """HotTableManager.load_table populates in-memory entry from Trino cursor."""
        mgr = _make_hot_manager()
        trino_rows = [(1, "Widget"), (2, "Gadget")]
        trino_cols = ["id", "name"]
        conn = _make_trino_conn_mock(trino_rows, trino_cols)

        # Mock Redis pipeline so we avoid a real Redis connection
        mock_redis = AsyncMock()
        mock_pipe = AsyncMock()
        mock_pipe.delete = MagicMock()
        mock_pipe.set = MagicMock()
        mock_pipe.execute = AsyncMock(return_value=[True, True, True])
        mock_redis.pipeline = MagicMock(return_value=mock_pipe)
        mgr._redis = mock_redis

        count = await mgr.load_table(conn, "products", "public", "test_pg", "id")
        assert count == 2
        assert mgr.is_hot("products")
        entry = mgr.get_entry("products")
        assert entry is not None
        assert entry.column_names == trino_cols
        assert len(entry.rows) == 2
        assert entry.rows[0]["id"] == 1
        assert entry.rows[1]["name"] == "Gadget"

    async def test_hot_table_skipped_when_exceeds_max_rows(self):
        """load_table returns early when row count exceeds max_rows."""
        mgr = HotTableManager(redis_url=REDIS_URL, auto_threshold=2, max_rows=2)
        trino_rows = [(i, f"item{i}") for i in range(10)]
        conn = _make_trino_conn_mock(trino_rows, ["id", "name"])

        mock_redis = AsyncMock()
        mgr._redis = mock_redis

        count = await mgr.load_table(conn, "big_table", "public", "test_pg", "id")
        assert count == 10
        assert not mgr.is_hot("big_table")

    async def test_hot_table_cached_in_redis(self):
        """After load_table, get_rows retrieves rows via Redis blob key."""
        import json

        mgr = _make_hot_manager()
        stored_rows = [{"id": 1, "name": "Alpha"}, {"id": 2, "name": "Beta"}]
        serialized = json.dumps(stored_rows)

        # Simulate Redis returning the blob
        mock_redis = AsyncMock()
        mock_redis.get = AsyncMock(return_value=serialized)
        mgr._redis = mock_redis

        rows = await mgr.get_rows("products")
        assert len(rows) == 2
        assert rows[0]["name"] == "Alpha"
        assert rows[1]["id"] == 2

    async def test_hot_table_get_rows_fallback_to_memory(self):
        """get_rows falls back to in-memory entry when Redis returns None."""
        mgr = _make_manager_with_entry(
            "products",
            [{"id": 1, "name": "Zing"}],
            ["id", "name"],
        )
        # Redis returns None — triggers in-memory fallback
        mock_redis = AsyncMock()
        mock_redis.get = AsyncMock(return_value=None)
        mgr._redis = mock_redis

        rows = await mgr.get_rows("products")
        assert rows == [{"id": 1, "name": "Zing"}]

    async def test_hot_table_not_found_raises_key_error(self):
        """get_rows raises KeyError when table is not in Redis or memory."""
        mgr = _make_hot_manager()
        mock_redis = AsyncMock()
        mock_redis.get = AsyncMock(return_value=None)
        mgr._redis = mock_redis

        with pytest.raises(KeyError, match="missing_table"):
            await mgr.get_rows("missing_table")


# ---------------------------------------------------------------------------
# Tests: SQL rewriting
# ---------------------------------------------------------------------------

class TestHotJoinRewriting:
    def test_hot_join_rewritten_to_cte(self):
        """rewrite_hot_joins rewrites a LEFT JOIN on a hot table to a VALUES CTE."""
        mgr = _make_manager_with_entry(
            "customers",
            [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}],
            ["id", "name"],
        )
        sql = (
            'SELECT "t0"."id", "t1"."name" '
            'FROM "public"."orders" "t0" '
            'LEFT JOIN "public"."customers" "t1" ON "t0"."customer_id" = "t1"."id"'
        )
        compiled = _make_compiled(sql)
        result = rewrite_hot_joins(compiled, mgr)

        assert "WITH" in result.sql.upper()
        assert "_hot_customers" in result.sql
        assert "VALUES" in result.sql.upper()
        # Original table reference replaced with CTE
        assert '"public"."customers"' not in result.sql

    def test_hot_cte_contains_correct_values(self):
        """The VALUES rows in the CTE match the hot table data."""
        mgr = _make_manager_with_entry(
            "customers",
            [{"id": 1, "name": "Alice"}],
            ["id", "name"],
        )
        sql = (
            'SELECT "t0"."id" FROM "public"."orders" "t0" '
            'LEFT JOIN "public"."customers" "t1" ON "t0"."customer_id" = "t1"."id"'
        )
        result = rewrite_hot_joins(_make_compiled(sql), mgr)
        assert "Alice" in result.sql
        assert "1" in result.sql

    def test_non_hot_table_not_rewritten(self):
        """A LEFT JOIN targeting a non-hot table is left unchanged."""
        mgr = _make_hot_manager()  # empty — no hot tables
        sql = (
            'SELECT "t0"."id" FROM "public"."orders" "t0" '
            'LEFT JOIN "public"."customers" "t1" ON "t0"."customer_id" = "t1"."id"'
        )
        compiled = _make_compiled(sql)
        result = rewrite_hot_joins(compiled, mgr)
        assert result.sql == sql
        assert "WITH" not in result.sql.upper()

    def test_multiple_hot_joins_all_rewritten(self):
        """When two hot tables appear in JOINs, both are rewritten to CTEs."""
        mgr = _make_manager_with_entry(
            "customers",
            [{"id": 1, "name": "Alice"}],
            ["id", "name"],
        )
        mgr._hot_tables["products"] = HotTableEntry(
            table_name="products",
            catalog="test_pg",
            schema="public",
            pk_column="id",
            rows=[{"id": 10, "label": "Widget"}],
            column_names=["id", "label"],
        )

        sql = (
            'SELECT "t0"."id" FROM "public"."orders" "t0" '
            'LEFT JOIN "public"."customers" "t1" ON "t0"."customer_id" = "t1"."id" '
            'LEFT JOIN "public"."products" "t2" ON "t0"."product_id" = "t2"."id"'
        )
        result = rewrite_hot_joins(_make_compiled(sql), mgr)
        assert "_hot_customers" in result.sql
        assert "_hot_products" in result.sql
        assert "WITH" in result.sql.upper()

    def test_hot_join_preserves_params(self):
        """rewrite_hot_joins does not drop query params from the CompiledQuery."""
        mgr = _make_manager_with_entry(
            "customers",
            [{"id": 1, "name": "Alice"}],
            ["id", "name"],
        )
        sql = (
            'SELECT "t0"."id" FROM "public"."orders" "t0" '
            'LEFT JOIN "public"."customers" "t1" ON "t0"."customer_id" = "t1"."id" '
            'WHERE "t0"."region" = $1'
        )
        compiled = CompiledQuery(
            sql=sql,
            params=["us-east"],
            root_field="orders",
            columns=[],
            sources={"test-pg"},
        )
        result = rewrite_hot_joins(compiled, mgr)
        assert result.params == ["us-east"]


# ---------------------------------------------------------------------------
# Tests: detect_hot_tables helper
# ---------------------------------------------------------------------------

class TestDetectHotTables:
    def test_many_to_one_target_auto_detected(self):
        tables = [{"table_name": "customers"}, {"table_name": "orders"}]
        relationships = [{"cardinality": "many-to-one", "target_table_id": "customers"}]
        result = detect_hot_tables(tables, relationships, {})
        assert "customers" in result

    def test_one_to_many_not_auto_detected(self):
        tables = [{"table_name": "orders"}]
        relationships = [{"cardinality": "one-to-many", "target_table_id": "orders"}]
        result = detect_hot_tables(tables, relationships, {})
        assert "orders" not in result

    def test_explicit_override_true_forces_inclusion(self):
        tables = [{"table_name": "reference_data"}]
        relationships = []
        result = detect_hot_tables(tables, relationships, {"reference_data": True})
        assert "reference_data" in result

    def test_explicit_override_false_excludes_table(self):
        tables = [{"table_name": "customers"}]
        relationships = [{"cardinality": "many-to-one", "target_table_id": "customers"}]
        result = detect_hot_tables(tables, relationships, {"customers": False})
        assert "customers" not in result
