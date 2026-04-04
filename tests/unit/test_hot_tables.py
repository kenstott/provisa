# Copyright (c) 2025 Kenneth Stott
# Canary: 0ac6ab9b-b620-4896-876b-0ab19affcbb2
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for hot tables (Phase AD6)."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from provisa.cache.hot_tables import (
    HotTableEntry,
    HotTableManager,
    detect_hot_tables,
)
from provisa.compiler.sql_gen import (
    ColumnRef,
    CompiledQuery,
    _sql_literal,
    rewrite_hot_joins,
)


# --- detect_hot_tables ---


class TestDetectHotTables:
    def test_auto_detect_many_to_one_target(self):
        tables = [
            {"table_name": "orders"},
            {"table_name": "countries"},
        ]
        relationships = [
            {
                "source_table_id": "orders",
                "target_table_id": "countries",
                "cardinality": "many-to-one",
            },
        ]
        result = detect_hot_tables(tables, relationships, {})
        assert "countries" in result
        assert "orders" not in result

    def test_hot_false_opts_out(self):
        tables = [
            {"table_name": "countries"},
        ]
        relationships = [
            {
                "source_table_id": "orders",
                "target_table_id": "countries",
                "cardinality": "many-to-one",
            },
        ]
        result = detect_hot_tables(tables, relationships, {"countries": False})
        assert result == []

    def test_hot_true_forces_in(self):
        tables = [
            {"table_name": "big_table"},
        ]
        # No relationships — would not auto-detect
        relationships = []
        result = detect_hot_tables(tables, relationships, {"big_table": True})
        assert "big_table" in result

    def test_hot_none_auto_detects(self):
        tables = [
            {"table_name": "statuses"},
        ]
        relationships = [
            {
                "source_table_id": "tickets",
                "target_table_id": "statuses",
                "cardinality": "many-to-one",
            },
        ]
        result = detect_hot_tables(tables, relationships, {"statuses": None})
        assert "statuses" in result

    def test_no_relationship_not_auto_detected(self):
        tables = [
            {"table_name": "standalone"},
        ]
        relationships = []
        result = detect_hot_tables(tables, relationships, {})
        assert result == []

    def test_one_to_many_not_auto_detected(self):
        """One-to-many targets are not auto-detected (only many-to-one)."""
        tables = [
            {"table_name": "orders"},
            {"table_name": "line_items"},
        ]
        relationships = [
            {
                "source_table_id": "orders",
                "target_table_id": "line_items",
                "cardinality": "one-to-many",
            },
        ]
        result = detect_hot_tables(tables, relationships, {})
        assert result == []


# --- HotTableManager ---


class TestHotTableManager:
    @pytest.fixture
    def manager(self):
        return HotTableManager(
            redis_url="redis://localhost:6379",
            auto_threshold=10_000,
            max_rows=10_000,
        )

    @pytest.mark.asyncio
    async def test_load_table_stores_rows(self, manager):
        # Mock Redis — pipeline() is synchronous, returns object with async execute
        mock_redis = MagicMock()
        mock_pipe = MagicMock()
        mock_pipe.execute = AsyncMock(return_value=[])
        mock_redis.pipeline.return_value = mock_pipe
        manager._redis = mock_redis

        # Mock Trino cursor
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            (1, "US", "United States"),
            (2, "UK", "United Kingdom"),
        ]
        mock_cursor.description = [
            ("id",), ("code",), ("name",),
        ]
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        count = await manager.load_table(
            mock_conn, "countries", "public", "pg", "id",
        )
        assert count == 2
        assert manager.is_hot("countries")
        entry = manager.get_entry("countries")
        assert entry is not None
        assert len(entry.rows) == 2
        assert entry.column_names == ["id", "code", "name"]

    @pytest.mark.asyncio
    async def test_max_rows_guard_skips_large_table(self, manager):
        mock_redis = AsyncMock()
        mock_pipe = AsyncMock()
        mock_redis.pipeline.return_value = mock_pipe
        mock_pipe.execute = AsyncMock(return_value=[])
        manager._redis = mock_redis

        # Return more rows than max_rows
        rows = [(i, f"code_{i}") for i in range(10_001)]
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = rows
        mock_cursor.description = [("id",), ("code",)]
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        count = await manager.load_table(
            mock_conn, "big_countries", "public", "pg", "id",
        )
        assert count == 10_001
        assert not manager.is_hot("big_countries")

    @pytest.mark.asyncio
    async def test_invalidate_removes_table(self, manager):
        # Pre-populate
        manager._hot_tables["countries"] = HotTableEntry(
            table_name="countries",
            catalog="pg",
            schema="public",
            pk_column="id",
            rows=[{"id": 1, "code": "US"}],
            column_names=["id", "code"],
        )

        mock_redis = AsyncMock()
        mock_redis.delete = AsyncMock(return_value=1)

        async def _empty_scan(*args, **kwargs):
            return
            yield  # noqa: make it an async generator

        mock_redis.scan_iter = _empty_scan
        manager._redis = mock_redis

        await manager.invalidate("countries")
        assert not manager.is_hot("countries")

    @pytest.mark.asyncio
    async def test_get_rows_from_redis(self, manager):
        import json
        rows = [{"id": 1, "name": "US"}, {"id": 2, "name": "UK"}]
        mock_redis = AsyncMock()
        mock_redis.get = AsyncMock(return_value=json.dumps(rows))
        manager._redis = mock_redis

        result = await manager.get_rows("countries")
        assert len(result) == 2
        assert result[0]["name"] == "US"

    @pytest.mark.asyncio
    async def test_get_rows_falls_back_to_memory(self, manager):
        mock_redis = AsyncMock()
        mock_redis.get = AsyncMock(return_value=None)
        manager._redis = mock_redis

        manager._hot_tables["countries"] = HotTableEntry(
            table_name="countries",
            catalog="pg",
            schema="public",
            pk_column="id",
            rows=[{"id": 1, "name": "US"}],
            column_names=["id", "name"],
        )
        result = await manager.get_rows("countries")
        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_get_rows_raises_when_not_found(self, manager):
        mock_redis = AsyncMock()
        mock_redis.get = AsyncMock(return_value=None)
        manager._redis = mock_redis

        with pytest.raises(KeyError, match="not found"):
            await manager.get_rows("nonexistent")


# --- VALUES CTE rewrite ---


class TestRewriteHotJoins:
    def _make_manager(self, hot_entries: dict[str, HotTableEntry]) -> HotTableManager:
        mgr = HotTableManager.__new__(HotTableManager)
        mgr._redis_url = ""
        mgr._auto_threshold = 10_000
        mgr._max_rows = 10_000
        mgr._redis = None
        mgr._hot_tables = hot_entries
        return mgr

    def test_rewrites_hot_join_to_values_cte(self):
        entry = HotTableEntry(
            table_name="countries",
            catalog="pg",
            schema="public",
            pk_column="id",
            rows=[
                {"id": 1, "code": "US"},
                {"id": 2, "code": "UK"},
            ],
            column_names=["id", "code"],
        )
        mgr = self._make_manager({"countries": entry})

        compiled = CompiledQuery(
            sql=(
                'SELECT "t0"."order_id", "t1"."code" '
                'FROM "public"."orders" "t0" '
                'LEFT JOIN "public"."countries" "t1" '
                'ON "t0"."country_id" = "t1"."id"'
            ),
            params=[],
            root_field="orders",
            columns=[
                ColumnRef(alias="t0", column="order_id", field_name="order_id", nested_in=None),
                ColumnRef(alias="t1", column="code", field_name="code", nested_in="countries"),
            ],
            sources={"pg"},
        )

        result = rewrite_hot_joins(compiled, mgr)
        assert "WITH _hot_countries" in result.sql
        assert "VALUES" in result.sql
        assert "(1, 'US')" in result.sql
        assert "(2, 'UK')" in result.sql
        assert 'LEFT JOIN "_hot_countries"' in result.sql

    def test_no_rewrite_when_not_hot(self):
        mgr = self._make_manager({})

        compiled = CompiledQuery(
            sql=(
                'SELECT "t0"."id" FROM "public"."orders" "t0" '
                'LEFT JOIN "public"."countries" "t1" '
                'ON "t0"."country_id" = "t1"."id"'
            ),
            params=[],
            root_field="orders",
            columns=[],
            sources={"pg"},
        )

        result = rewrite_hot_joins(compiled, mgr)
        assert result.sql == compiled.sql  # unchanged

    def test_no_rewrite_when_no_joins(self):
        entry = HotTableEntry(
            table_name="countries",
            catalog="pg",
            schema="public",
            pk_column="id",
            rows=[{"id": 1, "code": "US"}],
            column_names=["id", "code"],
        )
        mgr = self._make_manager({"countries": entry})

        compiled = CompiledQuery(
            sql='SELECT "id", "name" FROM "public"."orders"',
            params=[],
            root_field="orders",
            columns=[],
            sources={"pg"},
        )

        result = rewrite_hot_joins(compiled, mgr)
        assert result.sql == compiled.sql  # no joins to rewrite


# --- _sql_literal ---


class TestSqlLiteral:
    def test_none(self):
        assert _sql_literal(None) == "NULL"

    def test_bool_true(self):
        assert _sql_literal(True) == "TRUE"

    def test_bool_false(self):
        assert _sql_literal(False) == "FALSE"

    def test_int(self):
        assert _sql_literal(42) == "42"

    def test_float(self):
        assert _sql_literal(3.14) == "3.14"

    def test_string(self):
        assert _sql_literal("hello") == "'hello'"

    def test_string_with_quotes(self):
        assert _sql_literal("it's") == "'it''s'"


# --- Model config ---


class TestHotTableModelField:
    def test_table_hot_field_default_none(self):
        from provisa.core.models import Table
        t = Table(
            source_id="pg",
            domain_id="test",
            schema_name="public",
            table_name="orders",
            governance="pre-approved",
            columns=[],
        )
        assert t.hot is None

    def test_table_hot_true(self):
        from provisa.core.models import Table
        t = Table(
            source_id="pg",
            domain_id="test",
            schema_name="public",
            table_name="countries",
            governance="pre-approved",
            columns=[],
            hot=True,
        )
        assert t.hot is True

    def test_table_hot_false(self):
        from provisa.core.models import Table
        t = Table(
            source_id="pg",
            domain_id="test",
            schema_name="public",
            table_name="big_table",
            governance="pre-approved",
            columns=[],
            hot=False,
        )
        assert t.hot is False


class TestHotTablesConfig:
    def test_defaults(self):
        from provisa.core.models import HotTablesConfig
        cfg = HotTablesConfig()
        assert cfg.auto_threshold == 10_000
        assert cfg.refresh_interval == 300

    def test_custom(self):
        from provisa.core.models import HotTablesConfig
        cfg = HotTablesConfig(auto_threshold=5_000, refresh_interval=60)
        assert cfg.auto_threshold == 5_000
        assert cfg.refresh_interval == 60


# --- Mutation invalidation (integration-style mock test) ---


class TestMutationInvalidation:
    @pytest.mark.asyncio
    async def test_invalidate_called_on_mutation(self):
        mgr = HotTableManager.__new__(HotTableManager)
        mgr._redis_url = ""
        mgr._auto_threshold = 10_000
        mgr._max_rows = 10_000
        mgr._redis = AsyncMock()
        mgr._hot_tables = {
            "countries": HotTableEntry(
                table_name="countries",
                catalog="pg",
                schema="public",
                pk_column="id",
                rows=[{"id": 1, "code": "US"}],
                column_names=["id", "code"],
            ),
        }

        assert mgr.is_hot("countries")

        # Simulate invalidation
        mgr._redis.delete = AsyncMock(return_value=1)

        async def _empty_scan(*args, **kwargs):
            return
            yield

        mgr._redis.scan_iter = _empty_scan

        await mgr.invalidate("countries")
        assert not mgr.is_hot("countries")
