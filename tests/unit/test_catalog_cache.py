# Copyright (c) 2026 Kenneth Stott
# Canary: f7a8b9c0-d1e2-3456-f012-789012345678
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for catalog_cache helpers (REQ-464)."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from provisa.discovery.catalog_cache import (
    CachedTable,
    _DDL,
    read_cache,
    write_cache,
    invalidate_source,
)


# ---------------------------------------------------------------------------
# DDL sanity
# ---------------------------------------------------------------------------


class TestDDL:
    def test_contains_table_name(self):
        assert "source_catalog_cache" in _DDL

    def test_primary_key(self):
        assert "PRIMARY KEY" in _DDL
        assert "source_id" in _DDL
        assert "table_name" in _DDL


# ---------------------------------------------------------------------------
# read_cache
# ---------------------------------------------------------------------------


class TestReadCache:
    @pytest.mark.asyncio
    async def test_returns_none_on_empty(self):
        pool = MagicMock()
        conn = AsyncMock()
        conn.fetch = AsyncMock(return_value=[])
        pool.acquire = MagicMock(
            return_value=AsyncMock(
                __aenter__=AsyncMock(return_value=conn), __aexit__=AsyncMock(return_value=False)
            )
        )

        result = await read_cache(pool, "src1", "public")
        assert result is None

    @pytest.mark.asyncio
    async def test_returns_cached_tables(self):
        pool = MagicMock()
        conn = AsyncMock()
        conn.fetch = AsyncMock(
            return_value=[
                {
                    "schema_name": "public",
                    "table_name": "orders",
                    "column_names": ["id", "amount"],
                    "comment": "All orders",
                },
                {
                    "schema_name": "public",
                    "table_name": "products",
                    "column_names": ["id", "sku"],
                    "comment": None,
                },
            ]
        )
        pool.acquire = MagicMock(
            return_value=AsyncMock(
                __aenter__=AsyncMock(return_value=conn), __aexit__=AsyncMock(return_value=False)
            )
        )

        result = await read_cache(pool, "src1", "public")
        assert result is not None
        assert len(result) == 2
        assert result[0].table_name == "orders"
        assert result[0].column_names == ["id", "amount"]
        assert result[0].comment == "All orders"
        assert result[1].comment is None

    @pytest.mark.asyncio
    async def test_passes_correct_args(self):
        pool = MagicMock()
        conn = AsyncMock()
        conn.fetch = AsyncMock(return_value=[])
        pool.acquire = MagicMock(
            return_value=AsyncMock(
                __aenter__=AsyncMock(return_value=conn), __aexit__=AsyncMock(return_value=False)
            )
        )

        await read_cache(pool, "my-source", "sales")
        conn.fetch.assert_called_once()
        args = conn.fetch.call_args[0]
        assert "my-source" in args
        assert "sales" in args


# ---------------------------------------------------------------------------
# write_cache
# ---------------------------------------------------------------------------


class TestWriteCache:
    @pytest.mark.asyncio
    async def test_no_op_on_empty(self):
        pool = MagicMock()
        await write_cache(pool, "src1", "public", [])
        pool.acquire.assert_not_called()
        assert pool.acquire.call_count == 0

    @pytest.mark.asyncio
    async def test_calls_executemany(self):
        pool = MagicMock()
        conn = AsyncMock()
        conn.executemany = AsyncMock()
        pool.acquire = MagicMock(
            return_value=AsyncMock(
                __aenter__=AsyncMock(return_value=conn), __aexit__=AsyncMock(return_value=False)
            )
        )

        tables = [
            CachedTable(
                schema_name="public", table_name="orders", column_names=["id"], comment=None
            ),
            CachedTable(
                schema_name="public",
                table_name="products",
                column_names=["id", "sku"],
                comment="Products",
            ),
        ]
        await write_cache(pool, "src1", "public", tables)
        conn.executemany.assert_called_once()
        _, rows = conn.executemany.call_args[0]
        assert len(rows) == 2
        assert rows[0][2] == "orders"
        assert rows[1][2] == "products"


# ---------------------------------------------------------------------------
# invalidate_source
# ---------------------------------------------------------------------------


class TestInvalidateSource:
    @pytest.mark.asyncio
    async def test_executes_delete(self):
        pool = MagicMock()
        conn = AsyncMock()
        conn.execute = AsyncMock()
        pool.acquire = MagicMock(
            return_value=AsyncMock(
                __aenter__=AsyncMock(return_value=conn), __aexit__=AsyncMock(return_value=False)
            )
        )

        await invalidate_source(pool, "src-to-delete")
        conn.execute.assert_called_once()
        sql, arg = conn.execute.call_args[0]
        assert "DELETE" in sql
        assert arg == "src-to-delete"
