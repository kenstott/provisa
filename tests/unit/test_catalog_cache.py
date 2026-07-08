# Copyright (c) 2026 Kenneth Stott
# Canary: 8c1d0f2a-6b4e-4a71-9f3c-2d5e7a9b1c04
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-464: source catalog cache read/write, migrated to SQLAlchemy Core."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from provisa.core.schema_org import source_catalog_cache
from provisa.discovery.catalog_cache import (
    CachedTable,
    invalidate_source,
    read_cache,
    write_cache,
)


def _pool_with_conn(conn) -> MagicMock:
    """A pool whose acquire() yields the given (async) conn, and conn.transaction() is an async CM."""
    conn.transaction = MagicMock(
        return_value=AsyncMock(
            __aenter__=AsyncMock(return_value=None), __aexit__=AsyncMock(return_value=False)
        )
    )
    pool = MagicMock()
    pool.acquire = MagicMock(
        return_value=AsyncMock(
            __aenter__=AsyncMock(return_value=conn), __aexit__=AsyncMock(return_value=False)
        )
    )
    return pool


def _result(rows) -> MagicMock:
    result = MagicMock()
    result.fetchall.return_value = [MagicMock(_mapping=r) for r in rows]
    return result


# ---------------------------------------------------------------------------
# schema (metadata-authoritative — no raw DDL string)
# ---------------------------------------------------------------------------


class TestSchema:
    def test_table_name(self):
        assert source_catalog_cache.name == "source_catalog_cache"

    def test_primary_key(self):
        pk = {c.name for c in source_catalog_cache.primary_key.columns}
        assert pk == {"source_id", "schema_name", "table_name"}


# ---------------------------------------------------------------------------
# read_cache
# ---------------------------------------------------------------------------


class TestReadCache:
    @pytest.mark.asyncio
    async def test_returns_none_on_empty(self):
        conn = AsyncMock()
        conn.execute_core = AsyncMock(return_value=_result([]))
        result = await read_cache(_pool_with_conn(conn), "src1", "public")
        assert result is None

    @pytest.mark.asyncio
    async def test_returns_cached_tables(self):
        conn = AsyncMock()
        conn.execute_core = AsyncMock(
            return_value=_result(
                [
                    {
                        "source_id": "src1",
                        "schema_name": "public",
                        "table_name": "orders",
                        "column_names": ["id", "amount"],
                        "comment": "All orders",
                        "indexed_at": None,
                    },
                    {
                        "source_id": "src1",
                        "schema_name": "public",
                        "table_name": "products",
                        "column_names": ["id", "sku"],
                        "comment": None,
                        "indexed_at": None,
                    },
                ]
            )
        )
        result = await read_cache(_pool_with_conn(conn), "src1", "public")
        assert result is not None
        assert len(result) == 2
        assert result[0].table_name == "orders"
        assert result[0].column_names == ["id", "amount"]
        assert result[0].comment == "All orders"
        assert result[1].comment is None

    @pytest.mark.asyncio
    async def test_filters_by_source_and_schema(self):
        conn = AsyncMock()
        conn.execute_core = AsyncMock(return_value=_result([]))
        await read_cache(_pool_with_conn(conn), "my-source", "sales")
        stmt = conn.execute_core.await_args.args[0]
        compiled = str(stmt.compile(compile_kwargs={"literal_binds": True}))
        assert "my-source" in compiled
        assert "sales" in compiled


# ---------------------------------------------------------------------------
# write_cache
# ---------------------------------------------------------------------------


class TestWriteCache:
    @pytest.mark.asyncio
    async def test_no_op_on_empty(self):
        pool = MagicMock()
        await write_cache(pool, "src1", "public", [])
        assert pool.acquire.call_count == 0

    @pytest.mark.asyncio
    async def test_upserts_each_table(self):
        conn = AsyncMock()
        conn.upsert = AsyncMock()
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
        await write_cache(_pool_with_conn(conn), "src1", "public", tables)
        assert conn.upsert.await_count == 2
        upserted = [c.args[1]["table_name"] for c in conn.upsert.await_args_list]
        assert upserted == ["orders", "products"]


# ---------------------------------------------------------------------------
# invalidate_source
# ---------------------------------------------------------------------------


class TestInvalidateSource:
    @pytest.mark.asyncio
    async def test_executes_delete(self):
        conn = AsyncMock()
        conn.execute_core = AsyncMock()
        await invalidate_source(_pool_with_conn(conn), "src-to-delete")
        conn.execute_core.assert_awaited_once()
        stmt = conn.execute_core.await_args.args[0]
        compiled = str(stmt.compile(compile_kwargs={"literal_binds": True}))
        assert "DELETE" in compiled.upper()
        assert "src-to-delete" in compiled
