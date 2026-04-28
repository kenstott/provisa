# Copyright (c) 2026 Kenneth Stott
# Canary: 9f3b1a2c-4d7e-4f8b-a1c6-2e5d0b9f3a7c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration tests for hot tables with real Redis.

Requires: docker-compose up redis

Tests the hot table lifecycle:
  Manual rows → HotTableManager.get_rows() → rewrite_hot_joins() CTE injection

These tests WILL FAIL if Redis is not running — that is intentional.
Integration tests require the full stack.
"""

from __future__ import annotations

import json
import os
import uuid

import pytest
import pytest_asyncio

from provisa.cache.hot_tables import HOT_PREFIX, HotTableManager

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/1")


@pytest_asyncio.fixture(scope="module")
async def hot_mgr():
    """Real HotTableManager connected to Redis. Fails if Redis is not running."""
    mgr = HotTableManager(
        redis_url=REDIS_URL,
        auto_threshold=1000,
        max_rows=5000,
    )
    await mgr._connect()
    # Verify connectivity — raises ConnectionError if Redis is down
    await mgr._redis.ping()
    yield mgr
    await mgr.close()


@pytest_asyncio.fixture(autouse=True, scope="module")
async def _clean_redis_keys(hot_mgr):
    """Wipe provisa:hot:ht_test_* keys before and after the module."""
    async def _wipe():
        async for key in hot_mgr._redis.scan_iter(match=HOT_PREFIX + "ht_test_*"):
            await hot_mgr._redis.delete(key)

    await _wipe()
    yield
    await _wipe()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestHotTableRedisRoundTrip:
    async def test_set_and_get_rows(self, hot_mgr):
        """Rows stored in Redis blob key are retrieved intact."""
        table = f"ht_test_{uuid.uuid4().hex[:8]}"
        rows = [{"id": 1, "name": "alpha"}, {"id": 2, "name": "beta"}]
        blob_key = HOT_PREFIX + table + ":blob"

        await hot_mgr._redis.set(blob_key, json.dumps(rows))

        fetched = await hot_mgr.get_rows(table)
        assert fetched == rows

    async def test_invalidate_removes_rows(self, hot_mgr):
        """Invalidating a hot table removes it from Redis."""
        table = f"ht_test_{uuid.uuid4().hex[:8]}"
        blob_key = HOT_PREFIX + table + ":blob"
        pk_key = HOT_PREFIX + table + ":pk:1"

        await hot_mgr._redis.set(blob_key, json.dumps([{"id": 1}]))
        await hot_mgr._redis.set(pk_key, json.dumps({"id": 1}))

        from provisa.cache.hot_tables import HotTableEntry
        hot_mgr._hot_tables[table] = HotTableEntry(
            table_name=table, catalog="c", schema="s", pk_column="id"
        )

        await hot_mgr.invalidate(table)

        assert not await hot_mgr._redis.exists(blob_key)
        assert not hot_mgr.is_hot(table)

    async def test_get_rows_miss_raises(self, hot_mgr):
        """Fetching a non-existent table raises KeyError."""
        with pytest.raises(KeyError, match="not found in Redis"):
            await hot_mgr.get_rows("ht_test_nonexistent_xyz")

    async def test_is_hot_reflects_loaded_state(self, hot_mgr):
        """is_hot() returns True only after an entry is registered."""
        table = f"ht_test_{uuid.uuid4().hex[:8]}"
        assert not hot_mgr.is_hot(table)

        from provisa.cache.hot_tables import HotTableEntry
        hot_mgr._hot_tables[table] = HotTableEntry(
            table_name=table, catalog="c", schema="s", pk_column="id",
            rows=[{"id": 1}], column_names=["id"],
        )
        assert hot_mgr.is_hot(table)
        del hot_mgr._hot_tables[table]


class TestHotTableCTERewrite:
    async def test_rewrite_hot_joins_injects_values_cte(self, hot_mgr):
        """rewrite_hot_joins() rewrites SQL to use a VALUES CTE when table is hot."""
        from provisa.cache.hot_tables import HotTableEntry
        from provisa.compiler.sql_gen import rewrite_hot_joins, CompiledQuery

        table = "ht_test_products"
        rows = [
            {"id": 1, "name": "Widget A", "price": 19.99},
            {"id": 2, "name": "Widget B", "price": 29.99},
        ]
        blob_key = HOT_PREFIX + table + ":blob"
        await hot_mgr._redis.set(blob_key, json.dumps(rows))
        hot_mgr._hot_tables[table] = HotTableEntry(
            table_name=table,
            catalog="postgresql",
            schema="public",
            pk_column="id",
            rows=rows,
            column_names=["id", "name", "price"],
        )

        try:
            sql = (
                'SELECT o.id, p.name FROM "public"."orders" o '
                'LEFT JOIN "public"."ht_test_products" "p" ON o.product_id = p.id'
            )
            compiled = CompiledQuery(
                sql=sql,
                params=[],
                root_field="orders",
                columns=[],
                sources={"postgresql"},
            )

            result = rewrite_hot_joins(compiled, hot_mgr)

            rewritten = result.sql.upper()
            assert "WITH" in rewritten or "VALUES" in rewritten, (
                f"Expected WITH/VALUES CTE in rewritten SQL, got: {result.sql}"
            )
        finally:
            del hot_mgr._hot_tables[table]
            await hot_mgr._redis.delete(blob_key)

    async def test_rewrite_skips_non_hot_tables(self, hot_mgr):
        """rewrite_hot_joins() leaves SQL unchanged when no hot tables match."""
        from provisa.compiler.sql_gen import rewrite_hot_joins, CompiledQuery

        sql = 'SELECT o.id FROM "public"."orders" o'
        compiled = CompiledQuery(
            sql=sql,
            params=[],
            root_field="orders",
            columns=[],
            sources={"postgresql"},
        )

        result = rewrite_hot_joins(compiled, hot_mgr)
        assert result.sql == sql
