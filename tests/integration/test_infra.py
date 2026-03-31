# Copyright (c) 2025 Kenneth Stott
# Canary: ce341f4e-0d36-4279-aec3-075183c061ca
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Infrastructure integration tests — PG + Trino connectivity and sample data."""

import pytest

pytestmark = pytest.mark.integration


class TestPostgresConnectivity:
    def test_pg_connects(self, pg_pool):
        import asyncio

        async def _query():
            async with pg_pool.acquire() as conn:
                return await conn.fetchval("SELECT 1")

        result = asyncio.get_event_loop().run_until_complete(_query())
        assert result == 1

    def test_customers_table_exists(self, pg_pool):
        import asyncio

        async def _query():
            async with pg_pool.acquire() as conn:
                return await conn.fetchval(
                    "SELECT COUNT(*) FROM customers"
                )

        count = asyncio.get_event_loop().run_until_complete(_query())
        assert count == 20

    def test_products_table_exists(self, pg_pool):
        import asyncio

        async def _query():
            async with pg_pool.acquire() as conn:
                return await conn.fetchval(
                    "SELECT COUNT(*) FROM products"
                )

        count = asyncio.get_event_loop().run_until_complete(_query())
        assert count == 15

    def test_orders_table_exists(self, pg_pool):
        import asyncio

        async def _query():
            async with pg_pool.acquire() as conn:
                return await conn.fetchval(
                    "SELECT COUNT(*) FROM orders"
                )

        count = asyncio.get_event_loop().run_until_complete(_query())
        assert count == 25

    def test_orders_fk_to_customers(self, pg_pool):
        import asyncio

        async def _query():
            async with pg_pool.acquire() as conn:
                return await conn.fetchval("""
                    SELECT COUNT(*)
                    FROM orders o
                    JOIN customers c ON o.customer_id = c.id
                """)

        count = asyncio.get_event_loop().run_until_complete(_query())
        assert count == 25


class TestTrinoConnectivity:
    def test_trino_connects(self, trino_conn):
        cur = trino_conn.cursor()
        cur.execute("SELECT 1")
        result = cur.fetchone()
        assert result[0] == 1

    def test_trino_queries_pg_customers(self, trino_conn):
        cur = trino_conn.cursor()
        cur.execute("SELECT COUNT(*) FROM postgresql.public.customers")
        result = cur.fetchone()
        assert result[0] == 20

    def test_trino_queries_pg_orders(self, trino_conn):
        cur = trino_conn.cursor()
        cur.execute("SELECT COUNT(*) FROM postgresql.public.orders")
        result = cur.fetchone()
        assert result[0] == 25

    def test_trino_information_schema_columns(self, trino_conn):
        cur = trino_conn.cursor()
        cur.execute("""
            SELECT column_name, data_type
            FROM postgresql.information_schema.columns
            WHERE table_schema = 'public' AND table_name = 'orders'
            ORDER BY ordinal_position
        """)
        rows = cur.fetchall()
        column_names = [r[0] for r in rows]
        assert "id" in column_names
        assert "customer_id" in column_names
        assert "amount" in column_names
        assert "region" in column_names

    def test_trino_cross_join_pg_tables(self, trino_conn):
        cur = trino_conn.cursor()
        cur.execute("""
            SELECT o.id, c.name
            FROM postgresql.public.orders o
            JOIN postgresql.public.customers c ON o.customer_id = c.id
            LIMIT 5
        """)
        rows = cur.fetchall()
        assert len(rows) == 5
        assert all(isinstance(r[1], str) for r in rows)
