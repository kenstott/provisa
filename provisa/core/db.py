# Copyright (c) 2026 Kenneth Stott
# Canary: ad492cac-4438-4e3a-88d8-315e26a58491
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""asyncpg connection pool factory."""

import asyncio
import json

import asyncpg


def _json_encoder(v):
    return v if isinstance(v, str) else json.dumps(v)


async def _init_conn(conn: asyncpg.Connection) -> None:
    await conn.set_type_codec(
        "jsonb", encoder=_json_encoder, decoder=json.loads, schema="pg_catalog"
    )
    await conn.set_type_codec(
        "json", encoder=_json_encoder, decoder=json.loads, schema="pg_catalog"
    )


async def create_pool(
    host: str,
    port: int,
    database: str,
    user: str,
    password: str,
    min_size: int = 2,
    max_size: int = 10,
) -> asyncpg.Pool:
    return await asyncio.wait_for(
        asyncpg.create_pool(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password,
            min_size=min_size,
            max_size=max_size,
            init=_init_conn,
        ),
        timeout=10,
    )


async def init_schema(pool: asyncpg.Pool, schema_sql: str) -> None:
    """Execute schema SQL against the pool."""
    async with pool.acquire() as conn:
        await conn.execute("SELECT pg_advisory_lock(7337)")
        try:
            await conn.execute(schema_sql)
        finally:
            await conn.execute("SELECT pg_advisory_unlock(7337)")
