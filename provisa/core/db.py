# Copyright (c) 2025 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""asyncpg connection pool factory."""

import asyncpg


async def create_pool(
    host: str,
    port: int,
    database: str,
    user: str,
    password: str,
    min_size: int = 2,
    max_size: int = 10,
) -> asyncpg.Pool:
    return await asyncpg.create_pool(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password,
        min_size=min_size,
        max_size=max_size,
    )


async def init_schema(pool: asyncpg.Pool, schema_sql: str) -> None:
    """Execute schema SQL against the pool."""
    async with pool.acquire() as conn:
        await conn.execute(schema_sql)
