# Copyright (c) 2026 Kenneth Stott
# Canary: d7bb11a2-a6e2-4758-bfdc-ac1d874d3161
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""MySQL direct driver using aiomysql."""

from __future__ import annotations

import aiomysql

from provisa.executor.drivers.base import DirectDriver
from provisa.executor.trino import QueryResult


class MySQLDriver(DirectDriver):
    def __init__(self) -> None:
        self._pool: aiomysql.Pool | None = None

    async def connect(
        self, host: str, port: int, database: str,
        user: str, password: str, min_pool: int = 1, max_pool: int = 5,
    ) -> None:
        self._pool = await aiomysql.create_pool(
            host=host, port=port, db=database,
            user=user, password=password,
            minsize=min_pool, maxsize=max_pool,
        )

    async def execute(self, sql: str, params: list | None = None) -> QueryResult:
        # aiomysql uses %s placeholders — convert $N to %s
        exec_sql = sql
        if params:
            for i in range(len(params), 0, -1):
                exec_sql = exec_sql.replace(f"${i}", "%s")

        async with self._pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(exec_sql, params or ())
                rows = await cur.fetchall()
                columns = [desc[0] for desc in cur.description] if cur.description else []
                return QueryResult(rows=list(rows), column_names=columns)

    async def close(self) -> None:
        if self._pool:
            self._pool.close()
            await self._pool.wait_closed()
            self._pool = None

    @property
    def is_connected(self) -> bool:
        return self._pool is not None
