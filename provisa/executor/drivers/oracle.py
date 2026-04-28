# Copyright (c) 2026 Kenneth Stott
# Canary: 20681b37-73be-4a88-b752-108c3837d4d8
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Oracle direct driver using oracledb (python-oracledb thin mode — no Oracle Client needed)."""

from __future__ import annotations

import asyncio

import oracledb

from provisa.executor.drivers.base import DirectDriver
from provisa.executor.trino import QueryResult


class OracleDriver(DirectDriver):
    def __init__(self) -> None:
        self._pool: oracledb.AsyncConnectionPool | None = None

    async def connect(
        self, host: str, port: int, database: str,
        user: str, password: str, min_pool: int = 1, max_pool: int = 5,
    ) -> None:
        dsn = f"{host}:{port}/{database}"
        self._pool = oracledb.create_pool_async(
            user=user, password=password, dsn=dsn,
            min=min_pool, max=max_pool,
        )

    async def execute(self, sql: str, params: list | None = None) -> QueryResult:
        # oracledb uses :1, :2 bind syntax — convert $N to :N
        exec_sql = sql
        if params:
            for i in range(len(params), 0, -1):
                exec_sql = exec_sql.replace(f"${i}", f":{i}")

        async with self._pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(exec_sql, params or [])
                columns = [desc[0].lower() for desc in cur.description] if cur.description else []
                rows = await cur.fetchall()
                return QueryResult(rows=list(rows), column_names=columns)

    async def close(self) -> None:
        if self._pool:
            await self._pool.close()
            self._pool = None

    @property
    def is_connected(self) -> bool:
        return self._pool is not None
