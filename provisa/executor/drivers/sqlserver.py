# Copyright (c) 2026 Kenneth Stott
# Canary: f2981b29-d405-429c-b0c7-1d1f1af20dde
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""SQL Server direct driver using aioodbc.

Requires ODBC Driver 17/18 for SQL Server installed on the host.
"""

from __future__ import annotations

import aioodbc

from provisa.executor.drivers.base import DirectDriver
from provisa.executor.trino import QueryResult


class SQLServerDriver(DirectDriver):
    def __init__(self) -> None:
        self._pool: aioodbc.Pool | None = None

    async def connect(
        self, host: str, port: int, database: str,
        user: str, password: str, min_pool: int = 1, max_pool: int = 5,
    ) -> None:
        dsn = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={host},{port};"
            f"DATABASE={database};"
            f"UID={user};"
            f"PWD={password};"
            f"TrustServerCertificate=yes"
        )
        self._pool = await aioodbc.create_pool(
            dsn=dsn, minsize=min_pool, maxsize=max_pool,
        )

    async def execute(self, sql: str, params: list | None = None) -> QueryResult:
        # aioodbc uses ? placeholders — convert $N to ?
        exec_sql = sql
        if params:
            for i in range(len(params), 0, -1):
                exec_sql = exec_sql.replace(f"${i}", "?")

        async with self._pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(exec_sql, params or [])
                rows = await cur.fetchall()
                columns = [desc[0] for desc in cur.description] if cur.description else []
                return QueryResult(rows=[tuple(r) for r in rows], column_names=columns)

    async def close(self) -> None:
        if self._pool:
            self._pool.close()
            await self._pool.wait_closed()
            self._pool = None

    @property
    def is_connected(self) -> bool:
        return self._pool is not None
