# Copyright (c) 2025 Kenneth Stott
# Canary: 7b73f4d0-6a4a-481e-aa89-9e177a9c8afa
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""PostgreSQL direct driver using asyncpg.

Supports direct PG connections and PgBouncer (transaction pool mode).
When using PgBouncer, statement_cache_size=0 is required (PgBouncer
does not support prepared statements in transaction mode).
"""

from __future__ import annotations

import asyncpg

from provisa.executor.drivers.base import DirectDriver
from provisa.executor.trino import QueryResult


class PostgreSQLDriver(DirectDriver):
    def __init__(self, use_pgbouncer: bool = False) -> None:
        self._pool: asyncpg.Pool | None = None
        self._use_pgbouncer = use_pgbouncer

    async def connect(
        self, host: str, port: int, database: str,
        user: str, password: str, min_pool: int = 1, max_pool: int = 5,
    ) -> None:
        kwargs = dict(
            host=host, port=port, database=database,
            user=user, password=password,
            min_size=min_pool, max_size=max_pool,
        )
        if self._use_pgbouncer:
            # PgBouncer transaction mode: no prepared statements
            kwargs["statement_cache_size"] = 0
        self._pool = await asyncpg.create_pool(**kwargs)

    async def execute(self, sql: str, params: list | None = None) -> QueryResult:
        async with self._pool.acquire() as conn:
            if self._use_pgbouncer:
                # PgBouncer: use conn.fetch directly (no prepared statements)
                if params:
                    rows = await conn.fetch(sql, *params)
                else:
                    rows = await conn.fetch(sql)
                columns = list(rows[0].keys()) if rows else self._extract_columns(sql)
            else:
                stmt = await conn.prepare(sql)
                columns = [attr.name for attr in stmt.get_attributes()]
                rows = await stmt.fetch(*(params or []))
            return QueryResult(
                rows=[tuple(r.values()) for r in rows],
                column_names=columns,
            )

    def _extract_columns(self, sql: str) -> list[str]:
        """Fallback column extraction from SQL for empty results via PgBouncer."""
        # Parse SELECT ... FROM to get column names
        import re
        m = re.match(r'SELECT\s+(.+?)\s+FROM', sql, re.IGNORECASE | re.DOTALL)
        if not m:
            return []
        select_part = m.group(1)
        cols = []
        for part in select_part.split(","):
            part = part.strip()
            # Handle "alias"."col" or "col" or alias
            cleaned = part.split(".")[-1].strip().strip('"')
            cols.append(cleaned)
        return cols

    async def close(self) -> None:
        if self._pool:
            await self._pool.close()
            self._pool = None

    @property
    def is_connected(self) -> bool:
        return self._pool is not None
