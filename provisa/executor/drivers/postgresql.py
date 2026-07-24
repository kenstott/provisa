# Copyright (c) 2026 Kenneth Stott
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

# Requirements: REQ-052, REQ-053, REQ-068, REQ-550

from __future__ import annotations

from typing import Any

import asyncpg

from provisa.executor.drivers.base import DirectDriver, DirectResultStream
from provisa.executor.result import QueryResult


class _PgDirectStream(DirectResultStream):  # REQ-1190
    """asyncpg server-side cursor: a pooled connection + open transaction held for the stream's life,
    a prepared statement for the column attributes, and a cursor fetched in bounded batches. Releasing
    commits the (read-only) transaction and returns the connection to the pool. This bounds a large
    DIRECT passthrough scan to one fetch batch instead of the whole result (streaming-uniformity Defect 1)."""

    def __init__(self, pool: asyncpg.Pool, sql: str, params: list) -> None:
        self._pool = pool
        self._sql = sql
        self._params = params
        self._conn: Any = None
        self._tr: Any = None
        self._cur: Any = None
        self.column_names = []
        self.column_types = None

    async def _open(self) -> None:
        conn = await self._pool.acquire(timeout=PostgreSQLDriver._ACQUIRE_TIMEOUT)
        self._conn = conn
        # A server-side cursor requires an open transaction; the read is committed on close.
        self._tr = conn.transaction()
        await self._tr.start()
        stmt = await conn.prepare(self._sql)
        attrs = stmt.get_attributes()
        self.column_names = [a.name for a in attrs]
        self.column_types = [a.type.name for a in attrs]
        self._cur = await stmt.cursor(*self._params)

    async def fetch(self, size: int) -> list[tuple]:
        assert self._cur is not None
        records = await self._cur.fetch(size)
        return [tuple(r.values()) for r in records]

    async def close(self) -> None:
        if self._conn is None:
            return
        conn, tr = self._conn, self._tr
        self._conn = self._tr = self._cur = None
        try:
            if tr is not None:
                await tr.commit()
        finally:
            await self._pool.release(conn)


class PostgreSQLDriver(DirectDriver):  # REQ-052, REQ-053, REQ-068, REQ-550
    def __init__(self, use_pgbouncer: bool = False) -> None:
        self._pool: asyncpg.Pool | None = None
        self._use_pgbouncer = use_pgbouncer

    async def connect(
        self,
        host: str,
        port: int,
        database: str,
        user: str,
        password: str,
        min_pool: int = 1,
        max_pool: int = 5,
    ) -> None:  # REQ-052, REQ-053
        if self._use_pgbouncer:
            self._pool = await asyncpg.create_pool(
                host=host,
                port=port,
                database=database,
                user=user,
                password=password,
                min_size=min_pool,
                max_size=max_pool,
                statement_cache_size=0,
            )
        else:
            self._pool = await asyncpg.create_pool(
                host=host,
                port=port,
                database=database,
                user=user,
                password=password,
                min_size=min_pool,
                max_size=max_pool,
            )

    _ACQUIRE_TIMEOUT = 10.0

    async def execute(self, sql: str, params: list | None = None) -> QueryResult:
        pool = self._pool
        assert pool is not None
        async with pool.acquire(timeout=self._ACQUIRE_TIMEOUT) as conn:
            col_types: list[str] | None = None
            if self._use_pgbouncer:
                # PgBouncer: use conn.fetch directly (no prepared statements)
                if params:
                    rows = await conn.fetch(sql, *params)
                else:
                    rows = await conn.fetch(sql)
                columns = list(rows[0].keys()) if rows else self._extract_columns(sql)
            else:
                stmt = await conn.prepare(sql)
                attrs = stmt.get_attributes()
                columns = [attr.name for attr in attrs]
                # REQ-883: carry the source's real PG result-column types so downstream
                # binary encoders (DuckDB ATTACH / libpq COPY binary) tag each field with
                # the OID the catalog advertised — a missing type would encode as text and
                # break the client's binary reader.
                col_types = [attr.type.name for attr in attrs]
                rows = await stmt.fetch(*(params or []))
            return QueryResult(
                rows=[tuple(r.values()) for r in rows],
                column_names=columns,
                column_types=col_types,
            )

    @property
    def supports_streaming(self) -> bool:  # REQ-1190
        # PgBouncer transaction-pool mode forbids the long-lived server-side cursor a stream needs, so
        # only a direct connection streams; a pgbouncer'd source materializes via execute().
        return not self._use_pgbouncer

    async def open_stream(self, sql: str, params: list | None = None) -> _PgDirectStream:  # REQ-1190
        pool = self._pool
        assert pool is not None
        stream = _PgDirectStream(pool, sql, list(params or []))
        await stream._open()
        return stream

    def _extract_columns(self, sql: str) -> list[str]:
        """Fallback column extraction from SQL for empty results via PgBouncer."""
        # Parse SELECT ... FROM to get column names
        import re

        m = re.match(r"SELECT\s+(.+?)\s+FROM", sql, re.IGNORECASE | re.DOTALL)
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

    async def execute_ddl(self, sql: str) -> None:
        pool = self._pool
        assert pool is not None
        async with pool.acquire(timeout=self._ACQUIRE_TIMEOUT) as conn:
            await conn.execute(sql)

    async def fetch_enums(self) -> dict[str, list[str]]:  # REQ-636
        from provisa.compiler.enum_detect import fetch_enum_registry

        pool = self._pool
        assert pool is not None
        async with pool.acquire(timeout=self._ACQUIRE_TIMEOUT) as conn:
            return await fetch_enum_registry(conn)

    async def close(self) -> None:
        if self._pool:
            await self._pool.close()
            self._pool = None

    @property
    def is_connected(self) -> bool:
        return self._pool is not None
