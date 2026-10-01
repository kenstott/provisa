# Copyright (c) 2026 Kenneth Stott
# Canary: 82037dbb-0db6-4ad0-92bf-59b01efe826c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""DuckDB direct driver. Sync but in-process and fast — no pool needed."""

from __future__ import annotations

from typing import Any

import duckdb

from provisa.core import request_deadline
from provisa.core.sync_pool import BlockingPool
from provisa.executor.drivers.base import DirectDriver
from provisa.executor.result import QueryResult

# Requirements: REQ-027, REQ-068, REQ-1882


def _is_broken(exc: BaseException) -> bool:
    """A failure after which the connection cannot be trusted: it is closed, not pooled."""
    return isinstance(exc, (duckdb.ConnectionException, duckdb.FatalException))


class DuckDBDriver(DirectDriver):  # REQ-027, REQ-068
    """A DuckDB connection holds one current result and is not shareable between threads (the
    module declares threadsafety 1). Every request runs on its own thread (REQ-1882), so each
    borrows its own connection from a bounded pool: a CURSOR of the one database connection —
    DuckDB's per-thread connection onto the same database, in-memory databases included — used by
    one request at a time."""

    # Bounded wait for a pooled connection when all are checked out (request deadline permitting).
    _ACQUIRE_TIMEOUT = 10.0

    def __init__(self) -> None:
        self._database: str = ":memory:"
        self._root: duckdb.DuckDBPyConnection | None = None
        self._pool: BlockingPool[Any] | None = None

    # Async only for the DirectDriver awaitable contract; connects synchronously in-thread.
    async def connect(  # pyright: ignore[reportIncompatibleMethodOverride]
        self,
        host: str,  # pyright: ignore[reportUnusedParameter]
        port: int,  # pyright: ignore[reportUnusedParameter]
        database: str,
        user: str,  # pyright: ignore[reportUnusedParameter]
        password: str,  # pyright: ignore[reportUnusedParameter]
        min_pool: int = 1,
        max_pool: int = 5,
    ) -> None:
        # DuckDB: database is a file path or :memory:
        self._database = database
        root = duckdb.connect(database)
        self._root = root
        self._pool = BlockingPool(
            root.cursor,
            lambda c: c.close(),
            minsize=max(min_pool, 1),
            maxsize=max_pool,
            wait_s=self._ACQUIRE_TIMEOUT,
            name=f"duckdb:{database}",
        )

    def _require_pool(self) -> BlockingPool[Any]:
        if self._pool is None:
            raise RuntimeError("DuckDBDriver is not connected")
        return self._pool

    # Async only for the DirectDriver awaitable contract; executes synchronously in-thread.
    async def execute(self, sql: str, params: list | None = None) -> QueryResult:
        # DuckDB uses $1, $2 natively (like PG)
        with self._require_pool().connection(is_broken=_is_broken) as conn:
            with request_deadline.cancel_on_deadline(conn.interrupt):
                result = conn.execute(sql, params) if params else conn.execute(sql)
                columns = [desc[0] for desc in result.description] if result.description else []
                rows = result.fetchall() if result.description else []
        return QueryResult(rows=rows, column_names=columns)

    # Async only for the DirectDriver awaitable contract; closes synchronously in-thread.
    async def close(self) -> None:
        pool, self._pool = self._pool, None
        root, self._root = self._root, None
        if pool is not None:
            pool.closeall()
        if root is not None:
            root.close()

    @property
    def is_connected(self) -> bool:
        return self._pool is not None
