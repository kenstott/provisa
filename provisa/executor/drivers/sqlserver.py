# Copyright (c) 2026 Kenneth Stott
# Canary: f2981b29-d405-429c-b0c7-1d1f1af20dde
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""SQL Server direct driver over one shared, thread-safe pyodbc pool per source.

Requires ODBC Driver 17/18 for SQL Server installed on the host.

REQ-1882 (amended 2026-09-29): every request runs on its own thread, so the pool is shared by
request threads; a borrower waits (bounded) when all connections are out, and a blocking statement
is cancelled at the request deadline via ``cursor.cancel()``.
"""

# Requirements: REQ-052, REQ-068, REQ-229, REQ-550, REQ-1882

from __future__ import annotations

from typing import Any

import pyodbc

from provisa.core import request_deadline
from provisa.core.sync_pool import BlockingPool
from provisa.executor.drivers.base import DirectDriver
from provisa.executor.result import QueryResult


def _is_broken(exc: BaseException) -> bool:
    return isinstance(exc, (pyodbc.OperationalError, pyodbc.InterfaceError))


class SQLServerDriver(DirectDriver):  # REQ-052, REQ-068, REQ-229, REQ-550
    # Bounded wait for a pooled connection when all are checked out (request deadline permitting).
    _ACQUIRE_TIMEOUT = 10.0

    def __init__(self) -> None:
        self._pool: BlockingPool[Any] | None = None

    # Async only for the DirectDriver awaitable contract; connects synchronously in-thread.
    async def connect(
        self,
        host: str,
        port: int,
        database: str,
        user: str,
        password: str,
        min_pool: int = 1,
        max_pool: int = 5,
    ) -> None:  # REQ-052
        dsn = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={host},{port};"
            f"DATABASE={database};"
            f"UID={user};"
            f"PWD={password};"
            f"TrustServerCertificate=yes"
        )

        # min_pool connections open now, so an unreachable source fails at registration.
        self._pool = BlockingPool(
            lambda: pyodbc.connect(dsn, autocommit=True),
            lambda c: c.close(),
            minsize=max(min_pool, 1),
            maxsize=max_pool,
            wait_s=self._ACQUIRE_TIMEOUT,
            name=f"sqlserver:{host}:{port}/{database}",
        )

    def _require_pool(self) -> BlockingPool[Any]:
        if self._pool is None:
            raise RuntimeError("SQLServerDriver is not connected")
        return self._pool

    # Async only for the DirectDriver awaitable contract; executes synchronously in-thread.
    async def execute(self, sql: str, params: list | None = None) -> QueryResult:
        # pyodbc binds ? positionally: the values are ordered by placeholder occurrence.
        from provisa.compiler.params import bind_positionally

        exec_sql, bound = bind_positionally(sql, params, "?")

        with self._require_pool().connection(is_broken=_is_broken) as conn:
            cur = conn.cursor()
            try:
                with request_deadline.cancel_on_deadline(cur.cancel):
                    cur.execute(exec_sql, bound)
                    rows = cur.fetchall() if cur.description else []
                columns = [desc[0] for desc in cur.description] if cur.description else []
            finally:
                cur.close()
        return QueryResult(rows=[tuple(r) for r in rows], column_names=columns)

    # Async only for the DirectDriver awaitable contract; closes synchronously in-thread.
    async def close(self) -> None:
        pool, self._pool = self._pool, None
        if pool is not None:
            pool.closeall()

    @property
    def is_connected(self) -> bool:
        return self._pool is not None
