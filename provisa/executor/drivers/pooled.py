# Copyright (c) 2026 Kenneth Stott
# Canary: f6bce3d1-d211-42e2-9f5f-a9c8e8480458
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Direct drivers over a client connection that serves one statement at a time (REQ-1882).

Several client libraries hand out a connection that must not be shared between threads —
databricks-sql-connector, pyodbc and impyla declare DB-API ``threadsafety = 1``, a pyexasol
connection is one websocket carrying one request. Every request runs on its own thread, so a
driver over such a library holds a bounded pool of connections: a statement checks one out, runs
on the request's own thread, and returns it. Two requests never interleave on one connection, the
request deadline can cancel the statement, and a connection that failed at the transport level is
closed instead of pooled.
"""

from __future__ import annotations

from abc import abstractmethod
from collections.abc import Callable
from typing import Any

from provisa.core.sync_pool import BlockingPool
from provisa.executor.drivers.base import DirectDriver
from provisa.executor.result import QueryResult


class SingleStatementConnectionDriver(DirectDriver):
    """``connect`` builds the pool through :meth:`_open_pool`; a subclass supplies how one
    connection runs a statement (:meth:`_run`, which registers its own cancel with the request
    deadline) and which failures mean the connection is gone (:meth:`_is_broken`)."""

    # Bounded wait for a pooled connection when all are checked out (request deadline permitting).
    _ACQUIRE_TIMEOUT = 10.0

    _pool: BlockingPool[Any] | None = None

    def _open_pool(
        self, open_: Callable[[], Any], *, min_pool: int, max_pool: int, name: str
    ) -> None:
        # min_pool connections open now, so an unreachable source fails at registration.
        self._pool = BlockingPool(
            open_,
            lambda conn: conn.close(),
            minsize=max(min_pool, 1),
            maxsize=max_pool,
            wait_s=self._ACQUIRE_TIMEOUT,
            name=name,
        )

    @abstractmethod
    def _run(self, conn: Any, sql: str, params: list | None) -> QueryResult:
        """Run one statement on ``conn``, cancellable by the request deadline."""

    @abstractmethod
    def _is_broken(self, exc: BaseException) -> bool:
        """A failure after which ``conn`` cannot be trusted: it is closed, not pooled."""

    # Async only for the DirectDriver awaitable contract; executes synchronously in-thread.
    async def execute(self, sql: str, params: list | None = None) -> QueryResult:
        if self._pool is None:
            raise RuntimeError(f"{type(self).__name__} is not connected")
        with self._pool.connection(is_broken=self._is_broken) as conn:
            return self._run(conn, sql, params)

    # Async only for the DirectDriver awaitable contract; closes synchronously in-thread.
    async def close(self) -> None:
        pool, self._pool = self._pool, None
        if pool is not None:
            pool.closeall()

    @property
    def is_connected(self) -> bool:
        return self._pool is not None


def run_dbapi(
    conn: Any, sql: str, params: list | None, cancel_of: Callable[[Any], Callable[[], None]]
) -> QueryResult:
    """One statement on a DB-API connection: a cursor of its own, the request deadline wired to
    the cursor's cancel (``cancel_of(cursor)``), rows as tuples."""
    from provisa.core import request_deadline

    cur = conn.cursor()
    try:
        with request_deadline.cancel_on_deadline(cancel_of(cur)):
            if params:
                cur.execute(sql, params)
            else:
                cur.execute(sql)
            cols = [d[0] for d in cur.description] if cur.description else []
            rows = cur.fetchall() if cur.description else []
        return QueryResult(rows=[tuple(r) for r in rows], column_names=cols)
    finally:
        cur.close()
