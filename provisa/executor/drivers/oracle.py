# Copyright (c) 2026 Kenneth Stott
# Canary: 20681b37-73be-4a88-b752-108c3837d4d8
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Oracle direct driver using oracledb (python-oracledb thin mode — no Oracle Client needed).

REQ-1882 (amended 2026-09-29): one shared, thread-safe pool of sync ``oracledb`` connections per
source, used by every request thread. A borrower waits (bounded by the pool and the request
deadline) when all connections are out, and a blocking statement is cancelled at the request
deadline via ``conn.cancel()``."""

# Requirements: REQ-052, REQ-229, REQ-550, REQ-1882

from __future__ import annotations

from typing import Any

import oracledb  # pyright: ignore[reportMissingImports]

from provisa.core import request_deadline
from provisa.core.sync_pool import BlockingPool
from provisa.executor.drivers.base import DirectDriver
from provisa.executor.result import QueryResult


def _is_broken(exc: BaseException) -> bool:
    return isinstance(exc, (oracledb.OperationalError, oracledb.InterfaceError))


class OracleDriver(DirectDriver):  # REQ-052, REQ-229, REQ-550
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
        dsn = f"{host}:{port}/{database}"
        # min_pool connections open now, so an unreachable source fails at registration.
        self._pool = BlockingPool(
            lambda: oracledb.connect(user=user, password=password, dsn=dsn),
            lambda c: c.close(),
            minsize=max(min_pool, 1),
            maxsize=max_pool,
            wait_s=self._ACQUIRE_TIMEOUT,
            name=f"oracle:{dsn}",
        )

    def _require_pool(self) -> BlockingPool[Any]:
        if self._pool is None:
            raise RuntimeError("OracleDriver is not connected")
        return self._pool

    # Async only for the DirectDriver awaitable contract; executes synchronously in-thread.
    async def execute(self, sql: str, params: list | None = None) -> QueryResult:
        # oracledb binds a list positionally, one value per placeholder OCCURRENCE (a repeated
        # :1 is two positions) — so each occurrence gets its own :k and the values follow in order.
        from provisa.compiler.params import bind_positionally

        exec_sql, bound = bind_positionally(sql, params, lambda k: f":{k}")

        with self._require_pool().connection(is_broken=_is_broken) as conn:
            try:
                with conn.cursor() as cur:
                    with request_deadline.cancel_on_deadline(conn.cancel):
                        cur.execute(exec_sql, bound)
                        rows = cur.fetchall() if cur.description else []
                    # ``desc.name``, not ``desc[0]``: a FetchInfo indexes as the DB-API 7-tuple
                    # whose members span str, int, bool and None, so only the named attribute is
                    # the column name.
                    columns = (
                        [desc.name.lower() for desc in cur.description] if cur.description else []
                    )
                # Statement-level commit, matching the other direct drivers (autocommit semantics).
                conn.commit()
            except BaseException as exc:
                if not _is_broken(exc):
                    conn.rollback()
                raise
        return QueryResult(rows=list(rows), column_names=columns)

    # Async only for the DirectDriver awaitable contract; closes synchronously in-thread.
    async def close(self) -> None:
        pool, self._pool = self._pool, None
        if pool is not None:
            pool.closeall()

    @property
    def is_connected(self) -> bool:
        return self._pool is not None
