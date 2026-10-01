# Copyright (c) 2026 Kenneth Stott
# Canary: d7bb11a2-a6e2-4758-bfdc-ac1d874d3161
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""MySQL direct driver over one shared, thread-safe PyMySQL pool per source.

REQ-1882 (amended 2026-09-29): every request runs on its own thread, so the pool is shared by
request threads; a borrower waits (bounded) when all connections are out, and a blocking statement
is cancelled at the request deadline with ``KILL QUERY <thread_id>`` from a short-lived second
connection (MySQL has no in-band cancel)."""

# Requirements: REQ-052, REQ-068, REQ-229, REQ-550, REQ-1882

from __future__ import annotations

import ssl
from collections.abc import Callable
from typing import Any

import pymysql

from provisa.core import request_deadline
from provisa.core.sync_pool import BlockingPool
from provisa.executor.drivers.base import DirectDriver
from provisa.executor.result import QueryResult


def _is_broken(exc: BaseException) -> bool:
    return isinstance(exc, (pymysql.err.OperationalError, pymysql.err.InterfaceError))


class MySQLDriver(DirectDriver):  # REQ-052, REQ-068, REQ-229, REQ-550
    # Bounded wait for a pooled connection when all are checked out (request deadline permitting).
    _ACQUIRE_TIMEOUT = 10.0

    def __init__(self, require_ssl: bool = False) -> None:
        self._pool: BlockingPool[Any] | None = None
        self._connect: Callable[[], Any] | None = None
        # SingleStore Cloud's shared-tier workspaces reject any connection without TLS (MySQL error
        # 1251 "No SSL detected") — self-hosted mysql/mariadb/tidb/singlestoredb-dev have no such
        # requirement, so this is per-source-type, not a generic toggle (see registry.py's
        # _make_singlestore). The system default CA store already trusts SingleStore Cloud's cert
        # (a public AWS-hosted endpoint) — no bundled CA file needed.
        self._require_ssl = require_ssl

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
        require_ssl = self._require_ssl

        def _create() -> Any:
            return pymysql.connect(
                host=host,
                port=port,
                database=database,
                user=user,
                password=password,
                ssl=ssl.create_default_context() if require_ssl else None,
                autocommit=True,
            )

        self._connect = _create
        # min_pool connections open now, so an unreachable source fails at registration.
        self._pool = BlockingPool(
            _create,
            lambda c: c.close(),
            minsize=max(min_pool, 1),
            maxsize=max_pool,
            wait_s=self._ACQUIRE_TIMEOUT,
            name=f"mysql:{host}:{port}/{database}",
        )

    def _require_pool(self) -> BlockingPool[Any]:
        if self._pool is None:
            raise RuntimeError("MySQLDriver is not connected")
        return self._pool

    def _kill_query(self, thread_id: int) -> None:
        """Cancel the statement running on server thread ``thread_id`` from a second connection."""
        assert self._connect is not None
        killer = self._connect()
        try:
            with killer.cursor() as cur:
                cur.execute(f"KILL QUERY {int(thread_id)}")
        finally:
            killer.close()

    # Async only for the DirectDriver awaitable contract; executes synchronously in-thread.
    async def execute(self, sql: str, params: list | None = None) -> QueryResult:
        # PyMySQL binds %s positionally and %-formats the whole statement when values are bound,
        # so a literal % is escaped first and the values are ordered by placeholder occurrence.
        from provisa.compiler.params import bind_positionally

        exec_sql, bound = sql, []
        if params:
            exec_sql, bound = bind_positionally(sql.replace("%", "%%"), params, "%s")

        with self._require_pool().connection(is_broken=_is_broken) as conn:
            thread_id = conn.thread_id()
            with conn.cursor() as cur:
                with request_deadline.cancel_on_deadline(lambda: self._kill_query(thread_id)):
                    cur.execute(exec_sql, bound or None)
                    rows = cur.fetchall()
                columns = [desc[0] for desc in cur.description] if cur.description else []
        return QueryResult(rows=list(rows), column_names=columns)

    # Async only for the DirectDriver awaitable contract; closes synchronously in-thread.
    async def close(self) -> None:
        pool, self._pool = self._pool, None
        if pool is not None:
            pool.closeall()

    @property
    def is_connected(self) -> bool:
        return self._pool is not None
