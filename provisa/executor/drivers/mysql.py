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
from datetime import datetime, timezone
from typing import Any

import pymysql
import pymysql.converters
import pymysql.cursors
from pymysql.constants import FIELD_TYPE

from provisa.core import request_deadline
from provisa.core.sync_pool import BlockingPool
from provisa.executor.drivers.base import DirectDriver
from provisa.executor.drivers.cursor_stream import PooledCursorStream, fetch_chunked
from provisa.executor.result import QueryResult


def _is_broken(exc: BaseException) -> bool:
    return isinstance(exc, (pymysql.err.OperationalError, pymysql.err.InterfaceError))


def _timestamp_in_utc(raw: Any) -> Any:
    """A TIMESTAMP, given in the session's zone (UTC, the connection's ``init_command``), as its
    instant in UTC (REQ-2050). A value pymysql cannot read as a datetime (a zero date) is handed on
    as pymysql hands it on."""
    value = pymysql.converters.convert_datetime(raw)
    return value.replace(tzinfo=timezone.utc) if isinstance(value, datetime) else value


#: pymysql's conversions with a TIMESTAMP read as its instant in UTC; a DATETIME, which has no
#: zone, stays a wall time.
CONVERSIONS = {**pymysql.converters.conversions, FIELD_TYPE.TIMESTAMP: _timestamp_in_utc}


#: REQ-2167: a table's AUTO_INCREMENT column, with its place among the table's columns. A table
#: named without its database is the connection's.
_GENERATED_COLUMNS = """
SELECT column_name, ordinal_position FROM information_schema.columns
WHERE table_schema = coalesce($1, database()) AND table_name = $2
  AND extra LIKE '%auto_increment%'
ORDER BY ordinal_position
"""


class MySQLDriver(DirectDriver):  # REQ-052, REQ-068, REQ-229, REQ-550
    # Bounded wait for a pooled connection when all are checked out (request deadline permitting).
    _ACQUIRE_TIMEOUT = 10.0
    generated_keys = "last_insert_id"  # REQ-2167

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
                # REQ-2050: a TIMESTAMP is an instant, read in the session's zone -- UTC here --
                # and handed on as that instant in UTC (mysql_conversions).
                init_command="SET time_zone = '+00:00'",
                conv=CONVERSIONS,
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
                    rows = fetch_chunked(cur) if cur.description else []
                columns = [desc[0] for desc in cur.description] if cur.description else []
                # A statement with no result set (a data write) reports how many rows it changed:
                # the count pgwire's command tag answers.
                affected = cur.rowcount if cur.description is None and cur.rowcount >= 0 else None
        return QueryResult(rows=rows, column_names=columns, rowcount=affected)

    async def generated_columns(self, schema: str | None, table: str):  # REQ-2167
        from provisa.executor.drivers.base import GeneratedColumn

        found = await self.execute(_GENERATED_COLUMNS, [schema, table])
        return tuple(GeneratedColumn(name, int(position)) for name, position in found.rows)

    # Async only for the DirectDriver awaitable contract; executes synchronously in-thread.
    async def execute_reporting_generated(
        self, sql: str, params: list | None = None
    ) -> QueryResult:  # REQ-2167
        """The write, and LAST_INSERT_ID() as its own connection reports it. That value outlives
        the statement on a pooled connection, so it is set to 0 first -- a value no AUTO_INCREMENT
        column generates -- and a write that generated none leaves it there. For a statement
        that inserts several rows it is the first row's value, as MySQL's own LAST_INSERT_ID()."""
        from provisa.compiler.params import bind_positionally

        exec_sql, bound = sql, []
        if params:
            exec_sql, bound = bind_positionally(sql.replace("%", "%%"), params, "%s")

        with self._require_pool().connection(is_broken=_is_broken) as conn:
            thread_id = conn.thread_id()
            with conn.cursor() as cur:
                with request_deadline.cancel_on_deadline(lambda: self._kill_query(thread_id)):
                    cur.execute("SELECT LAST_INSERT_ID(0)")
                    cur.execute(exec_sql, bound or None)
                    rows = fetch_chunked(cur) if cur.description else []
                    columns = [desc[0] for desc in cur.description] if cur.description else []
                    affected = cur.rowcount if cur.description is None else None
                    cur.execute("SELECT LAST_INSERT_ID()")
                    (generated,) = cur.fetchone()
        return QueryResult(
            rows=rows,
            column_names=columns,
            rowcount=affected,
            generated_key=int(generated) or None,
        )

    @property
    def supports_streaming(self) -> bool:  # REQ-1190
        return True

    # Async only for the DirectDriver awaitable contract; opens synchronously in-thread.
    async def open_stream(
        self, sql: str, params: list | None = None
    ) -> PooledCursorStream:  # REQ-1190
        """The result a batch at a time, through PyMySQL's UNBUFFERED cursor: its default cursor
        reads the whole result into memory at execute, so a fetchmany over it bounds nothing."""
        from provisa.compiler.params import bind_positionally

        exec_sql, bound = sql, []
        if params:
            exec_sql, bound = bind_positionally(sql.replace("%", "%%"), params, "%s")
        return PooledCursorStream(
            self._require_pool(),
            open_cursor=lambda conn: conn.cursor(pymysql.cursors.SSCursor),
            execute=lambda cur: cur.execute(exec_sql, bound or None),
            columns=lambda cur: [desc[0] for desc in cur.description],
            cancel=lambda conn, _cur: self._kill_query(conn.thread_id()),
        )

    # Async only for the DirectDriver awaitable contract; closes synchronously in-thread.
    async def close(self) -> None:
        pool, self._pool = self._pool, None
        if pool is not None:
            pool.closeall()

    @property
    def is_connected(self) -> bool:
        return self._pool is not None
