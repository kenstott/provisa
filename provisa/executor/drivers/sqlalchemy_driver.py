# Copyright (c) 2026 Kenneth Stott
# Canary: 5b1d9e73-7c48-4d75-8e02-2c7a0d4f9e55
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Generic SQLAlchemy fallback driver (REQ-229, REQ-550).

The native drivers (asyncpg/aiomysql/oracledb/pyodbc) cover a fixed set of source types. This
driver is the FALLBACK for any type that has a SQLAlchemy dialect but no bespoke async driver —
it broadens the writable/readable set to the whole SQLAlchemy dialect universe (intersected with
SQLGlot's write dialects, since mutations are transpiled before they reach a driver — see
executor/writable.py).

SQLAlchemy Core is synchronous, so every call is offloaded to a thread. Writes run inside
``engine.begin()`` so they COMMIT on success (a mutation reaching the store must be durable, never
rolled back when the connection returns to the pool). The PG-canonical ``$N`` placeholders the
compiler emits are converted to SQLAlchemy named binds, which SQLAlchemy then renders in the
target dialect's own paramstyle.
"""

from __future__ import annotations

import asyncio
from typing import Any

from provisa.executor.drivers.base import DirectDriver, DirectResultStream
from provisa.executor.result import QueryResult


def _to_named_params(sql: str, params: list | None) -> tuple[str, dict[str, Any]]:
    """Convert ``$N`` positional placeholders to SQLAlchemy ``:pN`` named binds.

    Replaces highest index first so ``$1`` does not corrupt ``$10`` (same guard the native MySQL
    driver uses for its ``%s`` conversion). Returns the rewritten SQL and the bind dict.
    """
    from provisa.core.database import _literal_colons_escaped

    sql = _literal_colons_escaped(sql)  # a colon in a literal is text, never a bind
    if not params:
        return sql, {}
    out = sql
    bind: dict[str, Any] = {}
    for i in range(len(params), 0, -1):
        out = out.replace(f"${i}", f":p{i}")
        bind[f"p{i}"] = params[i - 1]
    return out, bind


def _writes(sql: str) -> bool:
    """Whether ``sql`` changes the database: anything but a query, EXPLAIN or PRAGMA."""
    import sqlglot
    from sqlglot import exp

    reads = (exp.Query, exp.Pragma, exp.Describe, exp.Show)
    return any(
        statement is not None
        and not isinstance(statement, reads)
        and not (isinstance(statement, exp.Command) and statement.name.upper() == "EXPLAIN")
        for statement in sqlglot.parse(sql, read="sqlite")
    )


class SQLAlchemyDriver(DirectDriver):  # REQ-229, REQ-550
    """A DirectDriver backed by a synchronous SQLAlchemy engine, offloaded to threads.

    ``drivername`` is a SQLAlchemy URL drivername (e.g. ``sqlite``, ``postgresql+psycopg2``,
    ``mssql+pyodbc``, ``redshift+redshift_connector``). The corresponding DBAPI package must be
    installed for the engine to connect.
    """

    def __init__(self, drivername: str) -> None:
        self._drivername = drivername
        self._engine: Any = None  # sqlalchemy.Engine (sync)
        self._shown = ""  # the database as the source names it, for messages

    async def connect(
        self,
        host: str,
        port: int,
        database: str,
        user: str,
        password: str,
        min_pool: int = 1,
        max_pool: int = 5,
    ) -> None:
        from sqlalchemy import create_engine
        from sqlalchemy.engine import URL

        # REQ-1997: a file database's path written with the org's tokens ({files}/...) is opened
        # where it resolves; messages name it as written.
        self._shown = database
        if self._drivername.startswith("sqlite") and database:
            from provisa.core.org_files import resolve_path

            database = resolve_path(database)
        url = URL.create(
            self._drivername,
            username=user or None,
            password=password or None,
            host=host or None,
            port=port or None,
            database=database or None,
        )
        loop = asyncio.get_running_loop()
        # Pooling args are dialect-specific (sqlite pools differ), so keep create_engine minimal and
        # let each dialect apply its own default pool. Engine construction is cheap but blocking.
        self._engine = await loop.run_in_executor(None, lambda: create_engine(url))

    async def execute(self, sql: str, params: list | None = None) -> QueryResult:
        named_sql, bind = _to_named_params(sql, params)
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self._run, named_sql, bind)

    def _run(self, sql: str, bind: dict[str, Any]) -> QueryResult:
        from sqlalchemy import text

        engine = self._engine
        assert engine is not None
        if self._drivername.startswith("sqlite") and _writes(sql):
            return self._write_sqlite(sql, bind)
        with engine.begin() as conn:  # begin() commits on success, rolls back on error
            result = conn.execute(text(sql), bind)
            if result.returns_rows:
                rows = [tuple(row) for row in result.fetchall()]
                columns = list(result.keys())
                return QueryResult(rows=rows, column_names=columns)
            # a write with no RETURNING has no result set; report the driver's DML row count instead
            rowcount = result.rowcount
            return QueryResult(
                rows=[], column_names=[], rowcount=rowcount if rowcount >= 0 else None
            )

    def _write_sqlite(self, sql: str, bind: dict[str, Any]) -> QueryResult:
        """REQ-1997, the maintainer's ruling on writes to file databases: "just error them out
        when they can't a lock on it". A SQLite file is locked by a write transaction: this one
        does not wait for another (busy_timeout 0), and when the file is locked the write is
        refused as that, named where the source is known (``executor.pool``)."""
        from sqlalchemy import exc, text

        from provisa.federation.file_locks import FileLocked, refuse_unprotected

        engine = self._engine
        assert engine is not None
        refuse_unprotected(engine.url.database or "", self._shown)
        try:
            with engine.begin() as conn:
                conn.exec_driver_sql("PRAGMA busy_timeout = 0")
                result = conn.execute(text(sql), bind)
                if result.returns_rows:
                    rows = [tuple(row) for row in result.fetchall()]
                    return QueryResult(rows=rows, column_names=list(result.keys()))
                rowcount = result.rowcount
                return QueryResult(
                    rows=[], column_names=[], rowcount=rowcount if rowcount >= 0 else None
                )
        except exc.OperationalError as failed:
            if "database is locked" in str(failed.orig):
                raise FileLocked(self._shown) from None
            raise

    # -- REQ-1190: a DIRECT read a batch at a time ------------------------------------------------

    @property
    def supports_streaming(self) -> bool:  # REQ-1190
        return True

    async def open_stream(self, sql: str, params: list | None = None) -> DirectResultStream:
        """The result a batch at a time on a ``stream_results`` connection: the dialect reads
        from the server as ``fetchmany`` asks (a server-side cursor where the DBAPI has one),
        so one batch is held, never the result."""
        named_sql, bind = _to_named_params(sql, params)
        engine = self._engine
        if engine is None:
            raise RuntimeError("SQLAlchemyDriver is not connected")
        # In-thread, as every DIRECT stream is (PooledCursorStream): the request's deadline is
        # bound to this thread, and the statement's cancel is registered with it here.
        return SqlAlchemyResultStream(engine, named_sql, bind)

    async def close(self) -> None:
        engine = self._engine
        if engine is not None:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, engine.dispose)
            self._engine = None

    @property
    def is_connected(self) -> bool:
        return self._engine is not None


class SqlAlchemyResultStream(DirectResultStream):  # REQ-1190
    """One statement's result read a batch at a time from a ``stream_results`` connection. A read
    to its end closes the connection back to its pool; one closed early, failed, or cut by the
    request's deadline has its statement cancelled where the DBAPI cursor can be, and the
    connection invalidated (its protocol state is unknown) -- never pooled again."""

    def __init__(self, engine: Any, sql: str, bind: dict[str, Any]) -> None:
        from sqlalchemy import text

        from provisa.core import request_deadline

        self.column_names = []
        self.column_types = None
        self._conn: Any = engine.connect().execution_options(stream_results=True)
        self._result: Any = None
        try:
            with request_deadline.cancel_on_deadline(self._cancel):
                self._result = self._conn.execute(text(sql), bind)
            if not self._result.returns_rows:
                self._release(completed=True)
            else:
                self.column_names = list(self._result.keys())
        except BaseException:
            self._release(completed=False)
            raise

    def _cancel(self) -> None:
        """Stop a statement still sending rows, through the DBAPI cursor's own cancel where the
        driver offers one (trino, psycopg2, hdbcli...); otherwise invalidating the connection on
        release ends it."""
        cancel = getattr(getattr(self._result, "cursor", None), "cancel", None)
        if callable(cancel):
            cancel()

    def _release(self, *, completed: bool) -> None:
        conn, self._conn = self._conn, None
        result, self._result = self._result, None
        if conn is None:
            return
        if completed:
            if result is not None:
                result.close()
            conn.close()
            return
        cancel = getattr(getattr(result, "cursor", None), "cancel", None)
        try:
            if callable(cancel):
                cancel()
        except Exception:  # noqa: BLE001 — the connection is invalidated whatever the cancel did
            pass
        finally:
            conn.invalidate()
            conn.close()

    # Async only for the DirectResultStream awaitable contract; fetches synchronously in-thread.
    async def fetch(self, size: int) -> list[tuple]:
        from provisa.core import request_deadline

        result = self._result
        if result is None:
            return []
        try:
            with request_deadline.cancel_on_deadline(self._cancel):
                rows = [tuple(r) for r in result.fetchmany(size)]
        except BaseException:
            self._release(completed=False)
            raise
        if not rows:
            self._release(completed=True)
        return rows

    async def close(self) -> None:
        self._release(completed=False)

    def __del__(self) -> None:
        if self._conn is not None:
            self._release(completed=False)
