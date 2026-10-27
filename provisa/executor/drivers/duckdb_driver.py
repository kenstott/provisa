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

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import duckdb

from provisa.core import engine_memory, request_deadline
from provisa.core.sync_pool import BlockingPool
from provisa.executor.drivers.base import DirectDriver
from provisa.executor.result import QueryResult

# Requirements: REQ-027, REQ-068, REQ-1882


def _is_broken(exc: BaseException) -> bool:
    """A failure after which the connection cannot be trusted: it is closed, not pooled."""
    return isinstance(exc, (duckdb.ConnectionException, duckdb.FatalException))


#: The statements that only read; any other changes the database and opens it read-write.
_READS = frozenset(
    {
        duckdb.StatementType.SELECT,
        duckdb.StatementType.EXPLAIN,
        duckdb.StatementType.PRAGMA,
        duckdb.StatementType.SET,
        duckdb.StatementType.VARIABLE_SET,
        duckdb.StatementType.PREPARE,
        duckdb.StatementType.RELATION,
        duckdb.StatementType.TRANSACTION,
    }
)


def _is_file(database: str) -> bool:
    return database not in ("", ":memory:") and not database.startswith(":memory:")


def fetch_rows(result: Any) -> list[tuple]:  # noqa: ANN401 — a DuckDB result relation
    """A DuckDB result's rows, each interval the IR interval it is.
    DuckDB's own Python rows fold an interval's months into days, 30 to a month (``1 year 2
    months`` reads as 420 days); its Arrow export keeps them, so a result with an interval column
    is read through Arrow (ir_arrow.column_values)."""
    if not any(str(d[1]) == "INTERVAL" for d in result.description):
        return result.fetchall()
    from provisa.core.ir_arrow import column_values

    columns = [column_values(c) for c in result.to_arrow_table().columns]
    return list(zip(*columns))


class DuckDBDriver(DirectDriver):  # REQ-027, REQ-068, REQ-1997
    """A DuckDB connection holds one current result and is not shareable between threads (the
    module declares threadsafety 1). Every request runs on its own thread (REQ-1882), so each
    borrows its own connection from a bounded pool: a CURSOR of the one database connection —
    DuckDB's per-thread connection onto the same database, in-memory databases included — used by
    one request at a time.

    REQ-1997: a database file is held READ-ONLY, so every process and node can read it at once
    (DuckDB shares a file among read-only opens only). A statement that changes it pauses this
    process's holders of the file (``federation.file_locks.exclusive``: this pool, the engine's
    attach), opens it read-write for that statement alone and closes it; when another process or
    node has the file open the open cannot take its lock, and the write is refused as locked."""

    # Bounded wait for a pooled connection when all are checked out (request deadline permitting).
    _ACQUIRE_TIMEOUT = 10.0

    def __init__(self) -> None:
        self._database: str = ":memory:"
        self._root: duckdb.DuckDBPyConnection | None = None
        self._pool: BlockingPool[Any] | None = None
        self._min_pool = 1
        self._max_pool = 5
        # Reads hold it shared; a write's pause holds it alone while the file is let go.
        from provisa.federation.file_locks import ReadersWriterGate

        self._gate = ReadersWriterGate()
        self._holder: int | None = None
        self._shown = ":memory:"  # the database as the source names it, for messages

    def _open(self) -> None:
        database = self._database
        root = engine_memory.connect(database, read_only=_is_file(database))
        self._root = root
        self._pool = BlockingPool(
            root.cursor,
            lambda c: c.close(),
            minsize=max(self._min_pool, 1),
            maxsize=self._max_pool,
            wait_s=self._ACQUIRE_TIMEOUT,
            name=f"duckdb:{database}",
        )

    def _let_go(self) -> None:
        pool, self._pool = self._pool, None
        root, self._root = self._root, None
        if pool is not None:
            pool.closeall()
        if root is not None:
            root.close()

    @contextmanager
    def _paused(self) -> Iterator[None]:
        with self._gate.write():
            self._let_go()
            try:
                yield
            finally:
                self._open()

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
        # DuckDB: database is a file path or :memory:. A path written with the org's tokens
        # ({files}/..., REQ-1997) is opened where it resolves; messages name it as written.
        from provisa.core.org_files import resolve_path

        self._shown = database
        self._database = resolve_path(database) if _is_file(database) else database
        self._min_pool, self._max_pool = min_pool, max_pool
        self._open()
        assert self._root is not None
        engine_memory.log_limit(self._root, f"source {database}")
        if _is_file(database):
            from provisa.federation import file_locks

            self._holder = file_locks.register(self._database, self._paused)

    def _require_pool(self) -> BlockingPool[Any]:
        if self._pool is None:
            raise RuntimeError("DuckDBDriver is not connected")
        return self._pool

    def _writes(self, sql: str) -> bool:
        if not _is_file(self._database):
            return False
        with self._require_pool().connection(is_broken=_is_broken) as conn:
            return any(s.type not in _READS for s in conn.extract_statements(sql))

    # Async only for the DirectDriver awaitable contract; executes synchronously in-thread.
    async def execute(self, sql: str, params: list | None = None) -> QueryResult:
        with self._gate.read():
            writes = self._writes(sql)
            if not writes:
                return self._read(sql, params)
        return self._write(sql, params)

    def _read(self, sql: str, params: list | None) -> QueryResult:
        # DuckDB uses $1, $2 natively (like PG)
        with self._require_pool().connection(is_broken=_is_broken) as conn:
            with request_deadline.cancel_on_deadline(conn.interrupt):
                result = conn.execute(sql, params) if params else conn.execute(sql)
                columns = [desc[0] for desc in result.description] if result.description else []
                rows = fetch_rows(result) if result.description else []
        return QueryResult(rows=rows, column_names=columns)

    def _write(self, sql: str, params: list | None) -> QueryResult:
        from provisa.federation import file_locks

        file_locks.refuse_unprotected(self._database, self._shown)
        with file_locks.exclusive(self._database):
            try:
                conn = engine_memory.connect(self._database)
            except duckdb.IOException as exc:
                if file_locks.is_lock_conflict(exc):
                    raise file_locks.FileLocked(self._shown) from None
                raise
            try:
                with request_deadline.cancel_on_deadline(conn.interrupt):
                    result = conn.execute(sql, params) if params else conn.execute(sql)
                    columns = [d[0] for d in result.description] if result.description else []
                    rows = fetch_rows(result) if result.description else []
            finally:
                conn.close()
        return QueryResult(rows=rows, column_names=columns)

    # Async only for the DirectDriver awaitable contract; closes synchronously in-thread.
    @property
    def supports_streaming(self) -> bool:  # REQ-1190
        return True

    async def open_stream(self, sql: str, params: list | None = None) -> Any:  # REQ-1190
        """The result a batch at a time: a cursor of the pooled connection, read by
        ``fetchmany``; DuckDB's own ``interrupt`` stops a statement still running."""
        from provisa.executor.drivers.cursor_stream import PooledCursorStream

        return PooledCursorStream(
            self._require_pool(),
            open_cursor=lambda conn: conn.cursor(),
            execute=lambda cur: cur.execute(sql, params) if params else cur.execute(sql),
            columns=lambda cur: [desc[0] for desc in cur.description],
            cancel=lambda _conn, cur: cur.interrupt(),
        )

    async def close(self) -> None:
        if self._holder is not None:
            from provisa.federation import file_locks

            file_locks.unregister(self._database, self._holder)
            self._holder = None
        self._let_go()

    @property
    def is_connected(self) -> bool:
        return self._pool is not None
