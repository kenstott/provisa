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
from provisa.executor.drivers.cursor_stream import FETCH_CHUNK_ROWS
from provisa.executor.drivers.duckdb_arrow import LOSSLESS_ARROW, answered_batches, arrow_rows
from provisa.executor.result import ArrowResult, QueryResult

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


def _arrow_result(cursor: Any) -> QueryResult:  # noqa: ANN401 — a DuckDB connection or cursor
    """The result of the statement ``cursor`` last ran, read through DuckDB's Arrow export, never
    its DB-API rows (REQ-2194): the table, with its rows built from it now, on this thread
    (duckdb_arrow.arrow_rows). The cursor exports losslessly (duckdb_arrow.LOSSLESS_ARROW)."""
    if not cursor.description:
        return QueryResult(rows=[], column_names=[])
    return ArrowResult(cursor.to_arrow_table(), to_rows=arrow_rows).with_rows()


class _ArrowCursor:
    """A DuckDB cursor read through its Arrow reader for :class:`PooledCursorStream`:
    ``fetchmany`` builds the next rows from the record batches (duckdb_arrow.arrow_rows), never
    from DuckDB's DB-API rows (REQ-2194); an engine error met in a later batch is PostgreSQL's own
    (duckdb_arrow.answered_batches)."""

    def __init__(self, conn: Any) -> None:  # noqa: ANN401 — a DuckDB connection
        self._cursor = conn.cursor()
        self._description: Any = None
        self._reader: Iterator[Any] | None = None
        self._pending: list[tuple] = []

    @property
    def description(self) -> Any:  # noqa: ANN401 — DuckDB's description
        return self._description

    def execute(self, sql: str, params: list | None) -> None:
        self._cursor.execute(LOSSLESS_ARROW)
        # The statement as a relation: DuckDB 2.0's DB-API result hands its Arrow reader over only
        # once it has computed the whole result, so a later batch's error would be raised here,
        # not as that batch is read.
        rel = self._cursor.sql(sql, params=params) if params else self._cursor.sql(sql)
        self._description = rel.description
        if self._description:
            self._reader = answered_batches(rel.to_arrow_reader(FETCH_CHUNK_ROWS))

    def fetchmany(self, size: int) -> list[tuple]:
        while len(self._pending) < size and self._reader is not None:
            batch = next(self._reader, None)
            if batch is None:
                self._reader = None
                break
            self._pending.extend(arrow_rows(batch))
        rows, self._pending = self._pending[:size], self._pending[size:]
        return rows

    def interrupt(self) -> None:
        self._cursor.interrupt()

    def close(self) -> None:
        self._reader = None
        self._cursor.close()


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
            conn.execute(LOSSLESS_ARROW)
            with request_deadline.cancel_on_deadline(conn.interrupt):
                conn.execute(sql, params) if params else conn.execute(sql)
                return _arrow_result(conn)

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
                conn.execute(LOSSLESS_ARROW)
                with request_deadline.cancel_on_deadline(conn.interrupt):
                    conn.execute(sql, params) if params else conn.execute(sql)
                    return _arrow_result(conn)
            finally:
                conn.close()

    # Async only for the DirectDriver awaitable contract; closes synchronously in-thread.
    @property
    def supports_streaming(self) -> bool:  # REQ-1190
        return True

    async def open_stream(self, sql: str, params: list | None = None) -> Any:  # REQ-1190
        """The result a batch at a time: a cursor of the pooled connection, read through its
        Arrow reader (:class:`_ArrowCursor`); DuckDB's own ``interrupt`` stops a statement still
        running."""
        from provisa.executor.drivers.cursor_stream import PooledCursorStream

        return PooledCursorStream(
            self._require_pool(),
            open_cursor=_ArrowCursor,
            execute=lambda cur: cur.execute(sql, params),
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
