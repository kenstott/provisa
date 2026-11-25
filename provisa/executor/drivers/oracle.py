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

# Requirements: REQ-052, REQ-229, REQ-550, REQ-1882, REQ-2194

from __future__ import annotations

from collections.abc import Generator
from typing import Any

import oracledb  # pyright: ignore[reportMissingImports]

from provisa.core import request_deadline
from provisa.core.sync_pool import BlockingPool
from provisa.executor.drivers.base import DirectDriver
from provisa.executor.drivers.cursor_stream import FETCH_CHUNK_ROWS, PooledCursorStream
from provisa.executor.drivers.oracle_arrow import arrow_statement, arrow_table, oracle_rows
from provisa.executor.drivers.zoned import oracle_connection
from provisa.executor.result import ArrowResult, QueryResult


def _is_broken(exc: BaseException) -> bool:
    return isinstance(exc, (oracledb.OperationalError, oracledb.InterfaceError))


def _read_arrow(conn: Any, sql: str, bound: Any) -> QueryResult:  # noqa: ANN401 — oracledb
    """``sql`` run on ``conn``. A statement that answers rows is read whole as Arrow
    (``fetch_df_all``, its NUMBER(p,s) columns as decimals), a column with no Arrow form cast in
    the database first or refused by name (oracle_arrow.arrow_statement; REQ-2050: a zoned
    timestamp is read as its instant); one that answers none is run on a cursor."""
    with conn.cursor() as cur:
        statement, described = arrow_statement(cur, sql)
        if not described:
            cur.execute(statement, bound)
            return QueryResult(rows=[], column_names=[])
    names = [d.name.lower() for d in described]
    frame = conn.fetch_df_all(statement, bound, arraysize=FETCH_CHUNK_ROWS, fetch_decimals=True)
    return ArrowResult(arrow_table(frame, names, described), to_rows=oracle_rows).with_rows()


class _ArrowCursor:
    """A statement read as Arrow for :class:`PooledCursorStream` (REQ-2194): python-oracledb's
    ``fetch_df_batches`` hands on one batch of ``FETCH_CHUNK_ROWS`` rows at a time;
    ``fetch_batch`` gives each as a pyarrow record batch, and ``fetchmany`` builds only the rows it
    is asked for from the batch in hand, never python-oracledb's own rows. The statement is parsed
    on a cursor of the connection first: one that answers no rows is run on it, and a column
    with no Arrow form is cast in the database or refused by name (oracle_arrow.arrow_statement)."""

    def __init__(self, conn: Any) -> None:  # noqa: ANN401 — an oracledb connection
        self._conn = conn
        self._cursor = conn.cursor()
        self._frames: Generator[Any, None, None] | None = None
        self._pending: list[Any] = []  # the record batches of the frame in hand
        self._described: Any = None
        self._batch: Any = None  # the record batch in hand
        self._offset = 0  # its first row not yet fetched
        self.description: Any = None
        self.column_names: list[str] = []

    def execute(self, sql: str, bound: Any) -> None:  # noqa: ANN401 — oracledb binds
        statement, described = arrow_statement(self._cursor, sql)
        if not described:
            self._cursor.execute(statement, bound)
            return
        self._described = described
        self.column_names = [d.name.lower() for d in described]
        frames = self._conn.fetch_df_batches(
            statement, bound, size=FETCH_CHUNK_ROWS, fetch_decimals=True
        )
        # The statement runs as its first batch is asked for: it is asked for now, so an error
        # in the statement is raised as it is run.
        self._frames = frames
        self._pending = self._next_batches()
        self.description = described

    def _next_batches(self) -> list[Any]:
        frame = next(self._frames, None) if self._frames is not None else None
        if frame is None:
            self._frames = None
            return []
        table = arrow_table(frame, self.column_names, self._described)
        return [b for b in table.to_batches() if b.num_rows]

    def fetch_batch(self) -> Any:  # noqa: ANN401 — a pyarrow RecordBatch, or None once exhausted
        """The rest of the batch in hand, else the next batch; None once the result is read."""
        if self._batch is not None:
            batch, self._batch = self._batch.slice(self._offset), None
            return batch
        while not self._pending and self._frames is not None:
            self._pending = self._next_batches()
        return self._pending.pop(0) if self._pending else None

    def fetchmany(self, size: int) -> list[tuple]:
        rows: list[tuple] = []
        while len(rows) < size:
            if self._batch is None:
                batch = self.fetch_batch()
                if batch is None:
                    break
                self._batch, self._offset = batch, 0
            take = min(size - len(rows), self._batch.num_rows - self._offset)
            rows.extend(oracle_rows(self._batch.slice(self._offset, take)))
            self._offset += take
            if self._offset >= self._batch.num_rows:
                self._batch = None
        return rows

    def close(self) -> None:
        frames, self._frames = self._frames, None
        self._pending, self._batch = [], None
        if frames is not None:
            frames.close()
        self._cursor.close()


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
            # REQ-2050: a zoned timestamp is read as the instant it is.
            lambda: oracle_connection(oracledb.connect(user=user, password=password, dsn=dsn)),
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
        """The statement's result read as Arrow through python-oracledb's data-frame fetch
        (REQ-2194, oracle_arrow), never its Python rows: the table, with its rows built from it
        now, on this thread. A statement that answers no rows is run on a cursor."""
        # oracledb binds a list positionally, one value per placeholder OCCURRENCE (a repeated
        # :1 is two positions) — so each occurrence gets its own :k and the values follow in order.
        from provisa.compiler.params import bind_positionally

        exec_sql, bound = bind_positionally(sql, params, lambda k: f":{k}")

        with self._require_pool().connection(is_broken=_is_broken) as conn:
            try:
                with request_deadline.cancel_on_deadline(conn.cancel):
                    result = _read_arrow(conn, exec_sql, bound)
                # Statement-level commit, matching the other direct drivers (autocommit semantics).
                conn.commit()
            except BaseException as exc:
                if not _is_broken(exc):
                    conn.rollback()
                raise
        return result

    @property
    def supports_streaming(self) -> bool:  # REQ-1190
        return True

    # Async only for the DirectDriver awaitable contract; opens synchronously in-thread.
    async def open_stream(
        self, sql: str, params: list | None = None
    ) -> PooledCursorStream:  # REQ-1190
        """The result a batch at a time, read as Arrow (REQ-2194): python-oracledb's
        ``fetch_df_batches`` reads ``FETCH_CHUNK_ROWS`` rows per batch from the server, and the
        rows a fetch asks for are built from the batch in hand (:class:`_ArrowCursor`)."""
        from provisa.compiler.params import bind_positionally

        exec_sql, bound = bind_positionally(sql, params, lambda k: f":{k}")
        return PooledCursorStream(
            self._require_pool(),
            open_cursor=_ArrowCursor,
            execute=lambda cur: cur.execute(exec_sql, bound),
            columns=lambda cur: cur.column_names,
            cancel=lambda conn, _cur: conn.cancel(),
            # Statement-level commit, as execute() does (autocommit semantics).
            finish=lambda conn: conn.commit(),
        )

    # Async only for the DirectDriver awaitable contract; closes synchronously in-thread.
    async def close(self) -> None:
        pool, self._pool = self._pool, None
        if pool is not None:
            pool.closeall()

    @property
    def is_connected(self) -> bool:
        return self._pool is not None
