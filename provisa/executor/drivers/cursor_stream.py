# Copyright (c) 2026 Kenneth Stott
# Canary: 8c246134-1571-4e5c-8715-0b0ff78a7527
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A DIRECT read handed back in bounded batches by a blocking DB-API driver (REQ-1190).

MySQL / MariaDB (PyMySQL's unbuffered cursor), SQL Server (pyodbc) and Oracle (oracledb) each
fetch from the server as ``fetchmany`` asks, so a stream over one holds one batch, not the
result. The connection is borrowed from the driver's pool for the stream's life. A stream read
to its end gives the connection back; one closed early has its statement cancelled and the
connection discarded (its protocol state is unknown), as does one whose read fails or is cut
by the request's deadline.
"""

# Requirements: REQ-1190, REQ-1905, REQ-1915

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

from provisa.core import request_deadline
from provisa.core.sync_pool import BlockingPool
from provisa.executor.drivers.base import DirectResultStream

log = logging.getLogger(__name__)

#: Rows converted per fetch of a materialized result, between two looks at the request's
#: deadline (the PostgreSQL driver's chunk).
FETCH_CHUNK_ROWS = 50_000


def fetch_chunked(cursor: Any) -> list[tuple]:
    """Every row of ``cursor``'s result, fetched in chunks with the request's deadline checked
    between them, not in one ``fetchall()`` the deadline cannot end until it returns."""
    rows: list[tuple] = []
    while chunk := cursor.fetchmany(FETCH_CHUNK_ROWS):
        rows.extend(tuple(r) for r in chunk)
        request_deadline.check()
    return rows


class PooledCursorStream(DirectResultStream):
    """One statement's result, read a batch at a time from a cursor on a pooled connection.

    ``open_cursor(conn)`` makes the driver's streaming cursor; ``execute(cursor)`` runs the
    statement; ``columns(cursor)`` names the result's columns; ``cancel(conn, cursor)`` stops a
    statement still sending rows; ``finish(conn)`` ends a read that completed (a commit where
    the driver needs one). ``per_fetch(cursor, size)`` sets what the driver buffers per round
    trip to the batch the reader asked for, for a driver that buffers by a cursor setting."""

    def __init__(
        self,
        pool: BlockingPool[Any],
        *,
        open_cursor: Callable[[Any], Any],
        execute: Callable[[Any], None],
        columns: Callable[[Any], list[str]],
        cancel: Callable[[Any, Any], None],
        finish: Callable[[Any], None] | None = None,
        per_fetch: Callable[[Any, int], None] | None = None,
    ) -> None:
        self._pool = pool
        self._cancel = cancel
        self._finish = finish
        self._per_fetch = per_fetch
        self._conn: Any = None
        self._cursor: Any = None
        self.column_names = []
        self.column_types = None
        shield = request_deadline.shielded()
        conn = None
        try:
            with shield.lock:
                shield.settle()
                conn = pool.getconn()
            cursor = open_cursor(conn)
            self._conn, self._cursor = conn, cursor
            with request_deadline.cancel_on_deadline(lambda: cancel(conn, cursor)):
                execute(cursor)
            if cursor.description is None:
                self._release(completed=True)  # a statement with no result: nothing to read
            else:
                self.column_names = columns(cursor)
        except BaseException:
            if self._conn is None and conn is not None:
                with shield.lock:
                    shield.settle()
                    pool.discard(conn)
            else:
                self._release(completed=False)
            raise

    def _release(self, *, completed: bool) -> None:
        """Give the connection back (a completed read) or discard it (anything else). Taken
        inside the shield: the request's deadline does not interrupt it (REQ-1905)."""
        shield = request_deadline.shielded()
        with shield.lock:
            shield.settle()
            conn, self._conn = self._conn, None
            cursor, self._cursor = self._cursor, None
            if conn is None:
                return
            if completed:
                try:
                    cursor.close()
                    if self._finish is not None:
                        self._finish(conn)
                except BaseException:
                    self._pool.discard(conn)
                    raise
                self._pool.putconn(conn)
                return
            try:
                self._cancel(conn, cursor)
            except Exception as exc:  # allow-ble: the connection is discarded whatever the cancel did; a failed cancel is logged with its cause
                log.warning("cancelling an abandoned DIRECT stream failed: %s", exc)
            self._pool.discard(conn)

    # Async only for the DirectResultStream awaitable contract; fetches synchronously in-thread.
    async def fetch(self, size: int) -> list[tuple]:
        conn, cursor = self._conn, self._cursor
        if cursor is None:
            return []
        try:
            if self._per_fetch is not None:
                self._per_fetch(cursor, size)
            with request_deadline.cancel_on_deadline(lambda: self._cancel(conn, cursor)):
                rows = cursor.fetchmany(size)
        except BaseException:
            self._release(completed=False)
            raise
        if not rows:
            self._release(completed=True)
            return []
        return [tuple(r) for r in rows]

    # Async only for the DirectResultStream awaitable contract; closes synchronously in-thread.
    async def close(self) -> None:
        self._release(completed=False)

    def __del__(self) -> None:
        # A stream dropped without close() — its request ended between open and whatever would
        # have closed it — still frees its pool slot.
        if self._conn is not None:
            self._release(completed=False)
