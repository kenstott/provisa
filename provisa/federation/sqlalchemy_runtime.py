# Copyright (c) 2026 Kenneth Stott
# Canary: 5c373332-91d0-47f4-81d2-e2a1dbec984b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""SqlAlchemyFederationRuntime — the self-only SQLAlchemy engine's runtime (REQ-905).

A single SQLAlchemy connection IS the engine and its own store. Every source LANDs into that store
(no in-place attach — the ``self-only`` reach model), so ``attach_source`` is a no-op: the physical
``schema.table`` resolves to the store's own landed table. Governed physical SQL runs directly against
the store. Conforms to the NativeEngineBackend runtime protocol: connection, run/run_sync,
attach_source, ensure_materialize_attached.
"""

from __future__ import annotations

import asyncio
from typing import Any

from provisa.executor.result import QueryResult, ResultStream, StreamingQueryResult
from provisa.federation.runtime_support import _STREAM_BATCH_ROWS

# Leading keywords of a row-returning statement — the only kind a server-side (streaming) cursor is
# valid for. Everything else (DDL/DML) is executed buffered; psycopg2 rejects DECLARE CURSOR FOR it
# (REQ-1222). A leading line/block comment is stripped before the keyword is read.
_ROW_RETURNING = frozenset({"SELECT", "WITH", "VALUES", "TABLE", "SHOW", "EXPLAIN"})


def _is_row_returning(sql: str) -> bool:
    s = sql.lstrip()
    while s.startswith("--") or s.startswith("/*"):
        if s.startswith("--"):
            s = s[s.find("\n") + 1 :].lstrip() if "\n" in s else ""
        else:
            end = s.find("*/")
            s = s[end + 2 :].lstrip() if end != -1 else ""
    return s[:12].split(None, 1)[0].upper() in _ROW_RETURNING if s else False


class SqlAlchemyFederationRuntime:  # REQ-825, REQ-840, REQ-905
    def __init__(self, *, url: str) -> None:
        from sqlalchemy import create_engine

        self._sa = create_engine(url)
        self._con = self._sa.raw_connection()  # a DBAPI connection (cursor) — cache terminal + run

    # -- source exposure -------------------------------------------------------

    def attach_source(self, source: Any) -> None:
        """Self-only: a source LANDs into the store; there is nothing to attach in place. The landed
        rows are a native table in the store, so the compiled physical name resolves directly."""
        return None

    # -- materialization store -------------------------------------------------

    def ensure_materialize_attached(self) -> str:
        """The store IS this engine's own database, so cache/landed tables live here directly; the
        reference is the store's database name (a catalog-physical ``db.schema.table`` cache ref then
        resolves natively)."""
        return self._sa.url.database or ""

    @property
    def connection(self):
        """The DBAPI connection — the backend's cache terminal issues CREATE TABLE/INSERT through its
        ``cursor()`` into the store, and run() executes against it."""
        return self._con

    # -- execution -------------------------------------------------------------

    def run_sync(self, sql: str, params: list | None = None) -> ResultStream:
        """Execute SQL already in the store's dialect (transpiled by the backend seam) and STREAM it.

        A raw DBAPI cursor over psycopg2 buffers the ENTIRE result client-side on ``execute`` (its
        default unnamed cursor), so ``fetchmany`` alone would not bound memory. Genuine streaming needs
        a SERVER-SIDE cursor; SQLAlchemy exposes that portably through the ``stream_results`` execution
        option — psycopg2 opens a named server-side cursor, other drivers use their equivalent. A
        server-side cursor is only valid for a ROW-RETURNING statement, though: psycopg2 eagerly issues
        ``DECLARE ... CURSOR FOR <sql>`` at execute, which is a syntax error for DDL/DML. So a
        non-row-returning statement runs BUFFERED in a committing transaction (it returns no rows to
        bound) and a row-returning one streams server-side. Either way the read runs on a DEDICATED
        connection from the engine, isolated from the ``self._con`` cache/write connection; the
        streaming connection closes when the stream drains (``on_close``). Consumers that call ``.rows``
        still get the full list — the buffering is then explicit at their call site (REQ-1217,
        REQ-1222)."""
        if not _is_row_returning(sql):  # DDL / DML — no server-side cursor; execute + commit now
            with self._sa.begin() as c:
                c.exec_driver_sql(sql, tuple(params) if params else ())
            return QueryResult(rows=[], column_names=[])

        conn = self._sa.connect().execution_options(
            stream_results=True, yield_per=_STREAM_BATCH_ROWS
        )
        result = conn.exec_driver_sql(sql, tuple(params) if params else ())

        def _close(*_: Any) -> None:
            conn.close()

        cols = list(result.keys())

        def _batches() -> Any:
            while True:
                chunk = result.fetchmany(_STREAM_BATCH_ROWS)
                if not chunk:
                    return
                yield [tuple(r) for r in chunk]

        return StreamingQueryResult(_batches(), column_names=cols, on_close=_close)

    async def run(self, sql: str, params: list | None = None) -> QueryResult:
        """Async variant: MATERIALIZES on the executor (unlike ``run_sync``), because a lazy
        ``fetchmany`` pulled across the async boundary would block the event loop (REQ-1217)."""
        loop = asyncio.get_event_loop()

        def _run() -> QueryResult:
            cur = self._con.cursor()
            cur.execute(sql, params or None)
            cols = [d[0] for d in cur.description] if cur.description else []
            rows = list(cur.fetchall()) if cur.description else []
            self._con.commit()
            cur.close()
            return QueryResult(rows=rows, column_names=cols)

        return await loop.run_in_executor(None, _run)

    def close(self) -> None:
        self._con.close()
        self._sa.dispose()
