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

from provisa.executor.result import QueryResult, ResultStream
from provisa.federation.runtime_support import stream_from_dbapi


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
        """Execute SQL already in the store's dialect (transpiled by the backend seam).

        Streams rows lazily over a PRIVATE cursor (batched ``fetchmany``) so a large result never
        fully materializes in the runtime — matches the DuckDB terminal (REQ-1217). The transaction
        commits and the cursor closes when the stream drains (``on_close``); a non-SELECT (``None``
        description) drains immediately, so a write commits at once as before. Consumers that call
        ``.rows`` still get the full list — the buffering is then explicit at their call site."""
        cur = self._con.cursor()
        cur.execute(sql, params or None)

        def _close(*_: Any) -> None:
            self._con.commit()
            cur.close()

        return stream_from_dbapi(cur, on_close=_close)

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
