# Copyright (c) 2026 Kenneth Stott
# Canary: 54db22a2-65d6-47e1-a189-699b04b38b5b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""PgFederationRuntime — the PostgreSQL engine's in-process federation runtime (REQ-904).

A single PostgreSQL connection is the engine. Each registered source is ATTACHed in place through the
(postgres, source_type) connector's SQL/MED DDL — postgres_fdw / sqlite_fdw import a foreign schema,
file_fdw defines a per-table foreign table — and then wrapped in a physical-named view so the compiled
``schema.table`` reference resolves unchanged. Non-attachable sources LAND into the materialization
store; for the pg engine the store is a PostgreSQL, so its cache/landed tables live in a schema the
engine reads directly. Conforms to the NativeEngineBackend runtime protocol: connection, run/run_sync,
attach_source, ensure_materialize_attached.
"""

from __future__ import annotations

import asyncio
from typing import Any

import psycopg2

from provisa.executor.result import QueryResult, ResultStream, StreamingQueryResult
from provisa.federation.engine import build_pg_engine
from provisa.federation.runtime_support import _STREAM_BATCH_ROWS


class PgFederationRuntime:  # REQ-825, REQ-840, REQ-904
    def __init__(self, *, engine_dsn: str, materialize_dsn: str | None = None) -> None:
        self._engine = build_pg_engine()
        self._con = psycopg2.connect(engine_dsn)
        self._con.autocommit = True
        self._engine_dsn = engine_dsn
        # The materialization store for a Postgres engine is a Postgres — its own DB unless an external
        # store is configured. Landed/cached rows live in a schema this same connection reads.
        self._materialize_dsn = materialize_dsn
        self._raw_attached: set[str] = set()

    # -- source exposure -------------------------------------------------------

    def attach_source(self, source: Any) -> None:
        """Expose an ATTACH source at its physical ``schema.table`` via the engine's connector DDL."""
        entry = self._engine.resolve(source)  # picks the (postgres, source_type) connector
        details = entry.details
        cur = self._con.cursor()
        if "attach_ddl" in details:  # postgres_fdw / sqlite_fdw — import a foreign schema
            if source.id not in self._raw_attached:
                for ddl in details["attach_ddl"]:
                    cur.execute(ddl)
                self._raw_attached.add(source.id)
            remote = f'"{details["local_schema"]}"."{source.table_name}"'
        elif (
            "server_ddl" in details
        ):  # file_fdw (csv) — per-table foreign table from column metadata
            if source.id not in self._raw_attached:
                for ddl in details["server_ddl"]:
                    cur.execute(ddl)
                self._raw_attached.add(source.id)
            cols = ", ".join(f'"{n}" {t}' for n, t in source.columns)
            ft = f'"{details["server"]}__{source.table_name}"'
            cur.execute(
                f"CREATE FOREIGN TABLE IF NOT EXISTS {ft} ({cols}) "
                f"SERVER {details['server']} {details['table_options']}"
            )
            remote = ft
        else:
            raise KeyError(f"pg connector for {source.type.value!r} has no attach/server DDL")
        cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{source.schema_name}"')
        cur.execute(
            f'CREATE OR REPLACE VIEW "{source.schema_name}"."{source.table_name}" '
            f"AS SELECT * FROM {remote}"
        )

    # -- materialization store -------------------------------------------------

    def ensure_materialize_attached(self) -> str:
        """The Postgres engine materializes into a Postgres store. When it is this same DB the cache
        lives here directly, so the reference is the current database name — a catalog-physical
        ``db.schema.table`` cache ref then resolves natively. (An external store is future work.)"""
        cur = self._con.cursor()
        cur.execute("SELECT current_database()")
        row = cur.fetchone()
        assert row is not None  # SELECT current_database() always returns exactly one row
        return row[0]

    @property
    def connection(self):
        """The psycopg2 connection — the backend's cache terminal issues CREATE TABLE/INSERT through
        its ``cursor()`` into the materialization store (this Postgres)."""
        return self._con

    # -- execution -------------------------------------------------------------

    def run_sync(self, sql: str, params: list | None = None) -> ResultStream:
        """Execute governed physical SQL (a SELECT — transpiled by the backend seam) and STREAM it.

        A psycopg2 default cursor buffers the entire result client-side on ``execute``, so ``fetchmany``
        alone would not bound memory. Genuine streaming needs a SERVER-SIDE (named) cursor, which holds
        an open portal and thus requires a transaction — incompatible with the engine connection's
        ``autocommit``. So the read runs on a DEDICATED short-lived connection (autocommit off): the
        named cursor pulls ``itersize`` rows per round-trip from Postgres, peak memory bounded by one
        batch. The cursor/transaction/connection all close when the stream drains (``on_close``). A
        private connection also isolates the open portal from the autocommit write/cache connection and
        from other concurrent streams. Consumers that call ``.rows`` still get the full list — the
        buffering is then explicit at their call site (REQ-1217)."""
        read_con = psycopg2.connect(self._engine_dsn)
        cur = read_con.cursor(name="provisa_stream")  # named ⇒ server-side portal
        cur.itersize = _STREAM_BATCH_ROWS
        cur.execute(sql, params or None)
        # psycopg2 populates a NAMED cursor's ``.description`` only after the first FETCH, so peek one
        # batch to force the portal and expose the columns before building the stream.
        first = cur.fetchmany(_STREAM_BATCH_ROWS)

        def _close(*_: Any) -> None:
            cur.close()
            read_con.commit()
            read_con.close()

        if not cur.description:  # non-row-returning statement — drain now
            _close()
            return QueryResult(rows=[], column_names=[])
        cols = [d[0] for d in cur.description]

        def _batches() -> Any:
            if first:
                yield first
            while True:
                chunk = cur.fetchmany(_STREAM_BATCH_ROWS)
                if not chunk:
                    return
                yield chunk

        return StreamingQueryResult(_batches(), column_names=cols, on_close=_close)

    # -- Arrow transport (ADBC zero-copy) (REQ-1220) ---------------------------

    def run_arrow(self, sql: str, params: list | None = None) -> Any:
        """Execute governed physical SQL and return a ``pyarrow.Table`` via the ADBC PostgreSQL
        driver's native Arrow reader — Postgres rows are decoded straight into Arrow, so NO Python
        rows are materialized for the Flight/airport transport (zero-copy relative to the row path).

        A dedicated short-lived ADBC connection isolates the read from the engine's psycopg2
        write/cache connection; it closes when the table is built (REQ-1220)."""
        from adbc_driver_postgresql import dbapi as adbc_pg

        con = adbc_pg.connect(self._engine_dsn)
        try:
            cur = con.cursor()
            cur.execute(sql, params or None)
            return cur.fetch_arrow_table()
        finally:
            con.close()

    def run_arrow_stream(self, sql: str, params: list | None = None) -> tuple[Any, Any]:
        """Execute governed physical SQL and return ``(schema, batch_generator)`` for lazy
        record-batch streaming. ADBC's ``fetch_record_batch`` yields an Arrow ``RecordBatchReader``
        that pulls batches from the Postgres server on demand, so the full result never materializes
        — peak memory is bounded by one batch. The dedicated ADBC connection closes when the
        generator drains or the consumer stops early (REQ-1220)."""
        from adbc_driver_postgresql import dbapi as adbc_pg

        con = adbc_pg.connect(self._engine_dsn)
        cur = con.cursor()
        cur.execute(sql, params or None)
        reader = cur.fetch_record_batch()
        schema = reader.schema

        def _batches() -> Any:
            try:
                for batch in reader:
                    yield batch
            finally:
                cur.close()
                con.close()

        return schema, _batches()

    async def run(self, sql: str, params: list | None = None) -> QueryResult:
        """Async variant: MATERIALIZES on the executor (unlike ``run_sync``), because a lazy
        server-side ``fetchmany`` pulled across the async boundary would block the event loop. Runs on
        the engine's autocommit connection with a client-side cursor (REQ-1217)."""
        loop = asyncio.get_event_loop()

        def _run() -> QueryResult:
            cur = self._con.cursor()
            cur.execute(sql, params or None)
            cols = [d[0] for d in cur.description] if cur.description else []
            rows = list(cur.fetchall()) if cur.description else []
            cur.close()
            return QueryResult(rows=rows, column_names=cols)

        return await loop.run_in_executor(None, _run)

    def close(self) -> None:
        self._con.close()
