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
from concurrent.futures import ThreadPoolExecutor
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
        # land_table/apply_cdc_events's dedicated single-worker executor — NOT the loop's default
        # pool. Same reasoning as DuckDBFederationRuntime._land_executor: self._con is ONE shared
        # connection, and dispatching writes against it via the default (multi-worker) executor
        # lets concurrent lands for different tables pile onto the same connection's cursors at
        # once instead of queuing — confirmed live as a real regression on the DuckDB engine (13
        # threads simultaneously blocked in one executemany call, 20+ min of accumulated CPU for
        # what should have been a handful of small serialized lands).
        self._land_executor = ThreadPoolExecutor(max_workers=1)

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

    # -- landing (MATERIALIZE_ONLY sources — no live FDW reach) -----------------

    def _existing_columns(self, cur: Any, schema: str, table: str) -> list[str]:
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position",
            (schema, table),
        )
        return [r[0] for r in cur.fetchall()]

    def _existing_pk(self, cur: Any, schema: str, table: str) -> list[str]:
        cur.execute(
            """
            SELECT a.attname
            FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            WHERE i.indrelid = %s::regclass AND i.indisprimary
            """,
            (f'"{schema}"."{table}"',),
        )
        return [r[0] for r in cur.fetchall()]

    def _create_table_ddl(
        self, schema: str, table: str, columns: list[tuple[str, str]], pk_columns: list[str]
    ) -> str:
        from provisa.core.ir_types import to_physical

        pk = set(pk_columns)
        col_defs = [f'"{name}" {to_physical(sql_type, "postgresql")}' for name, sql_type in columns]
        if pk:
            col_defs.append(f"PRIMARY KEY ({', '.join(f'"{c}"' for c in pk)})")
        return f'CREATE TABLE "{schema}"."{table}" ({", ".join(col_defs)})'

    def attach_landed_source(
        self, source: Any, columns: list[tuple[str, str]], *, pk_columns: list[str] | None = None
    ) -> str:
        """Eager reconcile (boot/registration, REQ-846/REQ-1651): converge the source's landed
        table to ``columns`` + ``pk_columns`` (DDL only, no rows — the refresh's job is
        ``land_table``) — the sync mirror of ``SqlAlchemyFederationRuntime.attach_landed_source``,
        raw psycopg2 SQL instead of SQLAlchemy Core since this runtime never uses SQLAlchemy.
        Returns ``created`` | ``kept`` | ``recreated`` (drift = column set/order or PK mismatch).

        The schema is ``{catalog}_{schema_name}``, not bare ``schema_name`` — MUST match
        ``PgBackend.landing_target``'s own fold exactly (its own docstring has the full reasoning:
        this engine's ``catalog_qualified=False`` means the compiler folds the catalog into the
        schema rather than stripping it, since two MATERIALIZED sources can otherwise share a
        native ``schema_name`` and collide once catalog qualification is gone). This method is
        reached independently of ``landing_target`` (``reconcile_landed_tables`` calls it directly
        off the registered-table row, not through the backend's ``landing_target`` seam), so it
        must recompute the SAME fold here rather than trust a value threaded through."""
        from provisa.compiler.naming import source_to_catalog

        schema = f"{source_to_catalog(source.id)}_{source.schema_name}"
        table = source.table_name
        want_cols = [name for name, _ in columns]
        want_pk = list(pk_columns or ())
        cur = self._con.cursor()
        cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
        cur.execute("SELECT to_regclass(%s)", (f'"{schema}"."{table}"',))
        row = cur.fetchone()
        assert row is not None  # to_regclass is a scalar function — always exactly one row
        if row[0] is None:
            cur.execute(self._create_table_ddl(schema, table, columns, want_pk))
            return "created"
        have_cols = self._existing_columns(cur, schema, table)
        have_pk = self._existing_pk(cur, schema, table)
        if have_cols == want_cols and sorted(have_pk) == sorted(want_pk):
            return "kept"
        cur.execute(f'DROP TABLE "{schema}"."{table}"')
        cur.execute(self._create_table_ddl(schema, table, columns, want_pk))
        return "recreated"

    def _ensure_table(
        self,
        cur: Any,
        schema: str,
        table: str,
        columns: list[tuple[str, str]],
        pk_columns: list[str],
    ) -> None:
        cur.execute("SELECT to_regclass(%s)", (f'"{schema}"."{table}"',))
        row = cur.fetchone()
        assert row is not None  # to_regclass is a scalar function — always exactly one row
        if row[0] is None:
            cur.execute(self._create_table_ddl(schema, table, columns, pk_columns))

    async def land_table(
        self,
        *,
        schema: str,
        table: str,
        columns: list[tuple[str, str]],
        rows: list[dict],
        change_signal: str = "ttl",
        watermark_column: str | None = None,
        pk_columns: list[str] | None = None,
        match_floor: float = 0.0,
        shape: str | None = None,
    ) -> str:
        """Land ``rows`` into ``schema.table`` of THIS engine's own store (REQ-1730) — create-if-
        absent only (drift is ``attach_landed_source``'s job), then REPLACE (delete+insert) or
        APPEND (insert, or upsert-by-key via Postgres's native ``ON CONFLICT`` when ``pk_columns``
        is given).

        Was a plain (non-``async``) ``def`` awaited unconditionally by
        ``NativeEngineBackend.land_source_table`` (``await runtime.land_table(...)``) — every call
        crashed with ``TypeError: object str can't be used in 'await' expression`` the moment a
        Postgres-store land fired (TTL background refresh or a query's own read-triggered stale
        materialize, REQ-1661). Now ``async``, dispatched to the executor on a PRIVATE cursor —
        same reasoning as ``DuckDBFederationRuntime.land_table``/``StreamingQueryResult``'s own
        private-cursor comments: run inline, a large land blocks the event loop for every other
        query on this connection's engine, not just callers of this table."""
        from provisa.core.change_signal import APPEND, CDC, REPLACE, select_landing_shape

        del match_floor
        pk = list(pk_columns or ())
        landing_shape = shape or select_landing_shape(change_signal, watermark_column)
        names = [name for name, _ in columns]

        def _run() -> str:
            cur = self._con.cursor()
            try:
                self._ensure_table(cur, schema, table, columns, pk)
                if landing_shape == REPLACE:
                    cur.execute(f'DELETE FROM "{schema}"."{table}"')
                    self._insert_rows(cur, schema, table, names, rows)
                elif landing_shape == APPEND:
                    if pk:
                        self._upsert_rows(cur, schema, table, names, pk, rows)
                    else:
                        self._insert_rows(cur, schema, table, names, rows)
                elif landing_shape == CDC:
                    if not pk:
                        raise ValueError(
                            f"CDC land into {schema}.{table} requires primary key columns"
                        )
                    self._upsert_rows(cur, schema, table, names, pk, rows)
                else:
                    raise ValueError(f"unhandled landing shape {landing_shape!r}")
                return f"{schema}.{table}"
            finally:
                cur.close()

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self._land_executor, _run)

    def _insert_rows(
        self, cur: Any, schema: str, table: str, names: list[str], rows: list[dict]
    ) -> None:
        if not rows:
            return
        cols_sql = ", ".join(f'"{n}"' for n in names)
        placeholders = ", ".join(["%s"] * len(names))
        values = [tuple(r.get(n) for n in names) for r in rows]
        cur.executemany(
            f'INSERT INTO "{schema}"."{table}" ({cols_sql}) VALUES ({placeholders})', values
        )

    def _upsert_rows(
        self,
        cur: Any,
        schema: str,
        table: str,
        names: list[str],
        pk: list[str],
        rows: list[dict],
    ) -> None:
        if not rows:
            return
        cols_sql = ", ".join(f'"{n}"' for n in names)
        placeholders = ", ".join(["%s"] * len(names))
        conflict_cols = ", ".join(f'"{c}"' for c in pk)
        update_cols = [n for n in names if n not in pk]
        set_sql = ", ".join(f'"{n}" = EXCLUDED."{n}"' for n in update_cols)
        do_update = f"DO UPDATE SET {set_sql}" if update_cols else "DO NOTHING"
        values = [tuple(r.get(n) for n in names) for r in rows]
        cur.executemany(
            f'INSERT INTO "{schema}"."{table}" ({cols_sql}) VALUES ({placeholders}) '
            f"ON CONFLICT ({conflict_cols}) {do_update}",
            values,
        )

    async def apply_cdc_events(
        self,
        *,
        schema: str,
        table: str,
        columns: list[tuple[str, str]],
        pk_columns: list[str],
        events: list,
    ) -> dict[str, int]:
        """Apply CDC change events (insert/update -> upsert by PK, delete -> tombstone) to a table
        already landed in this engine's own store (REQ-1733) — the raw-psycopg2 mirror of
        ``SqlAlchemyFederationRuntime.apply_cdc_events``.

        Dispatched to the executor, same reasoning as ``land_table`` above: this was ``async def``
        but ran its psycopg2 work inline, so a large event batch still blocked the event loop for
        every other query on this engine for its whole duration."""
        if not pk_columns:
            raise ValueError(f"CDC land into {schema}.{table} requires primary key columns")
        names = [name for name, _ in columns]

        def _run() -> dict[str, int]:
            cur = self._con.cursor()
            try:
                self._ensure_table(cur, schema, table, columns, pk_columns)
                counts = {"upsert": 0, "delete": 0}
                for ev in events:
                    if ev.operation.lower() == "delete":
                        where = " AND ".join(f'"{c}" = %s' for c in pk_columns)
                        cur.execute(
                            f'DELETE FROM "{schema}"."{table}" WHERE {where}',
                            tuple(ev.row.get(c) for c in pk_columns),
                        )
                        counts["delete"] += 1
                    else:
                        self._upsert_rows(cur, schema, table, names, pk_columns, [ev.row])
                        counts["upsert"] += 1
                return counts
            finally:
                cur.close()

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self._land_executor, _run)

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
