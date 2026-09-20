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
            # pyodbc's cursor.execute (unlike SQLAlchemy Core's own statement execution, used
            # elsewhere in this file) treats an explicitly-passed `None` second argument as ONE
            # parameter whose value is NULL, not "no parameters" — verified live (REQ-1730
            # engine-swap harness, 2026-09-20): a parameterless "SELECT 1" warmup probe against the
            # mssql engine raised `pyodbc.ProgrammingError: The SQL contains 0 parameter markers,
            # but 1 parameters were supplied`. Omit the argument entirely when there are none.
            if params:
                cur.execute(sql, params)
            else:
                cur.execute(sql)
            cols = [d[0] for d in cur.description] if cur.description else []
            rows = list(cur.fetchall()) if cur.description else []
            self._con.commit()
            cur.close()
            return QueryResult(rows=rows, column_names=cols)

        return await loop.run_in_executor(None, _run)

    # -- landing -----------------------------------------------------------

    async def attach_landed_source(
        self, source: Any, columns: list[tuple[str, str]], *, pk_columns: list[str] | None = None
    ) -> str:
        """Eager reconcile (boot/registration, REQ-846/932): converge ``source.schema_name``/
        ``source.table_name`` to ``columns`` + ``pk_columns`` (DDL only, no rows — the refresh's job
        is ``land_table``). ``NativeEngineBackend.reconcile_landed_tables`` no-ops for any runtime
        lacking this method (``hasattr(runtime, "attach_landed_source")``) — before this, the
        generic sqlalchemy engine's boot-time reconcile silently did nothing, so a MATERIALIZE_ONLY
        source's table was never created ahead of its first query (verified live, REQ-1730
        engine-swap harness, 2026-09-20: SQL Server raised `Invalid object name` on the very first
        query after a reboot into this engine).

        Unlike Snowflake/Databricks/BigQuery's own ``attach_landed_source`` (which lands into a
        MANGLED replica name and exposes a VIEW at the compiler's physical name), this engine's
        ``SqlAlchemyBackend.landing_target`` override already lands MATERIALIZE_ONLY sources
        directly at ``(schema_name, table_name)`` — the same address the compiler emits — so there
        is no separate replica/view indirection to converge here, just the one table.

        METADATA DRIFT (required, REQ-846/REQ-1651's own contract — mirrors
        ``snowflake_store.reconcile_snowflake_native`` exactly, generalized via SQLAlchemy Core's
        ``Inspector`` instead of an information_schema query hand-written per dialect): an existing
        table whose column set/order OR primary key no longer matches ``columns``/``pk_columns`` is
        DROPPED and RECREATED — its data re-lands on the next ``land_table`` refresh, same as
        Snowflake's own contract states ("a drifted one is recreated"). A table that already matches
        survives untouched (restart-safe); a table that does not exist yet is created. Returns
        ``created`` | ``kept`` | ``recreated``."""
        from provisa.federation.materialize_exec import build_table

        schema, table = source.schema_name, source.table_name
        tbl = build_table(schema, table, columns, tuple(pk_columns or ()))

        def _run() -> str:
            with self._sa.begin() as conn:
                if schema:
                    _ensure_schema(conn, schema)
                return _reconcile_table_shape(conn, tbl)

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _run)

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
        """Land ``rows`` into ``schema.table`` of THIS engine's own store (REQ-1730) — the
        ``NativeEngineBackend.land_source_table``/``hasattr(runtime, "land_table")`` seam every
        other native engine's runtime (DuckDB/Snowflake/Databricks/BigQuery) already uses to bypass
        the base ``EngineBackend`` default, which lands through ``store_writer``'s ASYNC driver
        (``_ASYNC_DRIVER`` — postgresql/mysql/mariadb/sqlite only; no async pyodbc driver is
        installed, and none should be added just for this) against
        ``self.engine.materialize_store()``, a SEPARATE database from this one. Runs synchronously
        on the executor (matching ``run()`` above, and Snowflake/Databricks's own
        ``asyncio.to_thread`` pattern) via ``self._sa`` — the SAME sync SQLAlchemy engine ``run_sync``
        already uses, so DDL/DML compiles through the real per-product dialect (Core's own
        identifier quoting handles case-sensitivity automatically; no dialect-specific SQL here,
        mirroring materialize_exec.py's own "vanilla SQLAlchemy Core, no dialect-specific SQL"
        contract, which this reimplements synchronously since that module's helpers are built
        against the async ``StoreConn`` protocol).

        Three landing shapes (REQ-932, mirrors materialize_exec.py's async versions exactly):
        REPLACE (delete + bulk insert), APPEND (bulk insert, or per-row UPDATE-then-INSERT upsert
        when ``pk_columns`` is given), CDC (per-event upsert/delete by PK — requires ``pk_columns``).
        """
        from provisa.core.change_signal import APPEND, CDC, REPLACE, select_landing_shape
        from provisa.federation.materialize_exec import _coerce_json_row, _json_columns, build_table

        del match_floor  # REQ-960 idempotency window — not yet applicable to this sync path
        pk = list(pk_columns or ())
        landing_shape = shape or select_landing_shape(change_signal, watermark_column)
        tbl = build_table(schema, table, columns, tuple(pk))

        def _run() -> str:
            with self._sa.begin() as conn:
                if schema:
                    _ensure_schema(conn, schema)
                _ensure_table(conn, tbl)
                json_cols = _json_columns(tbl)
                coerced = [_coerce_json_row(r, json_cols) for r in rows]
                if landing_shape == REPLACE:
                    conn.execute(tbl.delete())
                    if coerced:
                        conn.execute(tbl.insert(), coerced)
                elif landing_shape == APPEND:
                    if pk:
                        for row in coerced:
                            _sync_upsert(conn, tbl, row, index_elements=pk)
                    elif coerced:
                        conn.execute(tbl.insert(), coerced)
                elif landing_shape == CDC:
                    if not pk:
                        raise ValueError(
                            f"CDC land into {schema}.{table} requires primary key columns"
                        )
                    for row in coerced:
                        _sync_upsert(conn, tbl, row, index_elements=pk)
                else:
                    raise ValueError(f"unhandled landing shape {landing_shape!r}")
            return f"{schema}.{table}" if schema else table

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _run)

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
        already landed in this engine's own store (REQ-1733) — the sync equivalent of
        materialize_exec.py's ``apply_cdc``, for the same reason ``land_table`` above reimplements
        REPLACE/APPEND/CDC synchronously rather than going through the async ``store_writer`` path."""
        from provisa.federation.materialize_exec import _coerce_json_row, _json_columns, build_table

        if not pk_columns:
            raise ValueError(f"CDC land into {schema}.{table} requires primary key columns")
        tbl = build_table(schema, table, columns, tuple(pk_columns))

        def _run() -> dict[str, int]:
            counts = {"upsert": 0, "delete": 0}
            json_cols = _json_columns(tbl)
            with self._sa.begin() as conn:
                if schema:
                    _ensure_schema(conn, schema)
                _ensure_table(conn, tbl)
                for ev in events:
                    if ev.operation.lower() == "delete":
                        where = _and_eq(tbl, pk_columns, ev.row)
                        conn.execute(tbl.delete().where(where))
                        counts["delete"] += 1
                    else:
                        _sync_upsert(
                            conn,
                            tbl,
                            _coerce_json_row(ev.row, json_cols),
                            index_elements=pk_columns,
                        )
                        counts["upsert"] += 1
            return counts

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _run)

    def close(self) -> None:
        self._con.close()
        self._sa.dispose()


def _ensure_schema(conn: Any, schema: str) -> None:
    """Create ``schema`` if absent — an existence check + plain ``CreateSchema``, not
    ``CreateSchema(schema, if_not_exists=True)``. Verified live (REQ-1730 engine-swap harness,
    2026-09-20): SQL Server's dialect does not translate the ``if_not_exists`` flag into T-SQL at
    all (there is no ``CREATE SCHEMA IF NOT EXISTS`` in T-SQL), so SQLAlchemy emits that flag's
    literal Postgres/SQLite spelling regardless of dialect — ``Incorrect syntax near the keyword
    'IF'``. ``Inspector.has_schema`` (SQLAlchemy 2.0+) is the portable, dialect-correct existence
    check every backend implements properly, unlike the DDL flag."""
    from sqlalchemy import inspect
    from sqlalchemy.schema import CreateSchema

    if not inspect(conn).has_schema(schema):
        conn.execute(CreateSchema(schema))


def _ensure_table(conn: Any, table: Any) -> None:
    """Create ``table`` if absent — same portable existence-check pattern as ``_ensure_schema``,
    for the identical reason: ``CreateTable(table, if_not_exists=True)`` hit the same T-SQL
    ``Incorrect syntax near the keyword 'IF'`` live against SQL Server (verified 2026-09-20)."""
    from sqlalchemy import inspect
    from sqlalchemy.schema import CreateTable

    if not inspect(conn).has_table(table.name, schema=table.schema):
        conn.execute(CreateTable(table))


def _reconcile_table_shape(conn: Any, table: Any) -> str:
    """Converge ``table`` to its declared columns + primary key (REQ-846/REQ-1651) — DDL only, no
    data. Mirrors ``snowflake_store.reconcile_snowflake_native`` exactly (created/kept/recreated),
    generalized across dialects via SQLAlchemy Core's ``Inspector`` instead of a hand-written
    information_schema query: absent -> CREATE; an exact column-set/order + primary-key match ->
    KEPT (survives a restart untouched); anything else (added/removed/reordered/retyped-by-name
    columns, or a changed primary key) -> DROP + CREATE (a drifted replica's data re-lands on the
    next ``land_table`` refresh — dropping it here never re-lands rows itself)."""
    from sqlalchemy import inspect
    from sqlalchemy.schema import CreateTable, DropTable

    insp = inspect(conn)
    if not insp.has_table(table.name, schema=table.schema):
        conn.execute(CreateTable(table))
        return "created"
    have_cols = [c["name"] for c in insp.get_columns(table.name, schema=table.schema)]
    want_cols = [c.name for c in table.columns]
    have_pk = (
        insp.get_pk_constraint(table.name, schema=table.schema).get("constrained_columns") or []
    )
    want_pk = [c.name for c in table.columns if c.primary_key]
    if have_cols == want_cols and sorted(have_pk) == sorted(want_pk):
        return "kept"
    conn.execute(DropTable(table))
    conn.execute(CreateTable(table))
    return "recreated"


def _and_eq(table: Any, cols: list[str], row: dict) -> Any:
    from sqlalchemy import and_

    return and_(*[table.c[c] == row.get(c) for c in cols])


def _sync_upsert(conn: Any, table: Any, values: dict, *, index_elements: list[str]) -> None:
    """Sync mirror of ``provisa.core.database.Connection.upsert``: UPDATE by the conflict keys, then
    INSERT if no row matched — generic Core only (no dialect-specific ON CONFLICT/MERGE/ON DUPLICATE
    KEY), so it works on every SQLAlchemy backend. No savepoint isolation on the INSERT's
    IntegrityError branch (unlike the async version): this runs inside ``land_table``'s own single
    transaction per land call, which every dialect this engine family targets can roll a statement
    back within without aborting the whole transaction (unlike asyncpg's stricter behavior, which is
    what motivated the async version's savepoint in the first place)."""
    from sqlalchemy import insert as _insert, literal, select as _select, update as _update
    from sqlalchemy.exc import IntegrityError

    cols = [c for c in values if c not in index_elements]
    set_map = {c: values[c] for c in cols}
    where = _and_eq(table, index_elements, values)

    if set_map:
        res = conn.execute(_update(table).where(where).values(**set_map))
        if (res.rowcount or 0) > 0:
            return
    else:
        exists = conn.execute(_select(literal(1)).select_from(table).where(where))
        if exists.fetchone() is not None:
            return
    try:
        conn.execute(_insert(table).values(**values))
    except IntegrityError:
        if set_map:
            res = conn.execute(_update(table).where(where).values(**set_map))
            if (res.rowcount or 0) > 0:
                return
        raise
