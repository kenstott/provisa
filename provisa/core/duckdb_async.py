# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Async SQLAlchemy dialect for DuckDB — the embedded control-plane store (REQ-828).

``duckdb_engine`` is sync-only; ``create_async_engine`` rejects it ("not async").
REQ-828 requires the control-plane store to run on an embedded engine (DuckDB or
SQLite) behind the one :class:`~provisa.core.database.Database` abstraction, which
wraps a SQLAlchemy ``AsyncEngine``. This module supplies the missing async driver
by mediating the sync ``duckdb_engine`` DBAPI through a single dedicated worker
thread per connection (the same thread-affinity model aiosqlite uses for pysqlite)
and adapting it with SQLAlchemy's greenlet bridge (``await_only``).

Registered as the ``duckdb+aioduckdb`` driver; ``create_engine_from_url`` rewrites a
bare ``duckdb://`` control-plane URI onto it. DuckDB has no socket I/O, so this is
not true non-blocking I/O — it is a working asyncio interface (parity with aiosqlite),
which is exactly what the embedded, single-process desktop store needs.
"""
# complexity-gate: allow-ble=5 reason="async DBAPI adapter exception boundaries (execute/executemany/begin/commit/rollback): each catches the sync duckdb_engine driver error and re-raises it through _handle_exception (annotated NoReturn — it always `raise`s), the SQLAlchemy async-adapter translation seam (same pattern as SQLAlchemy's own aiosqlite/asyncpg dialects). These do NOT swallow — the exception always propagates; ruff cannot see the re-raise through the helper call"

from __future__ import annotations

import re
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from typing import Any, NoReturn

from sqlalchemy import event, pool, schema
from sqlalchemy.connectors.asyncio import AsyncAdapt_dbapi_module
from sqlalchemy.dialects import registry
from sqlalchemy.dialects.postgresql.base import PGDDLCompiler
from sqlalchemy.engine import AdaptedConnection
from sqlalchemy.sql import sqltypes
from sqlalchemy.util.concurrency import await_only

import duckdb_engine

_DML_VERB = re.compile(r"^\s*(?:insert|update|delete)\b", re.IGNORECASE)
_RETURNING = re.compile(r"\breturning\b", re.IGNORECASE)


# --------------------------------------------------------------------------- #
# autoincrement DDL — DuckDB has no SERIAL, so integer autoincrement PKs are
# realized as a sequence + ``DEFAULT nextval`` (dialect-neutral schema_org keeps
# ``autoincrement=True``; only the DuckDB emission differs, scoped to this driver).
# --------------------------------------------------------------------------- #
def _autoinc_seq_name(column: Any) -> str:
    return f"{column.table.name}_{column.name}_seq"


def _needs_autoinc_sequence(column: Any, dialect: Any) -> bool:
    """True for an integer autoincrement primary key the PG compiler would emit as
    SERIAL (which DuckDB rejects)."""
    if column is None or column.table is None:
        return False
    impl_type = column.type.dialect_impl(dialect)
    if isinstance(impl_type, sqltypes.TypeDecorator):
        impl_type = impl_type.impl
    return bool(
        column.primary_key
        and column is column.table._autoincrement_column
        and column.identity is None
        and (
            column.default is None
            or (isinstance(column.default, schema.Sequence) and column.default.optional)
        )
        and isinstance(impl_type, sqltypes.Integer)
    )


class DuckDBDDLCompiler(PGDDLCompiler):
    def get_column_specification(self, column: Any, **kw: Any) -> str:
        if _needs_autoinc_sequence(column, self.dialect):
            colspec = self.preparer.format_column(column)
            colspec += " " + self.dialect.type_compiler_instance.process(
                column.type, type_expression=column, identifier_preparer=self.preparer
            )
            colspec += f" DEFAULT nextval('{_autoinc_seq_name(column)}')"
            if not column.nullable:
                colspec += " NOT NULL"
            return colspec
        return super().get_column_specification(column, **kw)

    def define_constraint_cascades(self, constraint: Any) -> str:
        # DuckDB rejects referential actions (ON DELETE CASCADE / SET NULL / SET DEFAULT).
        # The control plane enforces cascade/nullify semantics in the app layer (repositories),
        # not the store, so dropping the clause is behavior-preserving on the embedded backend.
        return ""


@event.listens_for(schema.Table, "before_create")
def _create_autoinc_sequences(target: Any, connection: Any, **kw: Any) -> None:
    # Scoped to the async DuckDB control-plane driver; a no-op for every other backend
    # (including sync DuckDB stores, which never carry an autoincrement control-plane PK).
    if getattr(connection.dialect, "driver", None) != "aioduckdb":
        return
    col = target._autoincrement_column
    if _needs_autoinc_sequence(col, connection.dialect):
        connection.exec_driver_sql(f"CREATE SEQUENCE IF NOT EXISTS {_autoinc_seq_name(col)}")


class _Worker:
    """One dedicated thread per connection.

    DuckDB connections are not safe for concurrent use across threads; pinning a
    connection to a single thread (as aiosqlite does) both serializes access and
    keeps the connection's thread affinity stable across awaits."""

    def __init__(self) -> None:
        self._ex = ThreadPoolExecutor(max_workers=1, thread_name_prefix="duckdb-cp")

    def run(self, fn: Any) -> Any:
        import asyncio

        return asyncio.get_event_loop().run_in_executor(self._ex, fn)

    def shutdown(self) -> None:
        self._ex.shutdown(wait=False)


class _Cursor:
    """asyncio-mediated DBAPI cursor: every DuckDB call runs on the connection's
    worker thread and is driven back into the greenlet via ``await_``.

    A single worker hop per ``execute`` bundles the whole cursor lifecycle
    (cursor → execute → fetch → close), so results are buffered eagerly and later
    ``fetch*`` calls are pure in-memory reads — DuckDB reports no server-side cursor."""

    server_side = False

    def __init__(self, adapt_connection: "_Connection") -> None:
        self._adapt = adapt_connection
        self._sync_conn = adapt_connection._sync_conn
        self._worker = adapt_connection._worker
        self.await_ = adapt_connection.await_
        # duckdb_engine subclasses the psycopg2 PG execution context, whose post_exec
        # reads ``cursor.connection.notices``; expose the sync ConnectionWrapper (its
        # ``.notices`` is []) so that introspection path works over the async adapter.
        self.connection = self._sync_conn
        self.arraysize = 1
        self.rowcount = -1
        self.lastrowid = -1
        self.description: Any = None
        self._rows: deque[Any] = deque()

    def execute(self, operation: Any, parameters: Any = None) -> None:
        def _run() -> tuple[Any, list[Any] | None, int]:
            cur = self._sync_conn.cursor()
            if parameters is None:
                cur.execute(operation)
            else:
                cur.execute(operation, parameters)
            desc = cur.description
            # DuckDB reports no ``rowcount`` for INSERT/UPDATE/DELETE; it returns the affected
            # count as a one-row, one-column ``Count`` result set instead. Translate that into
            # an asyncpg-style rowcount (and no result set) so the store abstraction's rowcount-
            # driven paths (upsert match detection, DELETE status) behave identically to PG.
            if (
                desc is not None
                and len(desc) == 1
                and desc[0][0] == "Count"
                and _DML_VERB.match(operation)
                and not _RETURNING.search(operation)
            ):
                rows = cur.fetchall()
                cur.close()
                return None, None, int(rows[0][0]) if rows else -1
            rows = cur.fetchall() if desc else None
            rowcount = cur.rowcount
            cur.close()
            return desc, rows, rowcount

        try:
            desc, rows, rowcount = self.await_(self._worker.run(_run))
        except Exception as error:
            self._adapt._handle_exception(error)
        if desc:
            self.description = desc
            self.rowcount = -1
            self._rows = deque(rows or ())
        else:
            self.description = None
            self.rowcount = rowcount

    def executemany(self, operation: Any, seq_of_parameters: Any) -> None:
        def _run() -> int:
            cur = self._sync_conn.cursor()
            cur.executemany(operation, seq_of_parameters)
            rowcount = cur.rowcount
            cur.close()
            return rowcount

        try:
            self.rowcount = self.await_(self._worker.run(_run))
        except Exception as error:
            self._adapt._handle_exception(error)
        self.description = None

    def setinputsizes(self, *inputsizes: Any) -> None:
        pass

    async def _async_soft_close(self) -> None:
        # Rows are already buffered and the driver cursor closed within execute();
        # nothing to release here (parity with aiosqlite's buffered cursor).
        return

    def close(self) -> None:
        self._rows.clear()

    def __iter__(self) -> Any:
        while self._rows:
            yield self._rows.popleft()

    def fetchone(self) -> Any:
        return self._rows.popleft() if self._rows else None

    def fetchmany(self, size: int | None = None) -> list[Any]:
        if size is None:
            size = self.arraysize
        rr = self._rows
        return [rr.popleft() for _ in range(min(size, len(rr)))]

    def fetchall(self) -> list[Any]:
        retval = list(self._rows)
        self._rows.clear()
        return retval


class _Connection(AdaptedConnection):
    """asyncio-mediated DBAPI connection wrapping a sync ``duckdb_engine`` connection."""

    await_ = staticmethod(await_only)

    def __init__(self, dbapi: Any, sync_conn: Any, worker: _Worker) -> None:
        self.dbapi = dbapi
        self._sync_conn = sync_conn
        self._connection = sync_conn  # AdaptedConnection.driver_connection
        self._worker = worker

    # ``pool_pre_ping`` (psycopg do_ping) reads/sets these on the DBAPI connection.
    @property
    def autocommit(self) -> Any:
        return self._sync_conn.autocommit

    @autocommit.setter
    def autocommit(self, value: Any) -> None:
        self._sync_conn.autocommit = value

    @property
    def closed(self) -> Any:
        return self._sync_conn.closed

    def cursor(self, server_side: bool = False) -> _Cursor:
        # DuckDB exposes no distinct server-side cursor; buffered cursor is used for both.
        return _Cursor(self)

    def execute(self, operation: Any, parameters: Any = None) -> _Cursor:
        cur = self.cursor()
        cur.execute(operation, parameters)
        return cur

    def begin(self) -> None:
        # duckdb_engine.Dialect.do_begin issues an explicit BEGIN on the DBAPI connection.
        try:
            self.await_(self._worker.run(self._sync_conn.begin))
        except Exception as error:
            self._handle_exception(error)

    def commit(self) -> None:
        try:
            self.await_(self._worker.run(self._sync_conn.commit))
        except Exception as error:
            self._handle_exception(error)

    def rollback(self) -> None:
        try:
            self.await_(self._worker.run(self._sync_conn.rollback))
        except Exception as error:
            self._handle_exception(error)

    def close(self) -> None:
        try:
            self.await_(self._worker.run(self._sync_conn.close))
        finally:
            self._worker.shutdown()

    def _handle_exception(self, error: Exception) -> NoReturn:
        raise error


class _DuckDBAsyncDBAPI(AsyncAdapt_dbapi_module):
    """The async DBAPI module surface. Exposes the sync driver's error classes and
    ``numeric_dollar`` paramstyle so SQLAlchemy compiles ``$1`` binds DuckDB accepts."""

    def __init__(self, sync_dbapi: Any) -> None:
        self._sync = sync_dbapi
        self.paramstyle = sync_dbapi.paramstyle
        for name in ("Error", "TransactionException", "ParserException", "Binary"):
            if hasattr(sync_dbapi, name):
                setattr(self, name, getattr(sync_dbapi, name))

    def __getattr__(self, key: str) -> Any:
        return getattr(self._sync, key)


class DuckDBDialectAsync(duckdb_engine.Dialect):
    """Async DuckDB dialect. Inherits ``duckdb_engine``'s PG-flavored SQL compilation
    (it subclasses the psycopg2 PG dialect) and only replaces connection creation with
    the worker-thread async adapter."""

    driver = "aioduckdb"
    is_async = True
    supports_statement_cache = True
    ddl_compiler = DuckDBDDLCompiler

    @classmethod
    def import_dbapi(cls) -> _DuckDBAsyncDBAPI:  # type: ignore[override]
        return _DuckDBAsyncDBAPI(duckdb_engine.DBAPI)

    # SQLAlchemy calls dbapi() on older versions, import_dbapi() on newer; expose both.
    dbapi = import_dbapi  # type: ignore[assignment]  # SQLAlchemy's Dialect.dbapi declares a different callable signature

    def connect(self, *cargs: Any, **cparams: Any) -> _Connection:  # type: ignore[override]
        worker = _Worker()

        def _make() -> Any:
            # Reuse duckdb_engine's sync connect (extension preload, config apply,
            # ConnectionWrapper) but run it on the worker thread for thread affinity.
            return duckdb_engine.Dialect.connect(self, *cargs, **cparams)

        sync_conn = await_only(worker.run(_make))
        return _Connection(self.loaded_dbapi, sync_conn, worker)

    @classmethod
    def get_pool_class(cls, url: Any) -> type[pool.Pool]:
        # An in-memory DuckDB lives only inside its connection, so all access must
        # share one connection (StaticPool); a file DB uses the async queue pool.
        db = url.database
        if db in (None, "", ":memory:"):
            return pool.StaticPool
        return pool.AsyncAdaptedQueuePool


def register() -> None:
    """Register ``duckdb+aioduckdb`` with SQLAlchemy's dialect registry (idempotent)."""
    registry.register("duckdb.aioduckdb", "provisa.core.duckdb_async", "DuckDBDialectAsync")


register()

dialect = DuckDBDialectAsync
