# Copyright (c) 2026 Kenneth Stott
# Canary: 32904a51-ac86-4cba-8fb8-d5d43e6e7af3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""SQLAlchemy dialect for DuckDB as the embedded control-plane store (REQ-828).

Built on the sync ``duckdb_engine`` dialect (the control-plane engine is synchronous and shared by
every request thread — REQ-1882). Two things differ from stock ``duckdb_engine``:

- an integer autoincrement primary key is realized as a sequence + ``DEFAULT nextval`` (DuckDB has
  no SERIAL, which the inherited PG compiler would emit);
- DuckDB reports no ``rowcount`` for INSERT/UPDATE/DELETE, returning the affected count as a one-row
  ``Count`` result set; the cursor translates that into ``rowcount`` so the store abstraction's
  rowcount-driven paths (upsert match detection, DELETE status) behave as on PostgreSQL.

An in-memory DuckDB exists only inside its one connection, so it is served by a ``StaticPool`` (not
``duckdb_engine``'s ``SingletonThreadPool``, which would give every request thread its own empty
database); :class:`provisa.core.database.Database` serializes access to that single connection.

Registered as the ``duckdb+provisa`` driver; ``create_engine_from_url`` resolves a control-plane
``duckdb://`` URI onto it.
"""

from __future__ import annotations

import re
from collections import deque
from typing import Any

from sqlalchemy import event, pool, schema
from sqlalchemy.dialects import registry
from sqlalchemy.dialects.postgresql.base import PGDDLCompiler
from sqlalchemy.sql import sqltypes

import duckdb_engine

_DML_VERB = re.compile(r"^\s*(?:insert|update|delete)\b", re.IGNORECASE)
_RETURNING = re.compile(r"\breturning\b", re.IGNORECASE)
DRIVER = "provisa"


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
    # Scoped to this control-plane driver; a no-op for every other backend (including plain
    # duckdb_engine stores, which never carry an autoincrement control-plane PK).
    if getattr(connection.dialect, "driver", None) != DRIVER:
        return
    col = target._autoincrement_column
    if _needs_autoinc_sequence(col, connection.dialect):
        connection.exec_driver_sql(f"CREATE SEQUENCE IF NOT EXISTS {_autoinc_seq_name(col)}")


class _Cursor:
    """Buffered DBAPI cursor over a ``duckdb_engine`` connection.

    Each ``execute`` runs the whole driver-cursor lifecycle (cursor → execute → fetch → close) and
    buffers the rows, so later ``fetch*`` calls are in-memory reads — DuckDB reports no
    server-side cursor."""

    server_side = False

    def __init__(self, adapt_connection: "_Connection") -> None:
        self._sync_conn = adapt_connection._sync_conn
        # duckdb_engine subclasses the psycopg2 PG execution context, whose post_exec reads
        # ``cursor.connection.notices``; expose the ConnectionWrapper (its ``.notices`` is []).
        self.connection = self._sync_conn
        self.arraysize = 1
        self.rowcount = -1
        self.lastrowid = -1
        self.description: Any = None
        self._rows: deque[Any] = deque()

    def execute(self, operation: Any, parameters: Any = None) -> None:
        cur = self._sync_conn.cursor()
        if parameters is None:
            cur.execute(operation)
        else:
            cur.execute(operation, parameters)
        desc = cur.description
        if (
            desc is not None
            and len(desc) == 1
            and desc[0][0] == "Count"
            and _DML_VERB.match(operation)
            and not _RETURNING.search(operation)
        ):
            rows = cur.fetchall()
            cur.close()
            self.description = None
            self.rowcount = int(rows[0][0]) if rows else -1
            return
        rows = cur.fetchall() if desc else None
        rowcount = cur.rowcount
        cur.close()
        if desc:
            self.description = desc
            self.rowcount = -1
            self._rows = deque(rows or ())
        else:
            self.description = None
            self.rowcount = rowcount

    def executemany(self, operation: Any, seq_of_parameters: Any) -> None:
        cur = self._sync_conn.cursor()
        cur.executemany(operation, seq_of_parameters)
        self.rowcount = cur.rowcount
        cur.close()
        self.description = None

    def setinputsizes(self, *inputsizes: Any) -> None:
        pass

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


class _Connection:
    """DBAPI connection wrapping a ``duckdb_engine`` ``ConnectionWrapper``."""

    def __init__(self, sync_conn: Any) -> None:
        self._sync_conn = sync_conn

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

    @property
    def notices(self) -> list[str]:
        return self._sync_conn.notices

    def cursor(self, server_side: bool = False) -> _Cursor:
        del server_side  # DuckDB exposes no distinct server-side cursor
        return _Cursor(self)

    def execute(self, operation: Any, parameters: Any = None) -> _Cursor:
        cur = self.cursor()
        cur.execute(operation, parameters)
        return cur

    def begin(self) -> None:
        self._sync_conn.begin()

    def commit(self) -> None:
        self._sync_conn.commit()

    def rollback(self) -> None:
        self._sync_conn.rollback()

    def interrupt(self) -> None:
        """Abort the statement in flight on this connection (the request-deadline cancel)."""
        self._sync_conn.interrupt()

    def close(self) -> None:
        self._sync_conn.close()


class DuckDBStoreDialect(duckdb_engine.Dialect):
    """Control-plane DuckDB dialect: ``duckdb_engine``'s PG-flavored SQL compilation with the
    autoincrement DDL and DML-rowcount translation above."""

    driver = DRIVER
    supports_statement_cache = True
    ddl_compiler = DuckDBDDLCompiler

    def connect(self, *cargs: Any, **cparams: Any) -> _Connection:  # type: ignore[override]
        return _Connection(duckdb_engine.Dialect.connect(self, *cargs, **cparams))

    @classmethod
    def get_pool_class(cls, url: Any) -> type[pool.Pool]:
        if url.database in (None, "", ":memory:"):
            return pool.StaticPool
        return pool.QueuePool


def register() -> None:
    """Register ``duckdb+provisa`` with SQLAlchemy's dialect registry (idempotent)."""
    registry.register(f"duckdb.{DRIVER}", "provisa.core.duckdb_store", "DuckDBStoreDialect")


register()

dialect = DuckDBStoreDialect
