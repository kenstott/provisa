# Copyright (c) 2026 Kenneth Stott
# Canary: 9bce9c4e-bb27-4271-bb29-cd8a1ec6e6ef
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A session's temporary tables written in place (REQ-2176): rows added and removed by key, a
column added or widened, and each table's key indexed -- without landing the table again whole.

A store whose engine backend offers this face (:meth:`EngineBackend.session_rows`) writes a
session's tables through it: the embedded DuckDB store through its broker, as every write to that
file is, and a PostgreSQL or other SQLAlchemy store on its own connection. Any other store has no
such face, and a session's table is landed whole there on every change
(:mod:`provisa.pgwire.temp_exec`)."""

# Requirements: REQ-2176

from __future__ import annotations

from typing import Any, Protocol


class SessionRows(Protocol):
    """In-place writes to a session's temporary table ``schema.table``."""

    async def insert(
        self, schema: str, table: str, columns: list[tuple[str, str]], batch: Any
    ) -> None:
        """Add the Arrow ``batch``'s rows, in ``columns``' order."""
        ...

    async def delete(
        self, schema: str, table: str, key_columns: tuple[str, ...], keys: list[tuple]
    ) -> None:
        """Remove the rows whose ``key_columns`` hold one of ``keys``."""
        ...

    async def add_column(self, schema: str, table: str, column: str, kind: str) -> None:
        """Add the column ``column`` of the landed type ``kind``, NULL in every row."""
        ...

    async def retype(self, schema: str, table: str, column: str, kind: str) -> None:
        """Widen ``column`` to the landed type ``kind``, each value cast to it."""
        ...

    async def index(self, schema: str, table: str, key_columns: tuple[str, ...]) -> None:
        """Index the table on its key ``key_columns``, which no two rows share."""
        ...


def _ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _index_name(table: str) -> str:
    import hashlib

    # Short and fixed, as a build table's name is: a long table name cannot run it past the
    # store's identifier limit.
    return "key__" + hashlib.sha256(table.encode()).hexdigest()[:24]


def _column_type(kind: str, dialect: Any, *, dialect_name: str | None = None) -> str:
    """The store's DDL type of the landed type ``kind``, as a landing creates the column."""
    from provisa.federation.materialize_exec import build_table

    table = build_table("s", "t", [("c", kind)], (), dialect_name=dialect_name)
    return table.c["c"].type.compile(dialect=dialect)


class DuckDBSessionRows:
    """A session's tables in the embedded DuckDB store file, written through the store broker:
    one locked operation each, which the engine's copies of the file then see."""

    def __init__(self, broker: Any) -> None:
        self._broker = broker

    def _table(self, schema: str, table: str) -> str:
        from provisa.federation.materialize_broker import _MAT_STORE_ALIAS
        from provisa.federation.store_connection import _qualified

        return _qualified(_MAT_STORE_ALIAS, schema, table)

    def _run(self, fn: Any, *, unseen: bool) -> Any:
        import asyncio

        return asyncio.to_thread(self._broker._run, fn, unseen=unseen)

    async def insert(
        self, schema: str, table: str, columns: list[tuple[str, str]], batch: Any
    ) -> None:
        import pyarrow as pa

        names = ", ".join(_ident(n) for n, _ in columns)
        target = self._table(schema, table)

        def _do(con: Any) -> None:
            con.register("_session_rows", pa.Table.from_batches([batch]))
            try:
                con.execute(f'INSERT INTO {target} ({names}) SELECT {names} FROM "_session_rows"')
            finally:
                con.unregister("_session_rows")

        await self._run(_do, unseen=False)

    async def delete(
        self, schema: str, table: str, key_columns: tuple[str, ...], keys: list[tuple]
    ) -> None:
        import pyarrow as pa

        target = self._table(schema, table)
        held = pa.table({c: [k[i] for k in keys] for i, c in enumerate(key_columns)})
        same = " AND ".join(f"k.{_ident(c)} = t.{_ident(c)}" for c in key_columns)

        def _do(con: Any) -> None:
            con.register("_session_keys", held)
            try:
                con.execute(
                    f'DELETE FROM {target} AS t WHERE EXISTS (SELECT 1 FROM "_session_keys" k '
                    f"WHERE {same})"
                )
            finally:
                con.unregister("_session_keys")

        await self._run(_do, unseen=False)

    def _alter(self, schema: str, table: str, sql: str) -> Any:
        """Run the ``ALTER TABLE`` ``sql``. DuckDB alters no table an index stands on, so the
        table's indexes are dropped first and built again after, in the one operation."""
        from provisa.federation.materialize_broker import _MAT_STORE_ALIAS

        def _do(con: Any) -> None:
            held = con.execute(
                "SELECT index_name, sql FROM duckdb_indexes() WHERE database_name = ? "
                "AND schema_name = ? AND table_name = ?",
                [_MAT_STORE_ALIAS, schema, table],
            ).fetchall()
            for name, _ in held:
                con.execute(f"DROP INDEX {_MAT_STORE_ALIAS}.{_ident(schema)}.{_ident(name)}")
            con.execute(sql)
            if not held:
                return
            # An index's own DDL names its table by schema alone: built with the store current.
            was = con.execute("SELECT current_database()").fetchone()[0]
            con.execute(f"USE {_MAT_STORE_ALIAS}")
            try:
                for _, ddl in held:
                    con.execute(ddl)
            finally:
                con.execute(f"USE {_ident(was)}")

        return self._run(_do, unseen=False)

    async def add_column(self, schema: str, table: str, column: str, kind: str) -> None:
        from provisa.federation.store_connection import _duckdb_dialect

        await self._alter(
            schema,
            table,
            f"ALTER TABLE {self._table(schema, table)} ADD COLUMN {_ident(column)} "
            f"{_column_type(kind, _duckdb_dialect())}",
        )

    async def retype(self, schema: str, table: str, column: str, kind: str) -> None:
        from provisa.federation.store_connection import _duckdb_dialect

        await self._alter(
            schema,
            table,
            f"ALTER TABLE {self._table(schema, table)} ALTER COLUMN {_ident(column)} "
            f"TYPE {_column_type(kind, _duckdb_dialect())}",
        )

    async def index(self, schema: str, table: str, key_columns: tuple[str, ...]) -> None:
        cols = ", ".join(_ident(c) for c in key_columns)
        sql = (
            f"CREATE UNIQUE INDEX IF NOT EXISTS {_ident(_index_name(table))} "
            f"ON {self._table(schema, table)} ({cols})"
        )
        await self._run(lambda con: con.execute(sql), unseen=True)


class SqlAlchemySessionRows:
    """A session's tables in a PostgreSQL or other SQLAlchemy store, written on the store's own
    connection (:func:`provisa.federation.store_writer.store_connection`)."""

    def __init__(self, dsn: str) -> None:
        self._dsn = dsn

    async def _execute(self, statement: Any, rows: list[dict] | None = None) -> None:
        """One statement on its own connection, committed (the connection autocommits)."""
        from provisa.federation.store_writer import store_connection

        async with store_connection(self._dsn) as conn:
            if rows is None:
                await conn.execute_core(statement)
            else:
                await conn.execute_core_many(statement, rows)

    async def _dialect(self) -> Any:
        from sqlalchemy import make_url

        return make_url(self._dsn).get_dialect()()

    async def insert(
        self, schema: str, table: str, columns: list[tuple[str, str]], batch: Any
    ) -> None:
        from provisa.federation.materialize_exec import build_table

        target = build_table(schema, table, columns, ())
        await self._execute(target.insert(), batch.to_pylist())

    async def delete(
        self, schema: str, table: str, key_columns: tuple[str, ...], keys: list[tuple]
    ) -> None:
        from sqlalchemy import Column, MetaData, Table, tuple_

        target = Table(table, MetaData(), *(Column(c) for c in key_columns), schema=schema)
        await self._execute(target.delete().where(tuple_(*target.c).in_(keys)))

    async def add_column(self, schema: str, table: str, column: str, kind: str) -> None:
        from sqlalchemy import text

        kind_sql = _column_type(kind, await self._dialect())
        await self._execute(
            text(
                f"ALTER TABLE {_ident(schema)}.{_ident(table)} ADD COLUMN {_ident(column)} {kind_sql}"
            )
        )

    async def retype(self, schema: str, table: str, column: str, kind: str) -> None:
        from sqlalchemy import text

        kind_sql = _column_type(kind, await self._dialect())
        await self._execute(
            text(
                f"ALTER TABLE {_ident(schema)}.{_ident(table)} ALTER COLUMN {_ident(column)} "
                f"TYPE {kind_sql} USING {_ident(column)}::{kind_sql}"
            )
        )

    async def index(self, schema: str, table: str, key_columns: tuple[str, ...]) -> None:
        from sqlalchemy import text

        cols = ", ".join(_ident(c) for c in key_columns)
        await self._execute(
            text(
                f"CREATE UNIQUE INDEX IF NOT EXISTS {_ident(_index_name(table))} "
                f"ON {_ident(schema)}.{_ident(table)} ({cols})"
            )
        )
