# Copyright (c) 2026 Kenneth Stott
# Canary: 68d3bb01-dd63-443b-bcc4-ed104ef5efe1
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Where a replica's rows are written: a build table, then an atomic swap (REQ-1915, REQ-1912).

A replica build never writes into the replica a reader sees. It fills a fresh BUILD table beside
it and, when the copy is complete, swaps that table in as one atomic step, so a reader sees the
previous replica until then and a build that dies leaves the previous replica intact.

A target is chosen by the STORE the replica lives in, not by the engine that reads it. A
PostgreSQL store has one write face (:class:`PostgresStoreTarget`) whichever engine reads it:
Trino through its PostgreSQL catalog, the PostgreSQL engine directly, DuckDB through its attach.
The engine contributes only what it must do after a swap (``after_swap``).

No file is written on this host: a batch goes from memory to the store's connection.
"""

# Requirements: REQ-1915, REQ-1912

from __future__ import annotations

import asyncio
import hashlib
import logging
from typing import Any, Protocol

import pyarrow as pa

from provisa.core import request_deadline
from provisa.federation.data_replicator import TargetCaps, TargetLoad, TargetWrite

log = logging.getLogger(__name__)


class ReplicaTarget(Protocol):
    """One replica's write face in its store."""

    caps: TargetCaps

    async def begin(self) -> None:
        """Open the store and create the empty build table."""
        ...

    async def write(self, batch: pa.RecordBatch, rows: list[dict]) -> None:
        """Add one batch to the build table. ``rows`` is the same batch as Python rows, already
        made for the content hash; a face that writes rows uses it instead of converting the
        batch again."""
        ...

    async def swap(self) -> None:
        """Replace the replica with the build table in one atomic step, and release the store."""
        ...

    async def abort(self) -> None:
        """Discard the build table and release the store. The replica is untouched."""
        ...

    async def drop(self) -> None:
        """Remove the replica and anything a build of it left beside it (REQ-1915): the model
        no longer declares it. Not part of a build; called on a target that was never begun."""
        ...


def build_table_name(table: str) -> str:
    """The name of the build table of the replica ``table``: short and fixed, so a long replica
    name cannot run it past the store's identifier limit, and one per replica, so a build that
    died is found and dropped by the next one."""
    return "build__" + hashlib.sha256(table.encode()).hexdigest()[:24]


def _q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _libpq_dsn(dsn: str) -> str:
    """``dsn`` without a SQLAlchemy driver suffix (``postgresql+psycopg://`` → ``postgresql://``)."""
    scheme, sep, rest = dsn.partition("://")
    return f"{scheme.split('+', 1)[0]}://{rest}" if sep else dsn


class DuckDBStoreTarget:
    """A replica in an embedded DuckDB store file, written through the store broker.

    The store is a single-writer file that every process opens, uses and closes under one lock
    (``materialize_broker``). Each batch is one such operation appending to the build table, so
    readers are not shut out for the length of the build; the store reads the Arrow batch in
    place. The swap is one operation: drop and rename in a transaction. A build that dies
    leaves only its build table, which the next build drops."""

    caps = TargetCaps(
        frozenset({TargetWrite.BULK_BATCH}), atomic_swap=True, load=TargetLoad.BULK_STREAM
    )

    def __init__(
        self, broker: Any, *, schema: str, table: str, columns: list[tuple[str, str]]
    ) -> None:
        self._broker = broker
        self._schema = schema
        self._table = table
        self._columns = columns
        self._build = build_table_name(table)
        self._begun = False

    async def begin(self) -> None:
        from provisa.federation.replica_guard import require_replicas_schema

        require_replicas_schema(self._schema, self._table, action="build the replica at")
        self._broker.replica_begin(self._schema, self._table, self._build, self._columns)
        self._begun = True

    async def write(self, batch: pa.RecordBatch, rows: list[dict]) -> None:
        del rows  # the store reads the Arrow batch itself
        self._broker.replica_write(
            self._schema, self._build, [name for name, _ in self._columns], batch
        )

    async def swap(self) -> None:
        self._broker.replica_swap(self._schema, self._table, self._build)
        self._begun = False

    async def drop(self) -> None:
        """Remove the replica and any build table left beside it."""
        from provisa.federation.replica_guard import require_replicas_schema

        require_replicas_schema(self._schema, self._table, action="drop the replica at")
        self._broker.replica_abort(self._schema, self._build)
        self._broker.replica_abort(self._schema, self._table)

    async def abort(self) -> None:
        if self._begun:
            self._begun = False
            self._broker.replica_abort(self._schema, self._build)


#: Database engines on which ClickHouse exchanges two tables atomically.
_CLICKHOUSE_ATOMIC_ENGINES = frozenset({"Atomic", "Shared"})


class ClickHouseStoreTarget:
    """A replica in a ClickHouse store: Arrow batches inserted into a build table, which is then
    exchanged with the replica.

    ClickHouse has no multi-statement transaction; the swap is the single statement
    ``EXCHANGE TABLES``, atomic on a database of the ``Atomic`` or ``Shared`` engine, after
    which the build table (now holding the previous rows) is dropped. A replica built for the
    first time is renamed into place. The target declares an atomic swap only where the
    replicas database has such an engine, so a store that cannot swap is refused before a row
    is read. ``run`` runs a blocking store call with the runtime's one client held."""

    def __init__(
        self,
        backend: Any,
        run: Any,
        *,
        schema: str,
        table: str,
        columns: list[tuple[str, str]],
        pk_columns: list[str],
    ) -> None:
        from provisa.federation.clickhouse_store import _lit, ensure_namespace

        self._backend = backend
        self._run = run
        self._database = schema
        self._table = table
        self._columns = columns
        self._pk = tuple(pk_columns)
        self._build = build_table_name(table)
        self._begun = False
        ensure_namespace(backend, schema)
        rows, _ = backend.query(f"SELECT engine FROM system.databases WHERE name = {_lit(schema)}")
        self.database_engine = str(rows[0][0]) if rows else ""
        self.caps = TargetCaps(
            frozenset({TargetWrite.BULK_BATCH}),
            atomic_swap=self.database_engine in _CLICKHOUSE_ATOMIC_ENGINES,
            load=TargetLoad.BULK_STREAM,
        )

    def _table_engine(self, name: str) -> str | None:
        from provisa.federation.clickhouse_store import _lit

        rows, _ = self._backend.query(
            "SELECT engine FROM system.tables WHERE database = "
            f"{_lit(self._database)} AND name = {_lit(name)}"
        )
        return str(rows[0][0]) if rows else None

    def _begin(self) -> None:
        from provisa.federation.clickhouse_store import create_ddl, qualified
        from provisa.federation.replica_guard import ReplicaTargetError

        standing = self._table_engine(self._table)
        if standing is not None and not standing.endswith("MergeTree"):
            raise ReplicaTargetError(
                qualified((self._database, self._table)),
                f"{standing} relation",
                "build the replica at",
            )
        build = (self._database, self._build)
        self._backend.command(f"DROP TABLE IF EXISTS {qualified(build)}")
        self._backend.command(create_ddl(build, self._columns, self._pk))

    async def begin(self) -> None:
        from provisa.federation.replica_guard import require_replicas_schema

        require_replicas_schema(self._database, self._table, action="build the replica at")
        await self._run(self._begin)
        self._begun = True

    async def write(self, batch: pa.RecordBatch, rows: list[dict]) -> None:
        del rows  # the store reads the Arrow batch itself
        await self._run(
            lambda: self._backend.insert_arrow(self._database, self._build, self._columns, batch)
        )

    def _swap(self) -> None:
        from provisa.federation.clickhouse_store import qualified

        build = qualified((self._database, self._build))
        replica = qualified((self._database, self._table))
        if self._table_engine(self._table) is None:
            self._backend.command(f"RENAME TABLE {build} TO {replica}")
            return
        self._backend.command(f"EXCHANGE TABLES {build} AND {replica}")
        self._backend.command(f"DROP TABLE IF EXISTS {build}")  # the previous rows

    async def swap(self) -> None:
        await self._run(self._swap)
        self._begun = False

    async def drop(self) -> None:
        """Remove the replica and any build table left beside it."""
        from provisa.federation.clickhouse_store import qualified
        from provisa.federation.replica_guard import require_replicas_schema

        require_replicas_schema(self._database, self._table, action="drop the replica at")

        def _drop() -> None:
            for name in (self._build, self._table):
                self._backend.command(f"DROP TABLE IF EXISTS {qualified((self._database, name))}")

        await self._run(_drop)

    async def abort(self) -> None:
        from provisa.federation.clickhouse_store import qualified

        if self._begun:
            self._begun = False
            build = qualified((self._database, self._build))
            await self._run(lambda: self._backend.command(f"DROP TABLE IF EXISTS {build}"))


#: How a finished build replaces the replica, per SQLAlchemy dialect.
RENAME_IN_TRANSACTION = "rename_in_transaction"  # DDL is transactional: drop + rename, one commit
RENAME_PAIR = "rename_pair"  # ``RENAME TABLE a TO b, c TO a``: both names move in one statement
ROWS_IN_TRANSACTION = "rows_in_transaction"  # DELETE + INSERT ... SELECT, one commit

_SA_RENAME_IN_TRANSACTION = frozenset({"postgresql", "mssql"})
_SA_RENAME_PAIR = frozenset({"mysql", "mariadb"})
#: Dialects of stores with neither a transaction spanning two DML statements nor an atomic
#: rename reachable through SQLAlchemy: no method replaces a replica atomically there. Every
#: other dialect is a transactional database and takes ROWS_IN_TRANSACTION.
SA_NO_ATOMIC_REPLACE = frozenset(
    {"clickhouse", "hive", "trino", "presto", "awsathena", "impala", "druid", "databricks"}
)

#: How a batch is written into the build table, per (dialect, driver).
LOAD_INSERT = "insert"  # SQLAlchemy executemany: the floor every driver has
LOAD_ODBC_ARRAY = "odbc_array"  # pyodbc parameter arrays (``fast_executemany``) on the raw cursor
LOAD_ORACLE_DIRECT_PATH = "oracle_direct_path"  # python-oracledb Direct Path Load
LOAD_SINGLESTORE_INFILE = "singlestore_infile"  # streamed LOAD DATA LOCAL INFILE (REQ-990)

#: What each load method is, as the store declares it (``TargetCaps.load``).
_SA_LOAD_KIND = {
    LOAD_INSERT: TargetLoad.ROW_COPY,
    LOAD_ODBC_ARRAY: TargetLoad.ROW_COPY,
    LOAD_ORACLE_DIRECT_PATH: TargetLoad.BULK_STREAM,
    LOAD_SINGLESTORE_INFILE: TargetLoad.BULK_STREAM,
}

_SA_NATIVE_LOAD = {
    ("mssql", "pyodbc"): LOAD_ODBC_ARRAY,
    ("oracle", "oracledb"): LOAD_ORACLE_DIRECT_PATH,
}
#: Dialects with one client only, whose bulk call is keyed on the dialect alone: the singlestoredb
#: dialect names no driver until it connects, then names the wire protocol (``mysql``).
_SA_DIALECT_LOAD = {"singlestoredb": LOAD_SINGLESTORE_INFILE}


def sa_replace_method(dialect: str, *, rename: bool = True) -> str | None:
    """How a build replaces the replica on ``dialect``; None when the store has no atomic way.
    ``rename=False`` rules the rename methods out, leaving the one every transactional database
    has."""
    if dialect in SA_NO_ATOMIC_REPLACE:
        return None
    if rename and dialect in _SA_RENAME_IN_TRANSACTION:
        return RENAME_IN_TRANSACTION
    if rename and dialect in _SA_RENAME_PAIR:
        return RENAME_PAIR
    return ROWS_IN_TRANSACTION


def previous_table_name(table: str) -> str:
    """Where the replica ``table`` stands for the instant between a paired rename and the drop
    of its previous rows, in a store that swaps by renaming two tables at once."""
    return "prev__" + hashlib.sha256(table.encode()).hexdigest()[:24]


def sqlalchemy_store_target(
    sa_engine: Any,
    *,
    schema: str,
    table: str,
    columns: list[tuple[str, str]],
    pk_columns: list[str],
) -> Any:
    """The write face of a replica in the store a SQLAlchemy engine names. A PostgreSQL store
    has one write face whichever driver the URL names (:class:`PostgresStoreTarget`, the held
    COPY); every other store is written through its own driver's connection."""
    if sa_engine.dialect.name == "postgresql":
        return PostgresStoreTarget(
            sa_engine.url.render_as_string(hide_password=False),
            schema=schema,
            table=table,
            columns=columns,
            pk_columns=pk_columns,
        )
    return SqlAlchemyStoreTarget(
        sa_engine, schema=schema, table=table, columns=columns, pk_columns=pk_columns
    )


class SqlAlchemyStoreTarget:
    """A replica in a store reached through SQLAlchemy, written on the driver's own connection.

    One connection carries the build. The build table is filled a batch at a time, each batch
    committed (no reader addresses the build table), by the driver's bulk call where it has one:

    - SQL Server over pyodbc: parameter arrays on the raw cursor (``fast_executemany``);
    - Oracle over python-oracledb: Direct Path Load;
    - SingleStore: one streamed ``LOAD DATA LOCAL INFILE`` per batch (REQ-990), never executemany;
    - any other driver: SQLAlchemy's ``executemany``, the floor (PyMySQL sends it as multi-row
      ``INSERT`` statements; its ``LOAD DATA LOCAL`` reads a named file and is not used).

    The finished build then replaces the replica atomically:

    - PostgreSQL, SQL Server: the previous replica is dropped and the build table renamed onto
      its name in one transaction;
    - MySQL, MariaDB: one ``RENAME TABLE`` moves the previous replica aside and the build table
      onto its name, then the previous rows are dropped;
    - any other transactional database (Oracle and the rest): the replica's rows are deleted and
      the build table's inserted in ONE transaction, into the replica's own table (created, with
      its key, by the first build); the build table is dropped after the commit. A reader sees
      the whole previous rows or the whole new rows by the database's own isolation.

    A dialect in :data:`SA_NO_ATOMIC_REPLACE` has neither and declares no atomic swap, so no
    method builds a replica in it. A build that dies leaves the previous replica standing and
    its build table behind; the next build of the same replica drops that first.

    ``load`` and ``rename`` override the choice for a measurement or a proof: ``load=LOAD_INSERT``
    forces the floor, ``rename=False`` forces the rows-in-one-transaction replace."""

    def __init__(
        self,
        sa_engine: Any,
        *,
        schema: str,
        table: str,
        columns: list[tuple[str, str]],
        pk_columns: list[str],
        load: str | None = None,
        rename: bool = True,
    ) -> None:
        self._sa = sa_engine
        self._dialect = sa_engine.dialect.name
        self._schema = schema
        self._table = table
        self._columns = columns
        self._pk = tuple(pk_columns)
        self._build = build_table_name(table)
        self._previous = previous_table_name(table)
        self.load_method = (
            load
            or _SA_DIALECT_LOAD.get(self._dialect)
            or _SA_NATIVE_LOAD.get((self._dialect, sa_engine.dialect.driver), LOAD_INSERT)
        )
        self.replace_method = sa_replace_method(self._dialect, rename=rename)
        self._conn: Any = None
        self._build_table: Any = None
        self._json: frozenset[str] = frozenset()
        self._temporal: Any = None
        self._begun = False
        self.caps = TargetCaps(
            frozenset({TargetWrite.BULK_BATCH}),
            atomic_swap=self.replace_method is not None,
            load=_SA_LOAD_KIND[self.load_method],
        )

    def _core_table(self, name: str, *, keyed: bool) -> Any:
        import secrets

        from provisa.federation.materialize_exec import build_table

        table = build_table(
            self._schema,
            name,
            self._columns,
            self._pk if keyed else (),
            dialect_name=self._dialect,
        )
        if keyed and self._pk:
            # The key's constraint keeps its name through a rename, so each build names its
            # own: a store that scopes constraint names to the schema would refuse a second one.
            table.primary_key.name = f"pk__{self._build[7:]}_{secrets.token_hex(4)}"
        return table

    def _has_table(self, conn: Any, name: str) -> bool:
        """Whether ``name`` stands in the replicas schema, looked up by its exact case: the
        tables are created quoted, and Oracle's inspector folds an unquoted name to upper case
        and reports a table that is there as absent."""
        from sqlalchemy import inspect
        from sqlalchemy.sql.elements import quoted_name

        return inspect(conn).has_table(
            quoted_name(name, quote=True), schema=quoted_name(self._schema, quote=True)
        )

    def _drop_if_present(self, conn: Any, name: str) -> None:
        from sqlalchemy.schema import DropTable

        if self._has_table(conn, name):
            conn.execute(DropTable(self._core_table(name, keyed=False)))

    def _begin(self) -> None:
        from sqlalchemy.schema import CreateTable

        from provisa.federation.materialize_exec import _json_columns, temporal_columns
        from provisa.federation.sqlalchemy_runtime import _ensure_schema

        shield = request_deadline.shielded()
        with shield.lock:
            shield.settle()
            self._conn = self._sa.connect()
        conn = self._conn
        # Renamed onto the replica's name, the build table carries the key; replacing rows in
        # the replica's own table, the key is the replica's and the build table needs none.
        self._build_table = self._core_table(
            self._build, keyed=self.replace_method != ROWS_IN_TRANSACTION
        )
        self._json = _json_columns(self._build_table)
        self._temporal = temporal_columns(self._columns)
        _ensure_schema(conn, self._schema)
        self._drop_if_present(conn, self._build)
        if self.replace_method == RENAME_PAIR:
            # Left by a build that died between its rename and its drop: the replica is whole.
            self._drop_if_present(conn, self._previous)
        conn.execute(CreateTable(self._build_table))
        conn.commit()
        self._begun = True

    async def begin(self) -> None:
        from provisa.federation.replica_guard import require_replicas_schema

        require_replicas_schema(self._schema, self._table, action="build the replica at")
        await asyncio.to_thread(self._begin)

    def _write(self, rows: list[dict]) -> None:
        from provisa.federation.materialize_exec import _coerce_json_row, coerce_temporal_row

        coerced = [
            coerce_temporal_row(_coerce_json_row(row, self._json), self._temporal) for row in rows
        ]
        if self.load_method == LOAD_INSERT:
            self._conn.execute(self._build_table.insert(), coerced)
            self._conn.commit()
            return
        # The driver's own connection, the one this build's SQLAlchemy connection wraps.
        raw = self._conn.connection.driver_connection
        if self.load_method == LOAD_SINGLESTORE_INFILE:
            from provisa.core.database import singlestore_load_data

            singlestore_load_data(raw, self._sa.dialect, self._build_table, coerced)
            raw.commit()
            return
        names = [name for name, _ in self._columns]
        data = [tuple(_driver_value(row.get(name)) for name in names) for row in coerced]
        if self.load_method == LOAD_ODBC_ARRAY:
            quote = self._sa.dialect.identifier_preparer.quote_identifier
            cursor = raw.cursor()
            try:
                cursor.fast_executemany = True
                cursor.executemany(
                    f"INSERT INTO {quote(self._schema)}.{quote(self._build)} "
                    f"({', '.join(quote(name) for name in names)}) "
                    f"VALUES ({', '.join('?' * len(names))})",
                    data,
                )
            finally:
                cursor.close()
        elif self.load_method == LOAD_ORACLE_DIRECT_PATH:
            # The names are case-sensitive ones (created quoted); unquoted, the driver folds
            # them to upper case and the load addresses a table that does not exist (ORA-39826).
            raw.direct_path_load(
                f'"{self._schema}"',
                f'"{self._build}"',
                [f'"{name}"' for name in names],
                data,
            )
        else:
            raise ValueError(f"unknown load method {self.load_method!r}")
        raw.commit()

    async def write(self, batch: pa.RecordBatch, rows: list[dict]) -> None:
        del batch  # this face writes rows
        if rows:
            await asyncio.to_thread(self._write, rows)

    def _replace_rows(self, conn: Any, standing: bool) -> None:
        """Replace the replica's rows with the build table's in one transaction, in the
        replica's own table; the first build creates that table, with its key."""
        from sqlalchemy import select
        from sqlalchemy.schema import CreateTable

        replica = self._core_table(self._table, keyed=True)
        if not standing:
            conn.execute(CreateTable(replica))
            conn.commit()
        names = [name for name, _ in self._columns]
        conn.execute(replica.delete())
        conn.execute(
            replica.insert().from_select(
                names, select(*[self._build_table.c[name] for name in names])
            )
        )
        conn.commit()  # both statements, or neither
        self._drop_if_present(conn, self._build)

    def _swap(self) -> None:
        from sqlalchemy import text

        shield = request_deadline.shielded()
        conn = self._conn
        try:
            quote = self._sa.dialect.identifier_preparer.quote_identifier
            build = f"{quote(self._schema)}.{quote(self._build)}"
            replica = f"{quote(self._schema)}.{quote(self._table)}"
            standing = self._has_table(conn, self._table)
            if self.replace_method == ROWS_IN_TRANSACTION:
                self._replace_rows(conn, standing)
            elif self.replace_method == RENAME_PAIR:
                if standing:
                    previous = f"{quote(self._schema)}.{quote(self._previous)}"
                    conn.execute(
                        text(f"RENAME TABLE {replica} TO {previous}, {build} TO {replica}")
                    )
                    conn.execute(text(f"DROP TABLE {previous}"))
                else:
                    conn.execute(text(f"RENAME TABLE {build} TO {replica}"))
            elif self.replace_method == RENAME_IN_TRANSACTION:
                # One transaction: a reader sees the previous replica until the commit.
                if standing:
                    conn.execute(text(f"DROP TABLE {replica}"))
                if self._dialect == "mssql":
                    conn.execute(
                        text("EXEC sp_rename :build, :name"), {"build": build, "name": self._table}
                    )
                else:
                    conn.execute(text(f"ALTER TABLE {build} RENAME TO {quote(self._table)}"))
            else:
                raise ValueError(f"no atomic replace on a {self._dialect!r} store")
            conn.commit()
            self._begun = False
        finally:
            with shield.lock:
                shield.settle()
                self._conn = None
                conn.close()

    async def swap(self) -> None:
        await asyncio.to_thread(self._swap)

    def _abort(self) -> None:
        shield = request_deadline.shielded()
        conn, self._conn = self._conn, None
        try:
            if conn is not None:
                conn.rollback()
        finally:
            with shield.lock:
                shield.settle()
                if conn is not None:
                    conn.close()
        if self._begun:
            self._begun = False
            # On a connection of its own: the build's may be the thing that failed.
            with self._sa.begin() as fresh:
                self._drop_if_present(fresh, self._build)

    def _drop(self) -> None:
        with self._sa.begin() as conn:
            for name in (self._build, self._previous, self._table):
                self._drop_if_present(conn, name)

    async def drop(self) -> None:
        """Remove the replica and any build table left beside it."""
        from provisa.federation.replica_guard import require_replicas_schema

        require_replicas_schema(self._schema, self._table, action="drop the replica at")
        await asyncio.to_thread(self._drop)

    async def abort(self) -> None:
        await asyncio.to_thread(self._abort)


def _driver_value(value: Any) -> Any:
    """A row value as a driver's bulk call binds it: a JSON document as its text (the store
    column is the dialect's JSON or text type); everything else as it is."""
    import json

    return json.dumps(value) if isinstance(value, (dict, list)) else value


class _BuildAbandoned(Exception):
    """Raised into an open COPY to end it without committing its rows."""


def _pg_build_ddl(schema: str, build: str, columns: list[tuple[str, str]]) -> tuple[str, Any]:
    """The build table's CREATE statement (no key: it is added at the swap, on the replica's
    own name) and its SQLAlchemy table."""
    from sqlalchemy.dialects import postgresql
    from sqlalchemy.schema import CreateTable

    from provisa.federation.materialize_exec import build_table

    table = build_table(schema, build, columns)
    return str(CreateTable(table).compile(dialect=postgresql.dialect())), table


def _pg_swap(cur: Any, schema: str, table: str, build: str, pk_columns: list[str]) -> None:
    """Replace the replica with the build table, inside the caller's transaction."""
    replica = f"{_q(schema)}.{_q(table)}"
    cur.execute(f"DROP TABLE IF EXISTS {replica}")
    cur.execute(f"ALTER TABLE {_q(schema)}.{_q(build)} RENAME TO {_q(table)}")
    if pk_columns:
        key = ", ".join(_q(c) for c in pk_columns)
        cur.execute(f"ALTER TABLE {replica} ADD PRIMARY KEY ({key})")


def pg_statement_copy(
    cur: Any,
    *,
    source_relation: str,
    schema: str,
    table: str,
    columns: list[tuple[str, str]],
    pk_columns: list[str],
    prior_hash: str | None,
) -> tuple[int, str, bool]:
    """Build the replica ``schema.table`` from ``source_relation`` as PostgreSQL's own statements,
    in one transaction on ``cur``'s connection: fill a build table with ``INSERT ... SELECT``,
    hash its content, and swap it in — unless the hash equals ``prior_hash``, in which case the
    transaction is rolled back and the replica left as it was. No row leaves the server.

    ``source_relation`` is the already-quoted relation the engine reads the source through (its
    postgres_fdw foreign table). Returns ``(rows copied, content hash, changed)``.

    The content hash is computed by the server: each row's md5 over its text form, the two
    halves summed, with the row count. Summing does not depend on order. It is the same kind
    of digest as ``RowSetHash`` but not comparable with it — one hashes PostgreSQL's text form
    of a row, the other Provisa's — so it is prefixed and only ever compared with the hash of
    this replica's previous engine-side build."""
    from provisa.federation.replica_guard import require_pg_replica_table, require_replicas_schema

    require_replicas_schema(schema, table, action="build the replica at")
    build = build_table_name(table)
    ddl, _ = _pg_build_ddl(schema, build, columns)
    names = ", ".join(_q(name) for name, _ in columns)
    target = f"{_q(schema)}.{_q(build)}"
    cur.execute("BEGIN")
    try:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {_q(schema)}")
        require_pg_replica_table(cur, schema, table, action="build the replica at")
        cur.execute(f"DROP TABLE IF EXISTS {target}")
        cur.execute(ddl)
        cur.execute(f"INSERT INTO {target} ({names}) SELECT {names} FROM {source_relation}")
        copied = cur.rowcount
        cur.execute(
            "SELECT count(*), "
            "coalesce(sum(('x' || substr(h, 1, 16))::bit(64)::bigint::numeric), 0), "
            "coalesce(sum(('x' || substr(h, 17, 16))::bit(64)::bigint::numeric), 0) "
            f"FROM (SELECT md5(ROW({names})::text) AS h FROM {target}) hashed"
        )
        count, low, high = cur.fetchone()
        content_hash = f"pg:{count:x}:{low}:{high}"
        if content_hash == prior_hash:
            cur.execute("ROLLBACK")
            return copied, content_hash, False
        _pg_swap(cur, schema, table, build, pk_columns)
        cur.execute("COMMIT")
    except BaseException:
        cur.execute("ROLLBACK")
        raise
    return copied, content_hash, True


class PostgresStoreTarget:
    """A replica in a PostgreSQL store, written through one ``COPY ... FROM STDIN`` held open
    for the whole build.

    One connection and one transaction carry the build: the build table is created, every batch
    is written into the open COPY, and the swap drops the previous replica and renames the
    build table onto its name before the commit. PostgreSQL applies that atomically: a reader
    sees the previous replica until the commit, and if this process dies the server discards the
    transaction, build table included. The key is added after the load, on the replica's own
    name."""

    caps = TargetCaps(
        frozenset({TargetWrite.COPY_STREAM}), atomic_swap=True, load=TargetLoad.BULK_STREAM
    )

    def __init__(
        self,
        store_dsn: str,
        *,
        schema: str,
        table: str,
        columns: list[tuple[str, str]],
        pk_columns: list[str],
    ) -> None:
        self._dsn = _libpq_dsn(store_dsn)
        self._schema = schema
        self._table = table
        self._columns = columns
        self._pk = list(pk_columns)
        self._build = build_table_name(table)
        self._conn: Any = None
        self._cur: Any = None
        self._copy: Any = None
        self._copy_cm: Any = None
        self._json: frozenset[str] = frozenset()

    def _create_ddl(self) -> str:
        from provisa.federation.materialize_exec import _json_columns

        ddl, table = _pg_build_ddl(self._schema, self._build, self._columns)
        self._json = _json_columns(table)
        return ddl

    async def begin(self) -> None:
        import psycopg

        from provisa.core.request_context import current_org
        from provisa.federation.replica_guard import (
            require_pg_replica_table,
            require_replicas_schema,
        )
        from provisa.storage.quota import require_storage_headroom

        require_replicas_schema(self._schema, self._table, action="build the replica at")
        org_id = current_org.get()
        if (
            org_id is not None
        ):  # REQ-1047: the storage allowance, checked before anything is written
            await require_storage_headroom(
                org_id, operation=f"replicating {self._schema}.{self._table}"
            )
        ddl = self._create_ddl()
        shield = request_deadline.shielded()
        with shield.lock:
            shield.settle()
            self._conn = psycopg.connect(self._dsn)  # not autocommit: one transaction to the swap
        cur = self._cur = self._conn.cursor()
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {_q(self._schema)}")
        # Whatever stands at the replica's name must be nothing or an ordinary table: the swap
        # replaces it, and a view or a foreign table there would be a way into a source.
        require_pg_replica_table(cur, self._schema, self._table, action="build the replica at")
        cur.execute(f"DROP TABLE IF EXISTS {_q(self._schema)}.{_q(self._build)}")
        cur.execute(ddl)
        names = ", ".join(_q(name) for name, _ in self._columns)
        self._copy_cm = cur.copy(f"COPY {_q(self._schema)}.{_q(self._build)} ({names}) FROM STDIN")
        self._copy = self._copy_cm.__enter__()

    async def write(self, batch: pa.RecordBatch, rows: list[dict]) -> None:
        from psycopg.types.json import Json

        del batch  # this face writes rows
        names = [name for name, _ in self._columns]
        json_columns = self._json
        for row in rows:
            self._copy.write_row(
                [
                    Json(row.get(n))
                    if n in json_columns and isinstance(row.get(n), (dict, list))
                    else row.get(n)
                    for n in names
                ]
            )

    async def swap(self) -> None:
        shield = request_deadline.shielded()
        try:
            self._copy_cm.__exit__(None, None, None)  # ends the COPY; the server has every row
            self._copy = self._copy_cm = None
            _pg_swap(self._cur, self._schema, self._table, self._build, self._pk)
            self._conn.commit()
        finally:
            with shield.lock:
                shield.settle()
                self._close()

    async def drop(self) -> None:
        """Remove the replica and any build table left beside it, in one transaction."""
        import psycopg

        from provisa.federation.replica_guard import require_replicas_schema

        require_replicas_schema(self._schema, self._table, action="drop the replica at")
        shield = request_deadline.shielded()
        with shield.lock:
            shield.settle()
            conn = psycopg.connect(self._dsn)
        try:
            cur: Any = conn.cursor()
            for name in (self._build, self._table):
                cur.execute(f"DROP TABLE IF EXISTS {_q(self._schema)}.{_q(name)}")
            conn.commit()
        finally:
            with shield.lock:
                shield.settle()
                conn.close()

    async def abort(self) -> None:
        shield = request_deadline.shielded()
        with shield.lock:
            shield.settle()
            self._close()

    def _close(self) -> None:
        """End an unfinished COPY and close the connection. Anything not committed — the rows
        copied so far, the build table — is discarded by the server with the transaction."""
        import psycopg

        copy_cm, self._copy_cm, self._copy = self._copy_cm, None, None
        conn, self._conn, self._cur = self._conn, None, None
        if copy_cm is not None:
            try:
                # Leaving the COPY with an error tells the server to abandon it.
                copy_cm.__exit__(_BuildAbandoned, _BuildAbandoned(), None)
            except (_BuildAbandoned, psycopg.Error):
                # The COPY is being thrown away; a connection that is already gone has nothing
                # left to abandon. Closing it below is what discards the build.
                pass
        if conn is not None:
            conn.close()
