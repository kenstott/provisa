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

import hashlib
import logging
from typing import Any, Protocol

import pyarrow as pa

from provisa.core import request_deadline
from provisa.federation.data_replicator import TargetCaps, TargetWrite

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

    caps = TargetCaps(frozenset({TargetWrite.BULK_BATCH}), atomic_swap=True)

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

    async def abort(self) -> None:
        from provisa.federation.clickhouse_store import qualified

        if self._begun:
            self._begun = False
            build = qualified((self._database, self._build))
            await self._run(lambda: self._backend.command(f"DROP TABLE IF EXISTS {build}"))


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

    caps = TargetCaps(frozenset({TargetWrite.COPY_STREAM}), atomic_swap=True)

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
