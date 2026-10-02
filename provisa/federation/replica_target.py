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


class _BuildAbandoned(Exception):
    """Raised into an open COPY to end it without committing its rows."""


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
        from sqlalchemy.dialects import postgresql
        from sqlalchemy.schema import CreateTable

        from provisa.federation.materialize_exec import _json_columns, build_table

        table = build_table(self._schema, self._build, self._columns)  # the key comes at the swap
        self._json = _json_columns(table)
        return str(CreateTable(table).compile(dialect=postgresql.dialect()))

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
            cur = self._cur
            replica = f"{_q(self._schema)}.{_q(self._table)}"
            cur.execute(f"DROP TABLE IF EXISTS {replica}")
            cur.execute(
                f"ALTER TABLE {_q(self._schema)}.{_q(self._build)} RENAME TO {_q(self._table)}"
            )
            if self._pk:
                key = ", ".join(_q(c) for c in self._pk)
                cur.execute(f"ALTER TABLE {replica} ADD PRIMARY KEY ({key})")
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
