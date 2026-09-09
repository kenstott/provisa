# Copyright (c) 2026 Kenneth Stott
# Canary: d151d247-351d-4eaf-83ef-350ee7ce02bb
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Cassandra over CQL, engine-independently (REQ-1676).

The Trino connector was the only reader of a Cassandra source. This module is the native reader
(``cassandra-driver``, the ``cassandra`` extra): keyspaces, tables and typed columns from the
cluster's schema metadata for Register Table, and a paged ``SELECT`` of a table for the landing
path. A keyspace is the schema, a table is the table. Synchronous — callers run it in a thread.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Iterator

from provisa.cassandra.source import discover_schema

_SYSTEM_KEYSPACES = frozenset(
    {
        "system",
        "system_auth",
        "system_distributed",
        "system_schema",
        "system_traces",
        "system_views",
        "system_virtual_schema",
    }
)
_PAGE_SIZE = 1000


@dataclass(frozen=True)
class CassandraConnection:  # REQ-1676
    contact_points: tuple[str, ...]
    port: int = 9042
    username: str | None = None
    password: str | None = None

    @classmethod
    def build(
        cls, host: str, port: int, *, username: str | None = None, password: str | None = None
    ) -> "CassandraConnection":
        # The source's ``host`` may list several contact points, comma-separated.
        points = tuple(p.strip() for p in host.split(",") if p.strip()) or ("localhost",)
        return cls(
            contact_points=points,
            port=port,
            username=username or None,
            password=password if (username and password) else None,
        )

    @contextmanager
    def cluster(self) -> Iterator[Any]:
        from cassandra.auth import PlainTextAuthProvider
        from cassandra.cluster import Cluster

        auth = (
            PlainTextAuthProvider(username=self.username, password=self.password)
            if self.username and self.password
            else None
        )
        cluster = Cluster(
            contact_points=list(self.contact_points),
            port=self.port,
            auth_provider=auth,
            connect_timeout=30,
        )
        try:
            yield cluster
        finally:
            cluster.shutdown()


def list_keyspaces(conn: CassandraConnection) -> list[str]:  # REQ-1676
    """User keyspaces, sorted; the system keyspaces are not schemas a steward registers from."""
    with conn.cluster() as cluster:
        cluster.connect()
        names = [k for k in cluster.metadata.keyspaces if k not in _SYSTEM_KEYSPACES]
    return sorted(names)


def list_tables(conn: CassandraConnection, keyspace: str) -> list[str]:  # REQ-1676
    with conn.cluster() as cluster:
        cluster.connect()
        ks = cluster.metadata.keyspaces.get(keyspace)
        if ks is None:
            raise ValueError(f"Cassandra keyspace {keyspace!r} does not exist")
        return sorted(ks.tables)


def table_metadata(conn: CassandraConnection, keyspace: str, table: str) -> dict:  # REQ-1676
    """The ``{columns, partition_keys, clustering_keys}`` shape ``discover_schema`` types."""
    with conn.cluster() as cluster:
        cluster.connect()
        ks = cluster.metadata.keyspaces.get(keyspace)
        tbl = ks.tables.get(table) if ks is not None else None
        if tbl is None:
            raise ValueError(f"Cassandra table {keyspace}.{table} does not exist")
        return {
            "columns": [{"name": name, "type": col.cql_type} for name, col in tbl.columns.items()],
            "partition_keys": [c.name for c in tbl.partition_key],
            "clustering_keys": [c.name for c in tbl.clustering_key],
        }


def table_columns(conn: CassandraConnection, keyspace: str, table: str) -> list[dict]:  # REQ-1676
    """``discover_schema`` over the live table metadata: ``{name, type, isPartitionKey, …}``."""
    return discover_schema(table_metadata(conn, keyspace, table))


def fetch_rows(
    conn: CassandraConnection, keyspace: str, table: str, columns: list[str]
) -> list[dict]:  # REQ-1676
    """Every row of the table, ``columns`` projected, read page by page."""
    from cassandra.query import SimpleStatement

    cols = ", ".join(f'"{c}"' for c in columns)
    stmt = SimpleStatement(f'SELECT {cols} FROM "{keyspace}"."{table}"', fetch_size=_PAGE_SIZE)
    rows: list[dict] = []
    with conn.cluster() as cluster:
        session = cluster.connect()
        for row in session.execute(stmt):
            rows.append({c: getattr(row, c) for c in columns})
    return rows
