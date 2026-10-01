# Copyright (c) 2026 Kenneth Stott
# Canary: f9a3c5d1-7e40-4b62-9c18-2d6e0a8b4f73
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A replica is written only into an ordinary table of the store (REQ-826, REQ-1141, REQ-030).

Replicating a source means writing its rows — CREATE, DELETE, INSERT, DROP — into the engine's own
store. On an engine that also reaches the source live, the live reach is an object in that same
store: on Postgres a VIEW over a postgres_fdw FOREIGN TABLE, which Postgres will happily write
THROUGH. A replica write addressed to such an object is a write into the customer's source.

Every replica write therefore asks first what its target is, and proceeds only for an ordinary
table or no relation at all. Anything else raises :class:`ReplicaTargetError`, which names what is
there and, where it can be read off the catalog, which foreign server a write would have reached.
There is no path from a failed check to a write."""

# Requirements: REQ-826, REQ-1141, REQ-030

from __future__ import annotations

from typing import Any

# pg_class.relkind
_PG_TABLE = "r"
_PG_VIEW = "v"
_PG_KINDS = {
    "r": "table",
    "v": "view",
    "m": "materialized view",
    "f": "foreign table",
    "p": "partitioned table",
    "i": "index",
    "S": "sequence",
    "c": "composite type",
}

_PG_KIND_SQL = (
    "SELECT c.oid, c.relkind FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
    "WHERE n.nspname = {schema} AND c.relname = {table}"
)
# The foreign servers a view reads through (its rewrite rule depends on the foreign tables).
_PG_VIEW_REACH_SQL = (
    "SELECT DISTINCT d.refobjid::regclass::text, s.srvname, array_to_string(s.srvoptions, ', ') "
    "FROM pg_rewrite r "
    "JOIN pg_depend d ON d.objid = r.oid AND d.classid = 'pg_rewrite'::regclass "
    "JOIN pg_foreign_table ft ON ft.ftrelid = d.refobjid "
    "JOIN pg_foreign_server s ON s.oid = ft.ftserver "
    "WHERE r.ev_class = {p}"
)
_PG_FOREIGN_REACH_SQL = (
    "SELECT ft.ftrelid::regclass::text, s.srvname, array_to_string(s.srvoptions, ', ') "
    "FROM pg_foreign_table ft JOIN pg_foreign_server s ON s.oid = ft.ftserver "
    "WHERE ft.ftrelid = {p}"
)


class ReplicaTargetError(RuntimeError):
    """A replica write was addressed to a relation that is not an ordinary table of the store."""

    def __init__(self, relation: str, kind: str, action: str, reaches: str | None = None) -> None:
        self.relation = relation
        self.kind = kind
        self.action = action
        self.reaches = reaches
        through = f", which reads {reaches}" if reaches else ""
        super().__init__(
            f"refusing to {action} {relation}: a replica is written only into an ordinary table "
            f"of the store, and {relation} is a {kind}{through}. Writing it would write into the "
            "source, so nothing was written."
        )


def _reach(rows: list[tuple]) -> str | None:
    if not rows:
        return None
    return "; ".join(f"{rel} on foreign server {srv} ({opts})" for rel, srv, opts in rows)


def pg_kind_name(relkind: str) -> str:
    return _PG_KINDS.get(relkind, f"relation of kind {relkind!r}")


def pg_relation_kind(cur: Any, schema: str, table: str) -> tuple[int, str] | None:
    """``(oid, relkind)`` of ``schema.table`` on a psycopg2 cursor, or None when absent."""
    cur.execute(_PG_KIND_SQL.format(schema="%s", table="%s"), (schema, table))
    row = cur.fetchone()
    return None if row is None else (row[0], row[1])


def pg_reach(cur: Any, oid: int, relkind: str) -> str | None:
    """What a write to the relation ``oid`` would reach beyond the store, when it is a view over
    foreign tables or a foreign table itself."""
    if relkind == _PG_VIEW:
        cur.execute(_PG_VIEW_REACH_SQL.format(p="%s"), (oid,))
    elif relkind == "f":
        cur.execute(_PG_FOREIGN_REACH_SQL.format(p="%s"), (oid,))
    else:
        return None
    return _reach([tuple(r) for r in cur.fetchall()])


def require_pg_replica_table(cur: Any, schema: str, table: str, *, action: str) -> bool:
    """Whether ``schema.table`` exists, as the ordinary table a replica write needs. False when
    there is no such relation (the caller may create it); raises when it is anything else."""
    found = pg_relation_kind(cur, schema, table)
    if found is None:
        return False
    oid, relkind = found
    if relkind == _PG_TABLE:
        return True
    raise ReplicaTargetError(
        f'"{schema}"."{table}"', pg_kind_name(relkind), action, pg_reach(cur, oid, relkind)
    )


async def require_store_replica_table(conn: Any, schema: str, table: str, *, action: str) -> None:
    """The same check through the store write face (``core.database.Connection``). A Postgres
    store is the one where a relation can stand in front of another server; other store dialects
    address tables only."""
    if conn.capabilities.dialect != "postgresql":
        return
    row = await conn.fetchrow(_PG_KIND_SQL.format(schema="$1", table="$2"), schema, table)
    if row is None or row[1] == _PG_TABLE:
        return
    oid, relkind = row[0], row[1]
    sql = {_PG_VIEW: _PG_VIEW_REACH_SQL, "f": _PG_FOREIGN_REACH_SQL}.get(relkind)
    reach_rows = [] if sql is None else await conn.fetch(sql.format(p="$1"), oid)
    raise ReplicaTargetError(
        f'"{schema}"."{table}"',
        pg_kind_name(relkind),
        action,
        _reach([tuple(r) for r in reach_rows]),
    )


def require_duckdb_replica_table(
    con: Any, catalog: str, schema: str, table: str, *, action: str
) -> None:
    """The DuckDB store's check: a view at the replica's name is refused (a table or nothing is
    what a replica write expects)."""
    row = con.execute(
        "SELECT 1 FROM duckdb_views() WHERE database_name = ? AND schema_name = ? "
        "AND view_name = ? AND NOT internal",
        [catalog, schema, table],
    ).fetchone()
    if row is not None:
        raise ReplicaTargetError(f'"{catalog}"."{schema}"."{table}"', "view", action)


class ReplicaUnavailable(RuntimeError):
    """A read was refused because a replica of its source could not be reconciled."""

    def __init__(self, source_id: str, table_name: str, cause: BaseException) -> None:
        self.source_id = source_id
        self.table_name = table_name
        super().__init__(
            f"source {source_id!r} cannot be read: the replica of its table {table_name!r} could "
            f"not be reconciled ({type(cause).__name__}: {cause})"
        )
