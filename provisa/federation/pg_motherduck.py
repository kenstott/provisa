# Copyright (c) 2026 Kenneth Stott
# Canary: d882f1ab-cd1f-4db2-9bfa-2dba3fd24a87
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""A MotherDuck table exposed in a Postgres engine by pg_duckdb (REQ-1868, issue 254).

pg_duckdb does not import MotherDuck tables as foreign tables. A server of ``TYPE 'motherduck'``
and a user mapping holding the token start its background sync worker, which creates each
MotherDuck table as a ``duckdb`` table in a Postgres schema of its own naming, some seconds later:

* the server's default database: its ``main`` schema is ``public``, any other schema keeps its
  name;
* any other database: its ``main`` is ``ddb$<db>``, any other schema is ``ddb$<db>$<schema>``.

The sync is not done when a table first appears: a restart of the worker re-syncs, recreating
tables under new OIDs (seen live: a resync some 6 s after the first, and a read failing "could not
open relation with OID"). A database is read once its synced tables -- their OIDs and MotherDuck
catalog versions in pg_duckdb's ``duckdb.tables`` -- have held still for ``SETTLE_S``, checked
once per database per engine runtime.

pg_duckdb holds one MotherDuck server per Postgres instance (a second fails "MotherDuck FDW
already exists"), so the server is the engine's, whatever it is named, and the engine is signed in
to one MotherDuck account: a source holding another account's token is refused by name.
"""

# Requirements: REQ-1868

from __future__ import annotations

import time
from collections.abc import Mapping
from typing import Any

from provisa.federation.pg_quoting import pg_ident

SERVER = "motherduck"  # the name pg_duckdb's documentation uses, when the engine has none yet
_POLL_S = 0.5
# How long a database's synced tables must hold still to count as settled: longer than the ~6 s
# between pg_duckdb's first sync and the re-sync of a restarted worker, seen live (issue 254).
SETTLE_S = 10.0


class MotherDuckAttachError(RuntimeError):
    """A MotherDuck table the Postgres engine cannot expose; the message says why."""


def synced_schema(database: str, schema: str, default_database: str) -> str:
    """The Postgres schema pg_duckdb's sync creates for MotherDuck ``database.schema``, the
    server's default database being ``default_database``."""
    if database.casefold() == default_database.casefold():
        return "public" if schema == "main" else schema
    return f"ddb${database}" if schema == "main" else f"ddb${database}${schema}"


def _server(cur: Any, options: Mapping[str, str]) -> str:
    """The engine's MotherDuck server, created under ``SERVER`` when it has none."""
    cur.execute("SELECT srvname FROM pg_foreign_server WHERE srvtype = 'motherduck'")
    row = cur.fetchone()
    if row is not None:
        return row[0]
    clause = ", ".join(f"{k} %s" for k in options)
    cur.execute(
        f"CREATE SERVER {pg_ident(SERVER)} TYPE 'motherduck' FOREIGN DATA WRAPPER duckdb"
        + (f" OPTIONS ({clause})" if clause else ""),
        tuple(options.values()),
    )
    return SERVER


def _signed_in(cur: Any, server: str, token: str, source_id: str) -> None:
    """The current user's mapping on ``server`` holds ``token``: created when there is none, and
    a mapping holding another token (another MotherDuck account) refuses the source."""
    cur.execute(
        "SELECT umoptions FROM pg_user_mappings WHERE srvname = %s AND usename = current_user",
        (server,),
    )
    row = cur.fetchone()
    if row is None:
        cur.execute(
            f"CREATE USER MAPPING FOR CURRENT_USER SERVER {pg_ident(server)} OPTIONS (token %s)",
            (token,),
        )
        return
    held = dict(o.split("=", 1) for o in row[0] or [])
    if held.get("token") != token:
        raise MotherDuckAttachError(
            f"Source {source_id!r}: the Postgres engine is signed in to another MotherDuck "
            "account; pg_duckdb holds one MotherDuck account per Postgres instance"
        )


def _default_database(cur: Any) -> str:
    cur.execute("SELECT r['db']::text FROM duckdb.query('SELECT current_database() AS db') r")
    row = cur.fetchone()
    assert row is not None  # one row: the MotherDuck connection's current database
    return row[0]


def _located(cur: Any, schema: str, table: str) -> str | None:
    """``"schema"."table"`` as pg_duckdb's sync created it (its schema matched without regard
    to case: MotherDuck names are case-insensitive), or None while it is not there."""
    cur.execute(
        "SELECT n.nspname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
        "WHERE lower(n.nspname) = lower(%s) AND c.relname = %s",
        (schema, table),
    )
    row = cur.fetchone()
    return None if row is None else pg_ident(row[0], table)


def _synced(cur: Any, database: str) -> frozenset[tuple[int, str | None]]:
    """The synced tables of MotherDuck ``database`` as pg_duckdb records them: each one's OID and
    MotherDuck catalog version."""
    cur.execute(
        "SELECT relid::oid::bigint, motherduck_catalog_version FROM duckdb.tables "
        "WHERE lower(duckdb_db) = lower(%s)",
        (database,),
    )
    return frozenset((int(r[0]), r[1]) for r in cur.fetchall())


def attach(
    cur: Any,
    motherduck: Mapping[str, Any],
    source_id: str,
    schema: str,
    table: str,
    wait_s: float,
    settled: set[str],
) -> str:
    """Sign the engine in (once), then wait up to ``wait_s`` seconds for pg_duckdb's sync to
    create ``schema.table`` of the source's database and, the first time this runtime reads the
    database (``settled`` holds the ones already settled), for the database's synced tables to
    hold still for ``SETTLE_S``; the relation to read, or :class:`MotherDuckAttachError` naming
    the table and what it waited for."""
    server = _server(cur, motherduck["server_options"])
    _signed_in(cur, server, motherduck["token"], source_id)
    database = motherduck["database"]
    expected = synced_schema(database, schema, _default_database(cur))
    deadline = time.monotonic() + wait_s
    seen: frozenset[tuple[int, str | None]] | None = None
    still_since = time.monotonic()
    while True:
        found = _located(cur, expected, table)
        if found is not None and database.casefold() in settled:
            return found
        if found is not None:
            now = _synced(cur, database)
            if now != seen:
                seen, still_since = now, time.monotonic()
            elif time.monotonic() - still_since >= SETTLE_S:
                settled.add(database.casefold())
                return found
        if time.monotonic() >= deadline:
            what = (
                f"did not appear in Postgres schema {expected!r}"
                if found is None
                else f"appeared in {expected!r} but pg_duckdb's sync did not settle"
            )
            raise MotherDuckAttachError(
                f"Source {source_id!r}: MotherDuck table {database}.{schema}.{table} {what} "
                f"within {wait_s:.0f}s"
            )
        time.sleep(_POLL_S)
