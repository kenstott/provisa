# Copyright (c) 2026 Kenneth Stott
# Canary: 53dd4b8c-7ddb-4661-99da-ebcd1234095a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E (REQ-1999): MotherDuck as the engine's store. The DuckDB engine attaches a MotherDuck
database as ``mat_store`` on its own connection, lands a replica into it with the embedded store's
statements, and a statement reads it there in place.

Writes to the account: each run works in a scratch database of its own,
``provisa_it_<UTC stamp>_<hex>``, dropped when it ends; one left by a run that died is dropped by
the next run once it is a day old. Needs MOTHERDUCK_API_TOKEN (tests/env_creds loads it locally;
the warehouse lane's secret in CI)."""

# Requirements: REQ-1999

from __future__ import annotations

import os
import re
import uuid
from datetime import UTC, datetime, timedelta

import duckdb
import pytest

pytestmark = [pytest.mark.integration, pytest.mark.requires_warehouse]

_SCRATCH = re.compile(r"^provisa_it_(\d{14})_[0-9a-f]{8}$")
_STAMP = "%Y%m%d%H%M%S"


def _account(token: str) -> duckdb.DuckDBPyConnection:
    """A connection signed in to the account with every database attached (``md:``)."""
    from provisa.federation.duckdb_extensions import load_extension

    con = duckdb.connect()
    load_extension(con, "motherduck")
    con.execute("SET GLOBAL motherduck_token = ?", [token])
    con.execute("ATTACH 'md:'")
    return con


def _sweep(con: duckdb.DuckDBPyConnection, now: datetime) -> None:
    """Drop the scratch databases runs that died left, once they are a day old."""
    for (name,) in con.execute("SELECT database_name FROM duckdb_databases()").fetchall():
        match = _SCRATCH.match(name)
        if match is None:
            continue
        made = datetime.strptime(match.group(1), _STAMP).replace(tzinfo=UTC)
        if now - made > timedelta(days=1):
            con.execute(f'DROP DATABASE "{name}"')


@pytest.fixture
def scratch():
    token = os.environ["MOTHERDUCK_API_TOKEN"]
    now = datetime.now(UTC)
    name = f"provisa_it_{now.strftime(_STAMP)}_{uuid.uuid4().hex[:8]}"
    con = _account(token)
    try:
        _sweep(con, now)
        con.execute(f'CREATE DATABASE "{name}"')
        yield name, token
    finally:
        con.execute(f'DROP DATABASE IF EXISTS "{name}"')
        con.close()


def test_a_replica_lands_in_the_motherduck_store_and_is_read_there_in_place(scratch):
    from provisa.federation.duckdb_runtime import DuckDBFederationRuntime

    name, token = scratch
    runtime = DuckDBFederationRuntime(
        materialize_dsn=f"motherduck:///{name}?motherduck_token={token}"
    )
    try:
        assert runtime.ensure_materialize_attached() == "mat_store"
        broker = runtime._store_broker  # noqa: SLF001
        columns = [("id", "INTEGER"), ("region", "VARCHAR")]
        broker.reconcile("replicas", "orders", columns)
        broker.land(
            "replicas",
            "orders",
            columns,
            [{"id": 1, "region": "north"}, {"id": 2, "region": "south"}],
            "none",
            None,
        )
        sql = 'SELECT id, region FROM mat_store."replicas"."orders" ORDER BY id'
        rows = list(runtime.run_sync(sql).rows)
        assert rows == [(1, "north"), (2, "south")]
        # In place: the statement names the store, no per-statement copy of it is made.
        assert runtime._refresh_store_relations(sql) == sql  # noqa: SLF001
    finally:
        runtime.close()
    # And it is MotherDuck's: another connection to the account reads what the engine landed.
    con = _account(token)
    try:
        assert con.execute(f'SELECT count(*) FROM "{name}"."replicas"."orders"').fetchone() == (2,)
    finally:
        con.close()
