# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""TRUE end-to-end: DuckDB `ATTACH ... TYPE postgres` → Provisa pgwire → a REAL source (REQ-883).

Companion to ``test_duckdb_attach_pgwire.py``, which stubs the data-execution seams
(``plan_pgwire_sql`` / ``_execute_plan`` / ``execute_pgwire_sql``) because a live RDBMS
is not guaranteed in that tier. THIS file stubs NONE of them: the rows DuckDB reads are
fetched from a REAL PostgreSQL database through the actual Provisa pipeline —

    DuckDB `postgres` extension
      → libpq/COPY to ProvisaServer
        → plan_pgwire_sql  (real govern + route → Route.DIRECT, dialect postgres)
          → _execute_plan → federation_engine.execute_native
            → execute_direct → asyncpg → SELECT on the real Postgres table
        → PG binary/text-COPY encode of the fetched rows
      → DuckDB COPY decode → returned to the DuckDB client

A postgres source is a DIRECT rdbms (unlike sqlite, which router.VIRTUAL_SOURCES forces
through the federation engine), so the scan runs on the real source driver — no stub.
A wrong route, a governance drop, a transpile error, an OID/type mismatch, or a
COPY-encode bug all surface as a failed assertion on the DuckDB-side rows.

The live-Postgres pgwire server is the shared ``pgwire_pg_backend`` fixture (conftest).
"""

from __future__ import annotations

import pytest

pytestmark = [pytest.mark.integration]

duckdb = pytest.importorskip("duckdb", reason="duckdb required for ATTACH e2e")


def _attach(port: int):
    con = duckdb.connect()
    con.execute("INSTALL postgres")
    con.execute("LOAD postgres")
    dsn = f"host=127.0.0.1 port={port} dbname=provisa user=admin password=x"
    con.execute(f"ATTACH '{dsn}' AS prov (TYPE postgres, READ_ONLY)")
    return con


def test_attach_and_discover_table(pgwire_pg_backend):
    be = pgwire_pg_backend
    con = _attach(be["port"])
    tables = con.execute(
        "SELECT table_name FROM information_schema.tables "
        f"WHERE table_catalog = 'prov' AND table_name = '{be['table']}'"
    ).fetchall()
    con.close()
    assert (be["table"],) in tables


def test_scan_returns_real_postgres_rows(pgwire_pg_backend):
    """DuckDB reads the real rows out of Postgres through the whole un-stubbed pipeline."""
    be = pgwire_pg_backend
    con = _attach(be["port"])
    n = con.execute(f"SELECT count(*) FROM prov.{be['schema']}.{be['table']}").fetchone()[0]
    total = con.execute(f"SELECT sum(amount) FROM prov.{be['schema']}.{be['table']}").fetchone()[0]
    con.close()
    assert n == len(
        be["rows"]
    )  # every real row survived govern → route → transpile → COPY → decode
    assert total == pytest.approx(sum(r[1] for r in be["rows"]))


def test_scan_row_values_match_postgres(pgwire_pg_backend):
    """Exact per-row values must round-trip int/double/varchar (+NULL) from Postgres to DuckDB."""
    be = pgwire_pg_backend
    con = _attach(be["port"])
    rows = con.execute(
        f"SELECT id, amount, region FROM prov.{be['schema']}.{be['table']} ORDER BY id"
    ).fetchall()
    con.close()
    assert rows == be["rows"]  # the NULL in the last row must decode as None, not ""
