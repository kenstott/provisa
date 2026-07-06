# Copyright (c) 2026 Kenneth Stott
# Canary: 9d41e0a7-3c58-4b62-a8f1-6e2b5d93c470
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-827: governance-parity conformance across federation engines — the SUITE.

The comparator + certification gate (provisa/federation/conformance.py) are unit-covered.
This is the missing piece: the runner that executes the SAME query on a real
candidate engine and diffs its rows against the reference golden set. It validates the
ABSTRACTION (any admitted engine yields byte-identical governed output), which per-engine
execution tests structurally cannot — they never compare engines against each other.

Only the transpile -> transport stages differ per engine (REQ-827): governance is baked into
the canonical PG-dialect SQL, then `transpile()` retargets it per engine. The suite asserts
that retarget+execute never changes which rows are visible or which cells are masked.

DuckDB runs in-process (always in CI). A Postgres candidate is added when the demo stack is
reachable, exercising real cross-engine parity; it is skipped otherwise, never silently passed.
"""

from __future__ import annotations

import os

import pytest

from provisa.federation.conformance import (
    ConformanceRegistry,
    UncertifiedEngineError,
    compare_governed_results,
)
from provisa.transpiler.transpile import transpile

pytestmark = pytest.mark.integration

duckdb = pytest.importorskip("duckdb")

# --- golden dataset + identity matrix (REQ-827) ------------------------------

# customers(id, first_name, state, email). The analyst identity is subject to RLS
# (state = 'NY') and column masking (email -> 'REDACTED').
_GOLDEN_ROWS = [
    (1, "Alice", "NY", "alice@example.com"),
    (2, "Bob", "CA", "bob@example.com"),
    (3, "Carol", "NY", "carol@example.com"),
]

# The governance-baked canonical query (PG dialect): RLS predicate + masked email.
# This is what the semantic layer emits before the per-engine transpile stage.
_GOVERNED_PG_SQL = (
    "SELECT id, first_name, state, 'REDACTED' AS email FROM customers WHERE state = 'NY'"
)

# The reference engine's governed result — analyst sees only NY rows, email masked.
_REFERENCE_GOVERNED = [
    (1, "Alice", "NY", "REDACTED"),
    (3, "Carol", "NY", "REDACTED"),
]


def _load_duckdb():
    con = duckdb.connect()
    con.execute(
        "CREATE TABLE customers (id INTEGER, first_name VARCHAR, state VARCHAR, email VARCHAR)"
    )
    con.executemany("INSERT INTO customers VALUES (?, ?, ?, ?)", _GOLDEN_ROWS)
    return con


# --- the parity suite --------------------------------------------------------


def test_duckdb_candidate_is_governance_certified_against_reference():
    """DuckDB's execution of the query diffs clean against the reference golden set."""
    con = _load_duckdb()
    ddl = transpile(_GOVERNED_PG_SQL, "duckdb")
    candidate = con.execute(ddl).fetchall()

    result = compare_governed_results(_REFERENCE_GOVERNED, candidate)

    assert result.certified, result.divergences


def test_parity_is_order_independent():
    """NULL/collation ordering is an allowed semantic edge — the diff is a multiset compare."""
    con = _load_duckdb()
    ddl = transpile(_GOVERNED_PG_SQL + " ORDER BY id DESC", "duckdb")
    candidate = con.execute(ddl).fetchall()

    # Reversed row order must still certify.
    assert compare_governed_results(_REFERENCE_GOVERNED, candidate).certified


def test_suite_catches_an_rls_leak_on_a_noncompliant_engine():
    """An engine that drops the RLS predicate leaks a row — the suite MUST fail it (no false certify)."""
    con = _load_duckdb()
    leaky_sql = "SELECT id, first_name, state, 'REDACTED' AS email FROM customers"  # no WHERE
    candidate = con.execute(transpile(leaky_sql, "duckdb")).fetchall()

    result = compare_governed_results(_REFERENCE_GOVERNED, candidate)

    assert not result.certified
    leaked = [d.row for d in result.divergences if d.kind == "only_in_candidate"]
    assert (2, "Bob", "CA", "REDACTED") in leaked  # the CA row that RLS should have hidden


def test_suite_catches_a_masking_divergence():
    """An engine that fails to mask a cell diverges — visibility/masking is EXACT."""
    con = _load_duckdb()
    unmasked_sql = "SELECT id, first_name, state, email FROM customers WHERE state = 'NY'"
    candidate = con.execute(transpile(unmasked_sql, "duckdb")).fetchall()

    result = compare_governed_results(_REFERENCE_GOVERNED, candidate)

    assert not result.certified  # unmasked emails differ from 'REDACTED' -> divergence


def test_certification_gate_blocks_uncertified_engine():
    """A candidate is selectable only after it passes the suite (REQ-827 hard gate)."""
    con = _load_duckdb()
    reg = ConformanceRegistry(reference="trino")

    # Snowflake never ran the suite -> not selectable.
    with pytest.raises(UncertifiedEngineError):
        reg.require_certified("snowflake")

    # DuckDB passes the suite -> certify -> now selectable.
    candidate = con.execute(transpile(_GOVERNED_PG_SQL, "duckdb")).fetchall()
    assert compare_governed_results(_REFERENCE_GOVERNED, candidate).certified
    reg.certify("duckdb")
    reg.require_certified("duckdb")  # does not raise


def _pg_dsn() -> str | None:
    if os.environ.get("PROVISA_PG_STACK") != "1":
        return None
    u = os.environ.get("PG_USER", "provisa")
    pw = os.environ.get("PG_PASSWORD", "provisa")
    h = os.environ.get("PG_HOST", "localhost")
    p = os.environ.get("PG_PORT", "5432")
    db = os.environ.get("PG_DATABASE", "provisa")
    return f"postgresql://{u}:{pw}@{h}:{p}/{db}"


@pytest.mark.asyncio
async def test_postgres_candidate_matches_duckdb_candidate_cross_engine():
    """Real cross-engine parity: Postgres and DuckDB both certify against the same golden set.

    Skipped (not passed) unless the demo Postgres stack is opted in via PROVISA_PG_STACK=1 —
    a silent pass would misreport cross-engine coverage.
    """
    dsn = _pg_dsn()
    if dsn is None:
        pytest.skip("PROVISA_PG_STACK != 1 — cross-engine PG parity leg not exercised")
    asyncpg = pytest.importorskip("asyncpg")

    conn = await asyncpg.connect(dsn=dsn)
    try:
        await conn.execute("CREATE SCHEMA IF NOT EXISTS parity_e2e")
        await conn.execute(
            "CREATE TABLE parity_e2e.customers (id int, first_name text, state text, email text)"
        )
        await conn.executemany(
            "INSERT INTO parity_e2e.customers VALUES ($1, $2, $3, $4)", _GOLDEN_ROWS
        )
        pg_sql = _GOVERNED_PG_SQL.replace("FROM customers", "FROM parity_e2e.customers")
        pg_rows = [tuple(r) for r in await conn.fetch(transpile(pg_sql, "postgres"))]
    finally:
        await conn.execute("DROP SCHEMA IF EXISTS parity_e2e CASCADE")
        await conn.close()

    con = _load_duckdb()
    duck_rows = con.execute(transpile(_GOVERNED_PG_SQL, "duckdb")).fetchall()

    # Both engines certify against the reference, hence against each other.
    assert compare_governed_results(_REFERENCE_GOVERNED, pg_rows).certified
    assert compare_governed_results(_REFERENCE_GOVERNED, duck_rows).certified
    assert compare_governed_results(pg_rows, duck_rows).certified


if __name__ == "__main__":
    pytest.main([__file__, "-q"])
