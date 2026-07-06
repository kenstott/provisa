# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""E2E: DuckDB as the federation engine through the REAL query pipeline (REQ-825, REQ-840).

These tests do NOT bypass the product. Each drives the actual Provisa primitives —
generate_schema/build_context/compile_query, apply_governance (RLS), rewrite_semantic_to_physical,
transpile(..., "duckdb") — and then executes the resulting governed DuckDB SQL on a real DuckDB
connection against the real demo files (demo/files/*), asserting on the returned rows. If governance
were not applied, or DuckDB could not federate the heterogeneous sources, these fail.

The DuckDB objects are created at the physical schema.table names the compiler emits (DuckDB's
default schema is ``main``), which is exactly what a DuckDB-aware physical rewrite would produce.
"""

from __future__ import annotations

from pathlib import Path

import pytest

pytestmark = [pytest.mark.integration]

duckdb = pytest.importorskip("duckdb", reason="duckdb required for the DuckDB federation engine")

from provisa.compiler.introspect import ColumnMetadata  # noqa: E402
from provisa.compiler.parser import parse_query  # noqa: E402
from provisa.compiler.rls import RLSContext  # noqa: E402
from provisa.compiler.schema_gen import SchemaInput, generate_schema  # noqa: E402
from provisa.compiler.sql_gen import (  # noqa: E402
    build_context,
    compile_query,
    rewrite_semantic_to_physical,
)
from provisa.compiler.stage2 import apply_governance, build_governance_context  # noqa: E402
from provisa.transpiler.transpile import transpile  # noqa: E402

_FILES = Path(__file__).parent.parent.parent / "demo" / "files"
_CSV = str(_FILES / "customers.csv")
_ORDERS_SQLITE = str(_FILES / "orders.sqlite")

_ADMIN = {"id": "admin", "capabilities": ["admin"], "domain_access": ["*"]}


def _col(n: str, d: str = "varchar", nl: bool = True) -> ColumnMetadata:
    return ColumnMetadata(column_name=n, data_type=d, is_nullable=nl)


def _compile_to_duckdb(si: SchemaInput, gql: str, rls: RLSContext | None = None) -> str:
    """Run the real pipeline: compile -> (govern) -> physical -> transpile(duckdb). Returns SQL."""
    schema = generate_schema(si)
    ctx = build_context(si)
    compiled = compile_query(parse_query(schema, gql, {}), ctx)[0]
    sql = compiled.sql
    if rls is not None:
        gov_ctx = build_governance_context("admin", rls, {}, ctx, si.tables, role=_ADMIN)
        sql = apply_governance(sql, gov_ctx)
    return transpile(rewrite_semantic_to_physical(sql, ctx), "duckdb")


def test_duckdb_query_applies_rls():
    """An RLS rule filters real CSV rows when executed on DuckDB (governance is not bypassed)."""
    gt = duckdb.connect()
    by_state = gt.execute(
        f"SELECT state, count(*) FROM read_csv_auto('{_CSV}') GROUP BY state ORDER BY 2 DESC"
    ).fetchall()
    total = gt.execute(f"SELECT count(*) FROM read_csv_auto('{_CSV}')").fetchone()[0]
    target, expected = by_state[0]
    assert expected < total  # the RLS rule must actually remove rows for the test to be meaningful

    si = SchemaInput(
        tables=[
            {
                "id": 1,
                "source_id": "cust",
                "domain_id": "sales",
                "schema_name": "main",
                "table_name": "customers",
                "governance": "pre-approved",
                "columns": [
                    {"column_name": c, "visible_to": ["admin"]}
                    for c in ("id", "first_name", "state")
                ],
            }
        ],
        relationships=[],
        column_types={1: [_col("id", "integer", False), _col("first_name"), _col("state")]},
        naming_rules=[],
        role=_ADMIN,
        domains=[{"id": "sales", "description": "Sales"}],
        source_types={"cust": "csv"},
    )
    rls = RLSContext(rules={1: f"state = '{target}'"}, domain_rules={})
    sql = _compile_to_duckdb(si, "{ customers { id firstName state } }", rls=rls)
    assert "WHERE" in sql.upper()  # the RLS predicate made it into the governed SQL

    con = duckdb.connect()
    con.execute(f"CREATE VIEW main.customers AS SELECT * FROM read_csv_auto('{_CSV}')")
    rows = con.execute(sql).fetchall()

    assert len(rows) == expected  # RLS reduced 15 rows to only the target state
    assert all(r[2] == target for r in rows)


def test_duckdb_federates_csv_and_sqlite():
    """A relationship-join compiles and executes on DuckDB across a CSV and a SQLite source."""
    si = SchemaInput(
        tables=[
            {
                "id": 1,
                "source_id": "cust",
                "domain_id": "sales",
                "schema_name": "main",
                "table_name": "customers",
                "governance": "pre-approved",
                "columns": [
                    {"column_name": c, "visible_to": ["admin"]}
                    for c in ("id", "first_name", "state")
                ],
            },
            {
                "id": 2,
                "source_id": "ord",
                "domain_id": "sales",
                "schema_name": "main",
                "table_name": "orders",
                "governance": "pre-approved",
                "columns": [
                    {"column_name": c, "visible_to": ["admin"]}
                    for c in ("id", "customer_id", "amount")
                ],
            },
        ],
        relationships=[
            {
                "id": "ord2cust",
                "source_table_id": 2,
                "target_table_id": 1,
                "source_column": "customer_id",
                "target_column": "id",
                "cardinality": "many-to-one",
            }
        ],
        column_types={
            1: [_col("id", "integer", False), _col("first_name"), _col("state")],
            2: [
                _col("id", "integer", False),
                _col("customer_id", "integer"),
                _col("amount", "double"),
            ],
        },
        naming_rules=[],
        role=_ADMIN,
        domains=[{"id": "sales", "description": "Sales"}],
        source_types={"cust": "csv", "ord": "sqlite"},
    )
    sql = _compile_to_duckdb(si, "{ orders { id amount customer { firstName state } } }")

    con = duckdb.connect()
    con.execute("INSTALL sqlite")
    con.execute("LOAD sqlite")
    con.execute(
        f"CREATE VIEW main.customers AS SELECT * FROM read_csv_auto('{_CSV}')"
    )  # CSV backend
    con.execute(f"ATTACH '{_ORDERS_SQLITE}' AS o (TYPE sqlite)")  # SQLite backend
    con.execute("CREATE VIEW main.orders AS SELECT * FROM o.orders")
    rows = con.execute(sql).fetchall()
    cols = [d[0] for d in con.description]

    # Orders came from SQLite; each carries its customer (firstName/state) joined from the CSV.
    assert len(rows) == 30
    assert "customer" in cols
    joined = [r[cols.index("customer")] for r in rows]
    assert any(j and "firstName" in j for j in joined)
