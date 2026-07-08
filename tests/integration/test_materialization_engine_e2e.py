# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""E2E: the MATERIALIZATION path — a non-attachable source landed into a PG store, then federated by
the DuckDB engine (REQ-844, REQ-848, REQ-893).

The earlier engine e2e tests only exercised ATTACH sources (files/DBs referenced in place). This one
covers what those omitted: a source that CANNOT be attached (openapi/graphql_remote) is LANDED into a
Postgres materialization store via land_replace (WriteFace.SQLALCHEMY_UPSERT execution), and the
DuckDB engine then ATTACHes that PG store and federates the materialized data with an in-place file
source — through the real compile/govern/transpile/execute pipeline, with RLS asserted.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

duckdb = pytest.importorskip("duckdb")
asyncpg = pytest.importorskip("asyncpg")

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
from provisa.core.database import Database, create_engine_from_url  # noqa: E402
from provisa.federation.materialize_exec import build_table, land_replace  # noqa: E402
from provisa.transpiler.transpile import transpile  # noqa: E402

_CSV = str(Path(__file__).parent.parent.parent / "demo" / "files" / "customers.csv")
_ADMIN = {"id": "admin", "capabilities": ["admin"], "domain_access": ["*"]}


def _dsn(driver: str = "") -> str:
    u = os.environ.get("PG_USER", "provisa")
    pw = os.environ.get("PG_PASSWORD", "provisa")
    h = os.environ.get("PG_HOST", "localhost")
    p = os.environ.get("PG_PORT", "5432")
    db = os.environ.get("PG_DATABASE", "provisa")
    return f"postgresql{driver}://{u}:{pw}@{h}:{p}/{db}"


def _col(n: str, d: str = "varchar", nl: bool = True) -> ColumnMetadata:
    return ColumnMetadata(column_name=n, data_type=d, is_nullable=nl)


def _si() -> SchemaInput:
    customers = {
        "id": 1,
        "source_id": "cust",
        "domain_id": "sales",
        "schema_name": "main",
        "table_name": "customers",
        "governance": "pre-approved",
        "columns": [
            {"column_name": c, "visible_to": ["admin"]} for c in ("id", "first_name", "state")
        ],
    }
    orders_api = {
        "id": 2,
        "source_id": "ord_api",
        "domain_id": "sales",
        "schema_name": "main",
        "table_name": "orders_api",
        "governance": "pre-approved",
        "columns": [
            {"column_name": c, "visible_to": ["admin"]} for c in ("id", "customer_id", "amount")
        ],
    }
    rel = {
        "id": "o2c",
        "source_table_id": 2,
        "target_table_id": 1,
        "source_column": "customer_id",
        "target_column": "id",
        "cardinality": "many-to-one",
    }
    return SchemaInput(
        tables=[customers, orders_api],
        relationships=[rel],
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
        # ord_api is openapi — NON-attachable; it only reaches the engine via the materialize store.
        source_types={"cust": "csv", "ord_api": "openapi"},
    )


async def test_materialized_api_source_federates_via_pg_store():
    """Land a non-attachable API source into PG, then DuckDB federates it with an attached CSV."""
    conn = await asyncpg.connect(dsn=_dsn())
    try:
        # 1) MATERIALIZE: land API-shaped rows into the PG store (the write face's real execution).
        api_rows = [
            {"id": 10, "customer_id": 1, "amount": 19.99},  # customer 1 = Alice / NY
            {"id": 11, "customer_id": 2, "amount": 49.99},  # customer 2 = Bob / CA
            {"id": 12, "customer_id": 1, "amount": 5.0},
        ]
        from sqlalchemy.schema import CreateSchema

        eng = create_engine_from_url(_dsn("+asyncpg"), pool_size=1)
        try:
            async with Database(eng, name="mat").acquire() as sconn:
                await sconn.execute_core(CreateSchema("e2e_materialize", if_not_exists=True))
                table = build_table(
                    "e2e_materialize",
                    "orders",
                    [("id", "int"), ("customer_id", "int"), ("amount", "double precision")],
                )
                loc = await land_replace(sconn, table, api_rows)
        finally:
            await eng.dispose()
        assert loc == "e2e_materialize.orders"
        landed = await conn.fetch("SELECT * FROM e2e_materialize.orders ORDER BY id")
        assert len(landed) == 3  # the API source really landed in the store

        # 2) Compile a federated query joining the materialized source with the CSV file.
        rls = RLSContext(
            rules={1: "state = 'NY'"}, domain_rules={}
        )  # governance on the CSV customer
        schema = generate_schema(_si())
        ctx = build_context(_si())
        compiled = compile_query(
            parse_query(schema, "{ ordersApi { id amount customer { firstName state } } }", {}), ctx
        )[0]
        gov_ctx = build_governance_context("admin", rls, {}, ctx, _si().tables, role=_ADMIN)
        governed = apply_governance(compiled.sql, gov_ctx)
        ddl = transpile(rewrite_semantic_to_physical(governed, ctx), "duckdb")

        # 3) DuckDB engine: ATTACH the PG materialize store + the in-place CSV; federate.
        con = duckdb.connect()
        con.execute("INSTALL postgres")
        con.execute("LOAD postgres")
        u = os.environ.get("PG_USER", "provisa")
        pw = os.environ.get("PG_PASSWORD", "provisa")
        h = os.environ.get("PG_HOST", "localhost")
        p = os.environ.get("PG_PORT", "5432")
        db = os.environ.get("PG_DATABASE", "provisa")
        con.execute(
            f"ATTACH 'host={h} port={p} dbname={db} user={u} password={pw}' AS pg (TYPE postgres)"
        )
        con.execute(f"CREATE VIEW main.customers AS SELECT * FROM read_csv_auto('{_CSV}')")
        con.execute("CREATE VIEW main.orders_api AS SELECT * FROM pg.e2e_materialize.orders")
        rows = con.execute(ddl).fetchall()
        cols = [d[0] for d in con.description]

        # All three materialized orders come back; the nested customer is governed to NY-only.
        assert len(rows) == 3
        parsed = [
            json.loads(r[cols.index("customer")]) if r[cols.index("customer")] else None
            for r in rows
        ]
        visible = [c for c in parsed if c]
        assert visible and all(c["state"] == "NY" for c in visible)  # RLS applied on the joined CSV
        assert any(c is None for c in parsed)  # order 11 (Bob/CA) has no visible customer after RLS
    finally:
        await conn.execute("DROP SCHEMA IF EXISTS e2e_materialize CASCADE")
        await conn.close()
