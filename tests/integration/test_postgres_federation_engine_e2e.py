# Copyright (c) 2026 Kenneth Stott
# Canary: 19ab8fb8-f898-456c-9c96-e1004bbf2265
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""E2E: Postgres as the federation engine via FDW (SQL/MED) through the REAL pipeline (REQ-893).

Same discipline as the DuckDB engine e2e: drive the actual Provisa primitives (generate_schema/
build_context/compile_query, apply_governance, rewrite_semantic_to_physical, transpile(..,"postgres"))
and execute the resulting governed SQL on a real Postgres that has ATTACHed the sources via FDW —
asserting on returned rows. Two stock FDWs are exercised, both bundled with the standard PG image:

  - postgres_fdw: a remote PostgreSQL source attached as an imported foreign schema (loopback here).
  - file_fdw:     a CSV source attached as a foreign table over a server-side file.

A local engine table joined to the FDW-attached source proves the engine federates in place.
"""

from __future__ import annotations

import os

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

asyncpg = pytest.importorskip("asyncpg")

from provisa.compiler.introspect import ColumnMetadata  # noqa: E402
from provisa.compiler.parser import parse_query  # noqa: E402
from provisa.compiler.rls import RLSContext  # noqa: E402
from provisa.compiler.schema_gen import SchemaInput, generate_schema  # noqa: E402
from provisa.compiler.sql_gen import compile_query  # noqa: E402
from provisa.compiler.context import build_context  # noqa: E402
from provisa.compiler.sql_rewrite import rewrite_semantic_to_physical  # noqa: E402
from provisa.compiler.stage2 import apply_governance, build_governance_context  # noqa: E402
from provisa.transpiler.transpile import transpile  # noqa: E402

_ADMIN = {"id": "admin", "capabilities": ["admin"], "domain_access": ["*"]}


def _dsn() -> str:
    h = os.environ.get("PG_HOST", "localhost")
    p = os.environ.get("PG_PORT", "5432")
    u = os.environ.get("PG_USER", "provisa")
    pw = os.environ.get("PG_PASSWORD", "provisa")
    db = os.environ.get("PG_DATABASE", "provisa")
    return f"postgresql://{u}:{pw}@{h}:{p}/{db}"


def _col(n: str, d: str = "varchar", nl: bool = True) -> ColumnMetadata:
    return ColumnMetadata(column_name=n, data_type=d, is_nullable=nl)


def _orders_table(schema: str) -> dict:
    return {
        "id": 2,
        "source_id": "ord",
        "domain_id": "sales",
        "schema_name": schema,
        "table_name": "orders",
        "governance": "pre-approved",
        "columns": [
            {"column_name": c, "visible_to": ["admin"]} for c in ("id", "customer_id", "amount")
        ],
    }


def _customers_table(schema: str) -> dict:
    return {
        "id": 1,
        "source_id": "cust",
        "domain_id": "sales",
        "schema_name": schema,
        "table_name": "customers",
        "governance": "pre-approved",
        "columns": [{"column_name": c, "visible_to": ["admin"]} for c in ("id", "name", "state")],
    }


def _compile_to_postgres(si: SchemaInput, gql: str, rls: RLSContext | None = None) -> str:
    schema = generate_schema(si)
    ctx = build_context(si)
    compiled = compile_query(parse_query(schema, gql, {}), ctx)[0]
    sql = compiled.sql
    if rls is not None:
        gov_ctx = build_governance_context("admin", rls, {}, ctx, si.tables, role=_ADMIN)
        sql = apply_governance(sql, gov_ctx)
    return transpile(rewrite_semantic_to_physical(sql, ctx), "postgres")


def _si(customers_schema: str, orders_schema: str) -> SchemaInput:
    return SchemaInput(
        tables=[_customers_table(customers_schema), _orders_table(orders_schema)],
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
            1: [_col("id", "integer", False), _col("name"), _col("state")],
            2: [
                _col("id", "integer", False),
                _col("customer_id", "integer"),
                _col("amount", "double"),
            ],
        },
        naming_rules=[],
        role=_ADMIN,
        domains=[{"id": "sales", "description": "Sales"}],
        source_types={"cust": "postgresql", "ord": "postgresql"},
    )


async def test_postgres_federates_via_postgres_fdw():
    """Local engine table JOIN a postgres_fdw-attached remote source, through the real pipeline."""
    conn = await asyncpg.connect(dsn=_dsn())
    try:
        # remote source (postgres_fdw connects to it over loopback)
        await conn.execute("CREATE EXTENSION IF NOT EXISTS postgres_fdw")
        await conn.execute("CREATE SCHEMA IF NOT EXISTS e2e_remote")
        await conn.execute("DROP TABLE IF EXISTS e2e_remote.customers CASCADE")
        await conn.execute(
            "CREATE TABLE e2e_remote.customers(id int primary key, name text, state text)"
        )
        await conn.execute(
            "INSERT INTO e2e_remote.customers VALUES (1,'Alice','NY'),(2,'Bob','CA'),(3,'Cara','NY')"
        )
        # local engine table
        await conn.execute("CREATE SCHEMA IF NOT EXISTS e2e_engine")
        await conn.execute("DROP TABLE IF EXISTS e2e_engine.orders CASCADE")
        await conn.execute(
            "CREATE TABLE e2e_engine.orders(id int, customer_id int, amount numeric)"
        )
        await conn.execute(
            "INSERT INTO e2e_engine.orders VALUES (10,1,19.99),(11,2,49.99),(12,1,5.00),(13,3,7.50)"
        )
        # ATTACH the remote source via postgres_fdw. This is a SELF-referential FDW
        # (same postgres) for testing federation. postgres_fdw dials from INSIDE the
        # postgres container, so it must use the container-internal address
        # (localhost:5432), not the host-published ephemeral ${PG_PORT} which is not
        # bound inside the container.
        await conn.execute("DROP SERVER IF EXISTS e2e_remote_pg CASCADE")
        await conn.execute(
            f"CREATE SERVER e2e_remote_pg FOREIGN DATA WRAPPER postgres_fdw "
            f"OPTIONS (host 'localhost', port '5432', dbname '{os.environ.get('PG_DATABASE', 'provisa')}')"
        )
        u = os.environ.get("PG_USER", "provisa")
        pw = os.environ.get("PG_PASSWORD", "provisa")
        await conn.execute(
            f"CREATE USER MAPPING FOR CURRENT_USER SERVER e2e_remote_pg "
            f"OPTIONS (user '{u}', password '{pw}')"
        )
        await conn.execute("DROP SCHEMA IF EXISTS e2e_foreign CASCADE")
        await conn.execute("CREATE SCHEMA e2e_foreign")
        await conn.execute(
            "IMPORT FOREIGN SCHEMA e2e_remote LIMIT TO (customers) "
            "FROM SERVER e2e_remote_pg INTO e2e_foreign"
        )

        # Real pipeline: orders (local) with nested customer (postgres_fdw remote), RLS on customers.
        rls = RLSContext(rules={1: "state = 'NY'"}, domain_rules={})
        sql = _compile_to_postgres(
            _si(customers_schema="e2e_foreign", orders_schema="e2e_engine"),
            "{ orders { id amount customer { name state } } }",
            rls=rls,
        )
        rows = await conn.fetch(sql)

        # 4 orders total; the nested customer is governed to NY-only (Bob/CA masked out of the join).
        assert len(rows) == 4
        customers = [r["customer"] for r in rows]
        import json

        parsed = [json.loads(c) if c else None for c in customers]
        ny = [c for c in parsed if c]
        assert ny and all(c["state"] == "NY" for c in ny)  # RLS applied through the FDW join
        # order 11 (Bob/CA) has no visible customer after RLS
        assert any(c is None for c in parsed)
    finally:
        await conn.execute("DROP SCHEMA IF EXISTS e2e_foreign CASCADE")
        await conn.execute("DROP SERVER IF EXISTS e2e_remote_pg CASCADE")
        await conn.execute("DROP SCHEMA IF EXISTS e2e_remote CASCADE")
        await conn.execute("DROP SCHEMA IF EXISTS e2e_engine CASCADE")
        await conn.close()


async def test_postgres_federates_csv_via_file_fdw():
    """A CSV source attached with file_fdw, joined to a local engine table, through the pipeline."""
    conn = await asyncpg.connect(dsn=_dsn())
    try:
        # Produce a server-side CSV (so the test needs no docker cp), then attach it via file_fdw.
        await conn.execute("CREATE SCHEMA IF NOT EXISTS e2e_src")
        await conn.execute("DROP TABLE IF EXISTS e2e_src.customers CASCADE")
        await conn.execute("CREATE TABLE e2e_src.customers(id int, name text, state text)")
        await conn.execute(
            "INSERT INTO e2e_src.customers VALUES (1,'Alice','NY'),(2,'Bob','CA'),(3,'Cara','NY')"
        )
        await conn.execute(
            "COPY e2e_src.customers TO '/tmp/e2e_customers.csv' WITH (FORMAT csv, HEADER true)"
        )
        await conn.execute("CREATE EXTENSION IF NOT EXISTS file_fdw")
        await conn.execute("DROP SERVER IF EXISTS e2e_file_srv CASCADE")
        await conn.execute("CREATE SERVER e2e_file_srv FOREIGN DATA WRAPPER file_fdw")
        await conn.execute("DROP SCHEMA IF EXISTS e2e_files CASCADE")
        await conn.execute("CREATE SCHEMA e2e_files")
        await conn.execute(
            "CREATE FOREIGN TABLE e2e_files.customers (id int, name text, state text) "
            "SERVER e2e_file_srv OPTIONS (filename '/tmp/e2e_customers.csv', format 'csv', header 'true')"
        )
        # local engine table
        await conn.execute("CREATE SCHEMA IF NOT EXISTS e2e_engine2")
        await conn.execute("DROP TABLE IF EXISTS e2e_engine2.orders CASCADE")
        await conn.execute(
            "CREATE TABLE e2e_engine2.orders(id int, customer_id int, amount numeric)"
        )
        await conn.execute(
            "INSERT INTO e2e_engine2.orders VALUES (10,1,19.99),(11,2,49.99),(12,1,5.00)"
        )

        sql = _compile_to_postgres(
            _si(customers_schema="e2e_files", orders_schema="e2e_engine2"),
            "{ orders { id amount customer { name state } } }",
        )
        rows = await conn.fetch(sql)

        # 3 orders, each joined to its customer read from the CSV via file_fdw.
        assert len(rows) == 3
        import json

        parsed = [json.loads(r["customer"]) for r in rows if r["customer"]]
        assert {c["name"] for c in parsed} == {"Alice", "Bob"}  # customers 1 and 2 referenced
    finally:
        await conn.execute("DROP FOREIGN TABLE IF EXISTS e2e_files.customers")
        await conn.execute("DROP SCHEMA IF EXISTS e2e_files CASCADE")
        await conn.execute("DROP SERVER IF EXISTS e2e_file_srv CASCADE")
        await conn.execute("DROP SCHEMA IF EXISTS e2e_src CASCADE")
        await conn.execute("DROP SCHEMA IF EXISTS e2e_engine2 CASCADE")
        await conn.close()
