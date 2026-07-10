# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""E2E: a Postgres federation engine with ZERO connectors — everything materializes (REQ-844, REQ-893).

Experiment (per the "if PG had zero connectors it should still work" idea): with no attach connectors,
no source is reachable in place, so every source is LANDED into the engine's own native store and
federated with plain SQL — no FDW required, so it runs on a STOCK embedded PG (pgserver ships only
plpgsql + vector, no contrib FDWs). This is the SELF_ONLY/warehouse shape: land-into-self, then
native federation. Cost: full data movement (every source copied in), vs the FDW/attach in-place path.

Drives the real pipeline (compile -> apply_governance -> rewrite_semantic_to_physical ->
transpile("postgres") -> execute) against the landed native tables, reading the real demo files.
"""

from __future__ import annotations

import csv
import json
import os
import sqlite3
from pathlib import Path

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
from provisa.core.database import Database, create_engine_from_url  # noqa: E402
from provisa.federation.materialize_exec import build_table, land_replace  # noqa: E402
from provisa.transpiler.transpile import transpile  # noqa: E402

_FILES = Path(__file__).parent.parent.parent / "demo" / "files"
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


def _tbl(i: int, tname: str, cols: list[str]) -> dict:
    return {
        "id": i,
        "source_id": f"s{i}",
        "domain_id": "sales",
        "schema_name": "pgself",  # the engine's own native store
        "table_name": tname,
        "governance": "pre-approved",
        "columns": [{"column_name": c, "visible_to": ["admin"]} for c in cols],
    }


def _si() -> SchemaInput:
    return SchemaInput(
        tables=[
            _tbl(1, "customers", ["id", "first_name", "state"]),
            _tbl(2, "orders", ["id", "customer_id", "amount"]),
        ],
        relationships=[
            {
                "id": "o2c",
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
        source_types={"s1": "csv", "s2": "sqlite"},
    )


async def test_postgres_zero_connector_engine_lands_everything():
    """No connectors: CSV + SQLite sources are landed into PG's native store and federated natively."""
    with open(_FILES / "customers.csv") as f:
        customers = [
            {"id": int(r["id"]), "first_name": r["first_name"], "state": r["state"]}
            for r in csv.DictReader(f)
        ]
    sq = sqlite3.connect(str(_FILES / "orders.sqlite"))
    sq.row_factory = sqlite3.Row
    orders = [
        {"id": r["id"], "customer_id": r["customer_id"], "amount": r["amount"]}
        for r in sq.execute("SELECT id, customer_id, amount FROM orders")
    ]
    sq.close()

    conn = await asyncpg.connect(dsn=_dsn())
    try:
        # ZERO connectors => every source materializes into the engine's OWN native store (no FDW).
        from sqlalchemy.schema import CreateSchema

        eng = create_engine_from_url(_dsn("+asyncpg"), pool_size=1)
        try:
            async with Database(eng, name="mat").acquire() as sconn:
                await sconn.execute_core(CreateSchema("pgself", if_not_exists=True))
                await land_replace(
                    sconn,
                    build_table(
                        "pgself",
                        "customers",
                        [("id", "int"), ("first_name", "text"), ("state", "text")],
                    ),
                    customers,
                )
                await land_replace(
                    sconn,
                    build_table(
                        "pgself",
                        "orders",
                        [("id", "int"), ("customer_id", "int"), ("amount", "double precision")],
                    ),
                    orders,
                )
        finally:
            await eng.dispose()

        ctx = build_context(_si())
        compiled = compile_query(
            parse_query(
                generate_schema(_si()), "{ orders { id amount customer { firstName state } } }", {}
            ),
            ctx,
        )[0]
        rls = RLSContext(rules={1: "state = 'NY'"}, domain_rules={})
        gov_ctx = build_governance_context("admin", rls, {}, ctx, _si().tables, role=_ADMIN)
        governed = apply_governance(compiled.sql, gov_ctx)
        sql = transpile(rewrite_semantic_to_physical(governed, ctx), "postgres")
        rows = await conn.fetch(sql)

        # Every order comes back; the nested customer is governed to NY-only (native join, no FDW).
        assert len(rows) == len(orders)
        parsed = [json.loads(r["customer"]) if r["customer"] else None for r in rows]
        visible = [c for c in parsed if c]
        assert visible and all(c["state"] == "NY" for c in visible)
        assert any(c is None for c in parsed)  # non-NY customers masked out
    finally:
        await conn.execute("DROP SCHEMA IF EXISTS pgself CASCADE")
        await conn.close()
