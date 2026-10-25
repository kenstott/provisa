# Copyright (c) 2026 Kenneth Stott
# Canary: 402cf241-694d-453f-82e5-c90462268616
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""E2E: sqlite_fdw attaches a SQLite file IN PLACE inside a stock embedded PG (REQ-907).

No docker. Runs the embedded PG 16.11 from the test platform's private pginstall, whose staged
provisa-pg-ext bundle carries sqlite_fdw (its only runtime dep is the OS libsqlite3; tests/embedded_pg.py), then drives the REAL
SqliteFdwConnector DDL to attach the demo orders.sqlite and reads it through the REAL compiler
pipeline with an RLS predicate — asserting on returned rows. Unlike pg_duckdb, an FDW executes in
Postgres, so the postgres dialect (with nested SQL/JSON) runs unchanged.
"""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

pytestmark = [
    pytest.mark.integration,
    pytest.mark.local_platform,
    pytest.mark.asyncio(loop_scope="session"),
]

asyncpg = pytest.importorskip("asyncpg")

from tests.helpers import registry_write_ops, registry_write_returns_rows  # noqa: E402
from provisa.compiler.introspect import ColumnMetadata  # noqa: E402
from provisa.compiler.parser import parse_query  # noqa: E402
from provisa.compiler.rls import RLSContext  # noqa: E402
from provisa.compiler.schema_gen import SchemaInput, generate_schema  # noqa: E402
from provisa.compiler.sql_gen import compile_query  # noqa: E402
from provisa.compiler.context import build_context  # noqa: E402
from provisa.compiler.sql_rewrite import rewrite_semantic_to_physical  # noqa: E402
from provisa.compiler.stage2 import apply_governance, build_governance_context  # noqa: E402
from provisa.federation.connector_duckdb import SqliteFdwConnector  # noqa: E402
from provisa.transpiler.transpile import transpile  # noqa: E402
from tests.embedded_pg import start_local_postgres  # noqa: E402
from tests.helpers import ALL_DATA_CAPABILITIES  # noqa: E402

_SQLITE = Path(__file__).parent.parent.parent / "demo" / "files" / "orders.sqlite"
_ADMIN = {
    "description": "Test role.",
    "id": "admin",
    "capabilities": ALL_DATA_CAPABILITIES,
    "domain_access": ["*"],
}


@pytest.fixture(scope="session")
def embedded_pg_sqlite_fdw():
    pg = start_local_postgres(fdws=True)
    yield pg
    pg.stop()


def _col(n: str, d: str = "varchar", nl: bool = True) -> ColumnMetadata:
    return ColumnMetadata(column_name=n, data_type=d, is_nullable=nl)


def _si(orders_schema: str) -> SchemaInput:
    return SchemaInput(
        tables=[
            {
                "id": 1,
                "source_id": "ord",
                "domain_id": "sales",
                "schema_name": orders_schema,
                "table_name": "orders",
                "write_ops": registry_write_ops("sqlite"),
                "write_returns_rows": registry_write_returns_rows("sqlite"),
                "write_refused_forms": [],
                "columns": [
                    {"column_name": c, "visible_to": ["admin"]} for c in ("id", "amount", "region")
                ],
            }
        ],  # fmt: skip
        relationships=[],
        column_types={1: [_col("id", "integer", False), _col("amount", "double"), _col("region")]},
        naming_rules=[],
        role=_ADMIN,
        domains=[{"id": "sales", "description": "Sales"}],
        source_types={"ord": "sqlite"},
    )


def _compile(si: SchemaInput, gql: str, rls: RLSContext) -> str:
    ctx = build_context(si)
    compiled = compile_query(parse_query(generate_schema(si), gql, {}, ctx=ctx), ctx)[0]
    gov = build_governance_context("admin", rls, {}, ctx, si.tables, role=_ADMIN)
    return transpile(
        rewrite_semantic_to_physical(apply_governance(compiled.sql, gov), ctx), "postgres"
    )


async def test_sqlite_fdw_connector_attaches_and_reads(embedded_pg_sqlite_fdw):
    """The REAL SqliteFdwConnector DDL attaches orders.sqlite; a governed read runs in place."""
    conn = await asyncpg.connect(dsn=embedded_pg_sqlite_fdw.dsn())
    try:
        det = SqliteFdwConnector().details(
            SimpleNamespace(id="ord", catalog="ord", path=str(_SQLITE))
        )
        for ddl in det["attach_ddl"]:  # CREATE EXTENSION / SERVER / SCHEMA / IMPORT FOREIGN SCHEMA
            await conn.execute(ddl)
        orders_schema = det["local_schema"]  # fdw_ord

        rls = RLSContext(rules={1: "region = 'us-east'"}, domain_rules={})
        sql = _compile(_si(orders_schema), "{ orders { id amount region } }", rls)
        rows = await conn.fetch(sql)

        # 10 us-east orders read from orders.sqlite in place; governance filtered the other 20.
        assert len(rows) == 10
        assert {r["region"] for r in rows} == {"us-east"}
        assert all(r["amount"] is not None for r in rows)
    finally:
        await conn.execute("DROP SCHEMA IF EXISTS fdw_ord CASCADE")
        await conn.execute("DROP SERVER IF EXISTS fdw_ord CASCADE")
        await conn.close()
