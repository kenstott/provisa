# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""E2E: sqlite_fdw attaches a SQLite file IN PLACE inside a stock embedded PG (REQ-907).

No docker. Provisions a pgserver embedded PG 16.2, installs sqlite_fdw (built against the cached PG 16.2
source; its only runtime dep is the OS libsqlite3), then drives the REAL SqliteFdwConnector DDL to
attach the demo orders.sqlite and reads it through the REAL compiler pipeline with an RLS predicate —
asserting on returned rows. Unlike pg_duckdb, an FDW executes in Postgres, so the postgres dialect (with
nested SQL/JSON) runs unchanged.

Skips unless sqlite_fdw is prebuilt in the cache.
"""

from __future__ import annotations

import shutil
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

asyncpg = pytest.importorskip("asyncpg")
pgserver = pytest.importorskip("pgserver")

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

_SQLITE = Path(__file__).parent.parent.parent / "demo" / "files" / "orders.sqlite"
_ADMIN = {"id": "admin", "capabilities": ["admin"], "domain_access": ["*"]}
_CACHE = Path.home() / ".cache" / "provisa-fdw" / "pg162"
_SQLITE_FDW = _CACHE / "lib" / "postgresql" / "sqlite_fdw.dylib"


def _install_sqlite_fdw() -> None:
    src_lib = _CACHE / "lib" / "postgresql"
    src_ext = _CACHE / "share" / "postgresql" / "extension"
    pginstall = Path(pgserver.__file__).parent / "pginstall"
    dl = pginstall / "lib" / "postgresql"
    de = pginstall / "share" / "postgresql" / "extension"
    suffix = "dylib" if (dl / "plpgsql.dylib").exists() else "so"
    shutil.copy(src_lib / f"sqlite_fdw.{suffix}", dl / f"sqlite_fdw.{suffix}")
    shutil.copy(src_ext / "sqlite_fdw.control", de / "sqlite_fdw.control")
    for f in src_ext.glob("sqlite_fdw--*.sql"):
        shutil.copy(f, de / f.name)


@pytest.fixture(scope="session")
def embedded_pg_sqlite_fdw():
    if sys.platform != "darwin" or not _SQLITE_FDW.exists():
        pytest.skip("sqlite_fdw not prebuilt in cache")
    _install_sqlite_fdw()
    server = pgserver.get_server(tempfile.mkdtemp(prefix="provisa_sqlite_fdw_"))
    yield server


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
                "governance": "pre-approved",
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
    compiled = compile_query(parse_query(generate_schema(si), gql, {}), ctx)[0]
    gov = build_governance_context("admin", rls, {}, ctx, si.tables, role=_ADMIN)
    return transpile(
        rewrite_semantic_to_physical(apply_governance(compiled.sql, gov), ctx), "postgres"
    )


async def test_sqlite_fdw_connector_attaches_and_reads(embedded_pg_sqlite_fdw):
    """The REAL SqliteFdwConnector DDL attaches orders.sqlite; a governed read runs in place."""
    conn = await asyncpg.connect(dsn=embedded_pg_sqlite_fdw.get_uri())
    try:
        det = SqliteFdwConnector().details(SimpleNamespace(id="ord", path=str(_SQLITE)))
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
