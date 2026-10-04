# Copyright (c) 2026 Kenneth Stott
# Canary: de6dbe51-b8f5-45c9-bb83-c6724ffdfe2e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""E2E: DuckDBFederationRuntime federates every demo source type at once (REQ-825, REQ-840, REQ-844).

Consolidates the pieces the prior tests proved separately into the runtime object: ATTACH sources
(csv/sqlite/parquet) referenced in place via their connectors, and a NON-attachable openapi source
LANDED into the Postgres materialize store and attached. One DuckDB engine, one relationship-join
across the three heterogeneous file/db backends, plus the materialized source — all through the real
compiler. Honest boundary: this is the engine primitive, not the live HTTP/routing path.
"""

from __future__ import annotations

import os
from pathlib import Path
from types import SimpleNamespace

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

duckdb = pytest.importorskip("duckdb")
asyncpg = pytest.importorskip("asyncpg")

from provisa.compiler.introspect import ColumnMetadata  # noqa: E402
from provisa.compiler.parser import parse_query  # noqa: E402
from provisa.compiler.schema_gen import SchemaInput, generate_schema  # noqa: E402
from provisa.compiler.sql_gen import compile_query  # noqa: E402
from provisa.compiler.context import build_context  # noqa: E402
from provisa.compiler.sql_rewrite import rewrite_semantic_to_catalog_physical  # noqa: E402
from provisa.federation.duckdb_runtime import DuckDBFederationRuntime  # noqa: E402
from provisa.federation.replica_address import address_replicas  # noqa: E402
from tests.integration.replica_seed import (  # noqa: E402
    REPLICAS_SCHEMA,
    routes_to,
    seed_replica,
)
from tests.helpers import ALL_DATA_CAPABILITIES, registry_write_ops  # noqa: E402

_FILES = Path(__file__).parent.parent.parent / "demo" / "files"
_ADMIN = {"id": "admin", "capabilities": ALL_DATA_CAPABILITIES, "domain_access": ["*"]}


def _dsn() -> str:
    u = os.environ.get("PG_USER", "provisa")
    pw = os.environ.get("PG_PASSWORD", "provisa")
    h = os.environ.get("PG_HOST", "localhost")
    p = os.environ.get("PG_PORT", "5432")
    db = os.environ.get("PG_DATABASE", "provisa")
    return f"postgresql://{u}:{pw}@{h}:{p}/{db}"


def _col(n: str, d: str = "varchar", nl: bool = True) -> ColumnMetadata:
    return ColumnMetadata(column_name=n, data_type=d, is_nullable=nl)


def _src(sid: str, typ: str, table: str, path: str | None = None) -> SimpleNamespace:
    return SimpleNamespace(
        id=sid, type=SimpleNamespace(value=typ), schema_name="main", table_name=table, path=path
    )


def _tbl(i: int, sid: str, tname: str, cols: list[str]) -> dict:
    return {
        "id": i,
        "source_id": sid,
        "domain_id": "sales",
        "schema_name": "main",
        "table_name": tname,
        "write_ops": registry_write_ops({1: "csv", 2: "sqlite", 3: "parquet"}[i]),
        "columns": [{"column_name": c, "visible_to": ["admin"]} for c in cols],
    }


def _si() -> SchemaInput:
    return SchemaInput(
        tables=[
            _tbl(1, "cust", "customers", ["id", "first_name", "state"]),
            _tbl(2, "ordr", "orders", ["id", "customer_id", "product_id", "amount"]),
            _tbl(3, "prod", "products", ["id", "name", "category"]),
        ],
        relationships=[
            {
                "id": "o2c",
                "source_table_id": 2,
                "target_table_id": 1,
                "source_column": "customer_id",
                "target_column": "id",
                "cardinality": "many-to-one",
            },
            {
                "id": "o2p",
                "source_table_id": 2,
                "target_table_id": 3,
                "source_column": "product_id",
                "target_column": "id",
                "cardinality": "many-to-one",
            },
        ],
        column_types={
            1: [_col("id", "integer", False), _col("first_name"), _col("state")],
            2: [
                _col("id", "integer", False),
                _col("customer_id", "integer"),
                _col("product_id", "integer"),
                _col("amount", "double"),
            ],
            3: [_col("id", "integer", False), _col("name"), _col("category")],
        },
        naming_rules=[],
        role=_ADMIN,
        domains=[{"id": "sales", "description": "Sales"}],
        source_types={"cust": "csv", "ordr": "sqlite", "prod": "parquet"},
    )


async def test_runtime_federates_all_demo_source_types():
    rt = DuckDBFederationRuntime(materialize_dsn=_dsn())
    try:
        # ATTACH: csv + sqlite + parquet, referenced in place via their connectors.
        rt.attach_source(_src("cust", "csv", "customers", str(_FILES / "customers.csv")))
        rt.attach_source(_src("ordr", "sqlite", "orders", str(_FILES / "orders.sqlite")))
        rt.attach_source(_src("prod", "parquet", "products", str(_FILES / "products.parquet")))
        # REPLICATE: a non-attachable openapi source is replicated into the PG store, in the
        # replicas schema. Nothing is created at the table's registered name (REQ-1912).
        replica_schema, replica_table = await seed_replica(
            rt,
            "evt",
            "main",
            "events",
            [("id", "int"), ("order_id", "int"), ("kind", "text")],
            [
                {"id": 1, "order_id": 1, "kind": "shipped"},
                {"id": 2, "order_id": 2, "kind": "placed"},
            ],
        )

        ctx = build_context(_si())
        compiled = compile_query(
            parse_query(
                generate_schema(_si()),
                "{ orders { id amount customer { firstName } product { name category } } }",
                {},
            ),
            ctx,
        )[0]
        # Drive the runtime with catalog-physical names (catalog.schema.table) — exactly what the
        # request path (rewrite_semantic_to_catalog_physical) emits to every engine backend.
        res = await rt.execute(rewrite_semantic_to_catalog_physical(compiled.sql, ctx))

        # 30 orders (sqlite), each carrying its customer (csv) and product (parquet).
        assert len(res.rows) == 30
        assert res.column_names == ["id", "amount", "customer", "product"]
        first = dict(zip(res.column_names, res.rows[0]))
        assert "firstName" in first["customer"] and "category" in first["product"]

        # the replicated openapi source is read on the same engine at its replica's address: the
        # statement names the table catalog-physical and the address pass renames it
        routes = routes_to(
            "duckdb",
            "mat_store",
            live=("evt", "main", "events"),
            source_id="evt",
            schema_name="main",
            table_name="events",
        )
        lowered = address_replicas('SELECT count(*) AS n FROM "evt"."main"."events" AS "e"', routes)
        assert f'"mat_store"."{replica_schema}"."{replica_table}"' in lowered
        evt = await rt.execute(lowered)
        assert evt.rows[0][0] == 2
        with pytest.raises(duckdb.Error):
            await rt.execute('SELECT count(*) FROM "evt"."main"."events"')  # no exposure there
    finally:
        pg = await asyncpg.connect(dsn=_dsn())
        await pg.execute(f'DROP SCHEMA IF EXISTS "{REPLICAS_SCHEMA}" CASCADE')
        await pg.close()
        rt.close()
