# Copyright (c) 2026 Kenneth Stott
# Canary: 712607bd-8647-461a-beab-2f359744b78d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""E2E: Snowflake as the federation engine through the REAL query pipeline (REQ-988).

Mirrors the Databricks engine e2e: drive the real pipeline (compile → govern → catalog-physical →
transpile(..., "snowflake")) and execute the governed Snowflake SQL against a live account via the
Arrow read path (fetch_arrow_all / fetch_arrow_batches). SKIPPED here — no Snowflake account creds and
snowflake-connector-python is not installed in this environment (REQ-988). Set SNOWFLAKE_ACCOUNT /
SNOWFLAKE_USER / SNOWFLAKE_PASSWORD (and install the connector) to enable.
"""

from __future__ import annotations

import os
from urllib.parse import quote

import pytest

from tests.env_creds import unset

pytestmark = [pytest.mark.integration, pytest.mark.requires_warehouse]

# Skips when the driver is absent (current dev/CI) — the reason is explicit, never a silent pass.
pytest.importorskip("snowflake.connector", reason="snowflake-connector-python not installed")

_ENV = ("SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD")
_HAVE_CREDS = all(os.environ.get(v) for v in _ENV)
pytestmark.append(pytest.mark.skipif(not _HAVE_CREDS, reason=f"not set: {unset(*_ENV)}"))

from provisa.compiler.context import build_context  # noqa: E402
from provisa.compiler.introspect import ColumnMetadata  # noqa: E402
from provisa.compiler.parser import parse_query  # noqa: E402
from provisa.compiler.rls import RLSContext  # noqa: E402
from provisa.compiler.schema_gen import SchemaInput, generate_schema  # noqa: E402
from provisa.compiler.sql_gen import compile_query  # noqa: E402
from provisa.compiler.sql_rewrite import rewrite_semantic_to_catalog_physical  # noqa: E402
from provisa.compiler.stage2 import apply_governance, build_governance_context  # noqa: E402
from provisa.federation.snowflake_runtime import SnowflakeFederationRuntime  # noqa: E402
from provisa.transpiler.transpile import transpile  # noqa: E402
from tests.helpers import ALL_DATA_CAPABILITIES, registry_write_ops  # noqa: E402

_ADMIN = {"id": "admin", "capabilities": ALL_DATA_CAPABILITIES, "domain_access": ["*"]}
_SRC = "e2e-sf"
_SCHEMA = "public"
_TABLE = "orders"


def _col(n: str, d: str = "varchar", nl: bool = True) -> ColumnMetadata:
    return ColumnMetadata(column_name=n, data_type=d, is_nullable=nl)


def _schema_input() -> SchemaInput:
    return SchemaInput(
        tables=[
            {
                "id": 1,
                "source_id": _SRC,
                "domain_id": "sales",
                "schema_name": _SCHEMA,
                "table_name": _TABLE,
                "write_ops": registry_write_ops("snowflake"),
                "columns": [
                    {"column_name": c, "visible_to": ["admin"]} for c in ("id", "region", "amount")
                ],
            }
        ],
        relationships=[],
        column_types={1: [_col("id", "integer", False), _col("region"), _col("amount", "double")]},
        naming_rules=[],
        role=_ADMIN,
        domains=[{"id": "sales", "description": "Sales"}],
        source_types={_SRC: "snowflake"},
    )


def _compile_to_snowflake(si: SchemaInput, gql: str, rls: RLSContext | None = None) -> str:
    schema = generate_schema(si)
    ctx = build_context(si)
    compiled = compile_query(parse_query(schema, gql, {}, ctx=ctx), ctx)[0]
    sql = compiled.sql
    if rls is not None:
        gov_ctx = build_governance_context("admin", rls, {}, ctx, si.tables, role=_ADMIN)
        sql = apply_governance(sql, gov_ctx)
    return transpile(rewrite_semantic_to_catalog_physical(sql, ctx), "snowflake")


_CATALOG = _SRC.replace("-", "_")  # compiler catalog for source id (e2e_sf)


@pytest.fixture(scope="module")
def runtime():
    acct = os.environ["SNOWFLAKE_ACCOUNT"]
    user = os.environ["SNOWFLAKE_USER"]
    pw = os.environ["SNOWFLAKE_PASSWORD"]
    wh = os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
    # Percent-encode credentials: passwords may contain URL-reserved characters
    # (e.g. '#', '@', '/', ':') that would otherwise corrupt DSN parsing.
    url = (
        f"snowflake://{quote(user, safe='')}:{quote(pw, safe='')}"
        f"@{acct}/{_CATALOG}/{_SCHEMA}?warehouse={wh}"
    )
    rt = SnowflakeFederationRuntime(url=url)
    # Snowflake is a READ engine (REQ-988): seed the source table directly so the governed query has
    # data to read at the compiler's physical name (database=source catalog, schema, table).
    # The compiler emits quoted-lowercase physical names (e.g. "e2e_sf"."public"."orders");
    # Snowflake treats quoted identifiers as case-sensitive, so the seed DDL must quote them
    # too — unquoted DDL would fold to uppercase (E2E_SF) and the governed query wouldn't resolve.
    fq = f'"{_CATALOG}"."{_SCHEMA}"."{_TABLE}"'
    cur = rt.connection.cursor()
    try:
        cur.execute(f'CREATE DATABASE IF NOT EXISTS "{_CATALOG}"')
        cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{_CATALOG}"."{_SCHEMA}"')
        cur.execute(f'CREATE OR REPLACE TABLE {fq} ("id" NUMBER, "region" STRING, "amount" FLOAT)')
        cur.execute(f"INSERT INTO {fq} VALUES (1,'west',100),(2,'east',200)")
    finally:
        cur.close()
    try:
        yield rt
    finally:
        cur = rt.connection.cursor()
        try:
            # Drop the whole database the fixture created (cascades schema + table) so runs
            # never leak databases into the account.
            cur.execute(f'DROP DATABASE IF EXISTS "{_CATALOG}"')
        finally:
            cur.close()
        rt.close()


@pytest.mark.asyncio
async def test_snowflake_engine_governed_query_round_trip(runtime):
    sql = _compile_to_snowflake(_schema_input(), "{ orders { id region amount } }")
    table = runtime.run_arrow(sql)
    assert table.num_rows == 2


@pytest.mark.asyncio
async def test_snowflake_engine_applies_rls(runtime):
    rls = RLSContext(rules={1: "region = 'west'"}, domain_rules={})
    sql = _compile_to_snowflake(_schema_input(), "{ orders { id region amount } }", rls=rls)
    assert "WHERE" in sql.upper()
    table = runtime.run_arrow(sql)
    assert all(r == "west" for r in table.column("region").to_pylist())


@pytest.mark.asyncio
async def test_snowflake_export_view_exists_where_the_export_addresses_it(runtime):
    """REQ-1912: the reconcile publishes a SECURE VIEW of each replica in the export schema; the
    Horizon export shares and comments the object at the address it is handed for that table.
    Both are one address, so the export finds the view — and nothing is created at the table's
    registered address."""
    from provisa.api.metadata_export.model import AssetKind, AssetRef
    from provisa.api.metadata_export.snowflake_horizon import PublishedAddresses, _object_kind
    from provisa.federation.replica_address import export_view_address, replica_address

    identity = {"source_id": "evt-api", "schema_name": _SCHEMA, "table_name": "events"}
    replica = replica_address(org_id="itest", **identity)
    view = export_view_address(org_id="itest", **identity)
    columns = [("id", "integer"), ("kind", "text")]
    await runtime.reconcile_replica(
        schema=replica.schema, table=replica.table, columns=columns, pk_columns=["id"]
    )
    await runtime.publish_replica_view(
        view_schema=view.schema,
        view_table=view.table,
        schema=replica.schema,
        table=replica.table,
        replace=False,
    )
    await runtime.land_table(
        schema=replica.schema,
        table=replica.table,
        columns=columns,
        rows=[{"id": 1, "kind": "shipped"}, {"id": 2, "kind": "placed"}],
    )

    database = runtime.ensure_materialize_attached()
    addresses = PublishedAddresses(
        landing_database=database,
        export_views={("evt-api", _SCHEMA, "events"): (view.schema, view.table)},
    )
    published = addresses.table(
        AssetRef(kind=AssetKind.TABLE, parts=("evt-api", _SCHEMA, "events"))
    )

    assert published == (database, "org_itest_export", "evt-api__public__events")
    assert _object_kind(runtime, published) == "VIEW"
    assert (
        _object_kind(runtime, (database, _SCHEMA, "events")) is None
    )  # not the registered address
    count = runtime.run_sync(
        f'SELECT COUNT(*) FROM "{published[0]}"."{published[1]}"."{published[2]}"'
    ).rows
    assert count[0][0] == 2


if __name__ == "__main__":
    pytest.main([__file__, "-q"])
