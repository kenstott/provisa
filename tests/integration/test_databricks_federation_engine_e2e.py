# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""E2E: Databricks as the federation engine through the REAL query pipeline (REQ-987).

Drives the actual Provisa primitives — generate_schema/compile_query, apply_governance (RLS),
rewrite_semantic_to_catalog_physical, transpile(..., "databricks") — then MATERIALIZES demo rows into
the live Databricks warehouse via ``DatabricksFederationRuntime.materialize_source`` (columnar bulk
write, per-source Unity Catalog matching the compiler's physical name) and executes the governed
Databricks SQL on the warehouse via the Arrow read path, asserting on the returned rows. If governance
were not applied, or the landed name did not match the compiler's, these fail.

Requires a live SQL Warehouse: DATABRICKS_SERVER_HOSTNAME / DATABRICKS_HTTP_PATH / DATABRICKS_TOKEN.
In a TLS-intercepting dev environment, point SSL_CERT_FILE at a CA bundle that includes the proxy CA
(the connector otherwise cannot complete the handshake). Skipped when creds are absent (CI-safe).
"""

from __future__ import annotations

import os

import pytest

pytestmark = [pytest.mark.integration]

pytest.importorskip("databricks.sql", reason="databricks-sql-connector required")

_ENV = ("DATABRICKS_SERVER_HOSTNAME", "DATABRICKS_HTTP_PATH", "DATABRICKS_TOKEN")
_HAVE_CREDS = all(os.environ.get(v) for v in _ENV)
pytestmark.append(
    pytest.mark.skipif(not _HAVE_CREDS, reason="Databricks warehouse creds not set (DATABRICKS_*)")
)

from provisa.compiler.context import build_context  # noqa: E402
from provisa.compiler.introspect import ColumnMetadata  # noqa: E402
from provisa.compiler.parser import parse_query  # noqa: E402
from provisa.compiler.rls import RLSContext  # noqa: E402
from provisa.compiler.schema_gen import SchemaInput, generate_schema  # noqa: E402
from provisa.compiler.sql_gen import compile_query  # noqa: E402
from provisa.compiler.sql_rewrite import rewrite_semantic_to_catalog_physical  # noqa: E402
from provisa.compiler.stage2 import apply_governance, build_governance_context  # noqa: E402
from provisa.core.catalog import _to_catalog_name  # noqa: E402
from provisa.federation.databricks_runtime import DatabricksFederationRuntime  # noqa: E402
from provisa.transpiler.transpile import transpile  # noqa: E402

_ADMIN = {"id": "admin", "capabilities": ["admin"], "domain_access": ["*"]}
_SRC = "e2e-dbx"  # source id → Unity Catalog e2e_dbx (compiler naming: hyphen → underscore)
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
                "governance": "pre-approved",
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
        source_types={_SRC: "databricks"},
    )


def _compile_to_databricks(si: SchemaInput, gql: str, rls: RLSContext | None = None) -> str:
    """Run the real pipeline: compile → (govern) → catalog-physical → transpile(databricks)."""
    schema = generate_schema(si)
    ctx = build_context(si)
    compiled = compile_query(parse_query(schema, gql, {}), ctx)[0]
    sql = compiled.sql
    if rls is not None:
        gov_ctx = build_governance_context("admin", rls, {}, ctx, si.tables, role=_ADMIN)
        sql = apply_governance(sql, gov_ctx)
    return transpile(rewrite_semantic_to_catalog_physical(sql, ctx), "databricks")


@pytest.fixture(scope="module")
def runtime():
    url = (
        f"databricks://token:{os.environ['DATABRICKS_TOKEN']}"
        f"@{os.environ['DATABRICKS_SERVER_HOSTNAME']}"
        f"?http_path={os.environ['DATABRICKS_HTTP_PATH']}"
    )
    rt = DatabricksFederationRuntime(url=url)
    try:
        yield rt
    finally:
        # Clean the e2e catalog so runs are repeatable and nothing is left behind. Drop the whole
        # per-source Unity Catalog (CASCADE takes its schema + table) — dropping only the table would
        # leak the catalog the engine auto-created on first write.
        cat = _to_catalog_name(_SRC)
        cur = rt.connection.cursor()
        try:
            cur.execute(f"DROP CATALOG IF EXISTS `{cat}` CASCADE")
        finally:
            cur.close()
        rt.close()


_ROWS = [
    {"id": 1, "region": "west", "amount": 100.0},
    {"id": 2, "region": "east", "amount": 200.0},
    {"id": 3, "region": "west", "amount": 300.0},
]


async def _materialize(runtime, rows):
    from types import SimpleNamespace

    src = SimpleNamespace(id=_SRC, type="databricks", schema_name=_SCHEMA, table_name=_TABLE)
    cols = [("id", "bigint"), ("region", "text"), ("amount", "double")]
    await runtime.materialize_source(src, cols, rows, change_signal="ttl")


@pytest.mark.asyncio
async def test_databricks_engine_governed_query_round_trip(runtime):
    """Materialize demo rows, then read them back via the governed pipeline's Databricks SQL."""
    await _materialize(runtime, _ROWS)
    sql = _compile_to_databricks(_schema_input(), "{ orders { id region amount } }")
    # The governed physical name must equal where the runtime landed (per-source Unity Catalog).
    assert f"`{_to_catalog_name(_SRC)}`.`{_SCHEMA}`.`{_TABLE}`" in sql
    table = runtime.run_arrow(sql)
    assert table.num_rows == 3
    assert set(table.column_names) == {"id", "region", "amount"}


@pytest.mark.asyncio
async def test_databricks_engine_applies_rls(runtime):
    """An RLS predicate filters rows when executed on the live warehouse (governance not bypassed)."""
    await _materialize(runtime, _ROWS)
    rls = RLSContext(rules={1: "region = 'west'"}, domain_rules={})
    sql = _compile_to_databricks(_schema_input(), "{ orders { id region amount } }", rls=rls)
    assert "WHERE" in sql.upper()  # the predicate made it into the governed SQL
    table = runtime.run_arrow(sql)
    regions = table.column("region").to_pylist()
    assert regions and all(r == "west" for r in regions)  # RLS removed the 'east' row
    assert table.num_rows == 2


_R2 = (
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_ENDPOINT_OVERRIDE",
    "CLOUDFLARE_ACCOUNT_ID",
)
_HAVE_R2 = all(os.environ.get(v) for v in _R2)
_COPY_SRC = (
    "e2e-dbx-copy"  # its own Unity Catalog so the COPY-INTO test never collides with the above
)


@pytest.mark.asyncio
@pytest.mark.skipif(not _HAVE_R2, reason="R2 staging creds not set (bulk COPY INTO path)")
async def test_databricks_bulk_copy_into_lands_large_batch(runtime):
    """A batch >= COPY_INTO_ROW_THRESHOLD lands via the REAL bulk COPY-INTO path (staged Parquet on R2
    → COPY INTO the Delta table), then reads back the exact row count + a sample value on the live
    warehouse. Below-threshold batches take the INSERT path (covered in unit); this pins the bulk seam."""
    from types import SimpleNamespace

    from provisa.federation.databricks_store import COPY_INTO_ROW_THRESHOLD

    bucket = os.environ.get("PROVISA_R2_TEST_BUCKET", "pubs")
    acct = os.environ["CLOUDFLARE_ACCOUNT_ID"]
    os.environ["PROVISA_DATABRICKS_STAGE_URL"] = (
        f"r2://{bucket}@{acct}.r2.cloudflarestorage.com/provisa_stage/"
    )

    n = COPY_INTO_ROW_THRESHOLD + 500  # comfortably above the gate → COPY INTO, not INSERT
    rows = [{"id": i, "region": "west" if i % 2 else "east", "amount": float(i)} for i in range(n)]
    cols = [("id", "bigint"), ("region", "text"), ("amount", "double")]
    src = SimpleNamespace(id=_COPY_SRC, type="databricks", schema_name=_SCHEMA, table_name=_TABLE)
    cat = _to_catalog_name(_COPY_SRC)
    try:
        await runtime.materialize_source(src, cols, rows, change_signal="ttl")
        table = runtime.run_arrow(
            f"SELECT count(*) AS n, max(amount) AS mx FROM `{cat}`.`{_SCHEMA}`.`{_TABLE}`"
        )
        assert table.column("n").to_pylist()[0] == n
        assert table.column("mx").to_pylist()[0] == float(n - 1)
    finally:
        cur = runtime.connection.cursor()
        try:
            cur.execute(f"DROP TABLE IF EXISTS `{cat}`.`{_SCHEMA}`.`{_TABLE}`")
        finally:
            cur.close()
        os.environ.pop("PROVISA_DATABRICKS_STAGE_URL", None)


if __name__ == "__main__":
    pytest.main([__file__, "-q"])
