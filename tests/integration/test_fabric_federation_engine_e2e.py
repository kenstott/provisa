# Copyright (c) 2026 Kenneth Stott
# Canary: fde72626-cfee-4eee-995c-e96c830bbf22
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""E2E: Microsoft Fabric Warehouse as a federation engine through the REAL query pipeline.

Drives the actual Provisa primitives (compile → govern → catalog-physical → transpile(tsql)),
MATERIALIZES demo rows into a per-source schema via ``materialize_source`` (T-SQL bulk insert), reads
them back via the Arrow path, and asserts RLS on the live warehouse. A second test drives the ATTACH
connector's ``attach_source`` for an S3-compatible (Cloudflare R2) source — which AUTO-PROVISIONS the
whole external-data chain (an ``AmazonS3Compatible`` connection + a lakehouse + a OneLake shortcut via
the Fabric REST API) and reads the R2 parquet zero-copy via OPENROWSET. Auth is Azure AD (``az login``).

Skipped without FABRIC_SQL_SERVER / FABRIC_DATABASE (and, for the external link, R2 creds +
FABRIC_WORKSPACE_ID). pyodbc + the Microsoft ODBC driver (PROVISA_MSSQL_ODBC_DRIVER) must be present."""

from __future__ import annotations

import os

import pytest

pytestmark = [pytest.mark.integration]

pytest.importorskip("pyodbc", reason="pyodbc required")
pytest.importorskip("azure.identity", reason="azure-identity required")

_HAVE = bool(os.environ.get("FABRIC_SQL_SERVER") and os.environ.get("FABRIC_DATABASE"))
pytestmark.append(
    pytest.mark.skipif(not _HAVE, reason="Fabric creds not set (FABRIC_SQL_SERVER/…)")
)

from provisa.compiler.context import build_context  # noqa: E402
from provisa.compiler.introspect import ColumnMetadata  # noqa: E402
from provisa.compiler.parser import parse_query  # noqa: E402
from provisa.compiler.rls import RLSContext  # noqa: E402
from provisa.compiler.schema_gen import SchemaInput, generate_schema  # noqa: E402
from provisa.compiler.sql_gen import compile_query  # noqa: E402
from provisa.compiler.sql_rewrite import rewrite_semantic_to_catalog_physical  # noqa: E402
from provisa.compiler.stage2 import apply_governance, build_governance_context  # noqa: E402
from provisa.federation.mssql_warehouse_runtime import MssqlWarehouseRuntime  # noqa: E402
from provisa.transpiler.transpile import transpile  # noqa: E402

_DB = os.environ.get("FABRIC_DATABASE", "")
_ADMIN = {"id": "admin", "capabilities": ["admin"], "domain_access": ["*"]}
_SRC = "fab-e2e"
_SCH = "provisa_e2e_it"


def _col(n, d="varchar", nl=True):
    return ColumnMetadata(column_name=n, data_type=d, is_nullable=nl)


def _si(schema=_SCH):
    return SchemaInput(
        tables=[
            {
                "id": 1,
                "source_id": _SRC,
                "domain_id": "s",
                "schema_name": schema,
                "table_name": "orders",
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
        domains=[{"id": "s", "description": "S"}],
        source_types={_SRC: "fabric"},
        source_catalogs={_SRC: _DB},  # T-SQL: catalog is the warehouse database
    )


def _compile(rls=None, schema=_SCH):
    s = _si(schema)
    ctx = build_context(s)
    compiled = compile_query(
        parse_query(generate_schema(s), "{ orders { id region amount } }", {}), ctx
    )[0]
    sql = compiled.sql
    if rls is not None:
        sql = apply_governance(
            sql, build_governance_context("admin", rls, {}, ctx, s.tables, role=_ADMIN)
        )
    return transpile(rewrite_semantic_to_catalog_physical(sql, ctx), "tsql")


@pytest.fixture(scope="module")
def runtime():
    rt = MssqlWarehouseRuntime(
        server=os.environ["FABRIC_SQL_SERVER"], database=_DB, engine_name="fabric"
    )
    try:
        yield rt
    finally:
        # Drop the object then the schema materialize_source/attach auto-created — dropping only
        # the object would leak the schema. T-SQL requires an empty schema before DROP SCHEMA.
        for sch, obj in ((_SCH, "TABLE"), ("provisa_ext_it", "VIEW")):
            cur = rt.connection.cursor()
            try:
                cur.execute(f"DROP {obj} IF EXISTS [{sch}].[orders]")
                cur.execute(f"DROP SCHEMA IF EXISTS [{sch}]")
                rt.connection.commit()
            except Exception:  # noqa: BLE001 — best-effort teardown
                pass
            finally:
                cur.close()
        rt.close()


@pytest.mark.asyncio
async def test_fabric_engine_land_govern_read_and_rls(runtime):
    from types import SimpleNamespace

    from provisa.core.models import SourceType

    src = SimpleNamespace(id=_SRC, type=SourceType.parquet, schema_name=_SCH, table_name="orders")
    await runtime.materialize_source(
        src,
        [("id", "bigint"), ("region", "text"), ("amount", "double")],
        [
            {"id": 1, "region": "west", "amount": 100.0},
            {"id": 2, "region": "east", "amount": 200.0},
            {"id": 3, "region": "west", "amount": 300.0},
        ],
        change_signal="ttl",
    )
    sql = _compile()
    assert f"[{_DB}].[{_SCH}].[orders]" in sql  # governed name == where the runtime landed
    assert runtime.run_arrow(sql).num_rows == 3

    rls = runtime.run_arrow(_compile(RLSContext(rules={1: "region = 'west'"}, domain_rules={})))
    assert rls.num_rows == 2 and set(rls.column("region").to_pylist()) == {"west"}


@pytest.mark.asyncio
async def test_fabric_r2_external_link_autoprovisions_shortcut(runtime):
    import io

    from types import SimpleNamespace

    r2 = (
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "CLOUDFLARE_ACCOUNT_ID",
        "FABRIC_WORKSPACE_ID",
    )
    if not all(os.environ.get(v) for v in r2):
        pytest.skip("R2 creds / FABRIC_WORKSPACE_ID not set")
    boto3 = pytest.importorskip("boto3")
    import pyarrow as pa
    import pyarrow.parquet as pq

    from provisa.core.models import SourceType

    bucket = os.environ.get("PROVISA_R2_TEST_BUCKET", "pubs")
    key = "provisa_ext_link_test/orders.parquet"
    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ["AWS_ENDPOINT_OVERRIDE"],
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        region_name="auto",
    )
    buf = io.BytesIO()
    pq.write_table(
        pa.table(
            {"id": [1, 2, 3], "region": ["west", "east", "west"], "amount": [10.0, 20.0, 30.0]}
        ),
        buf,
    )
    s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue())

    acct = os.environ["CLOUDFLARE_ACCOUNT_ID"]
    src = SimpleNamespace(
        id="fab-r2",
        type=SourceType.parquet,
        schema_name="provisa_ext_it",
        table_name="orders",
        path=f"s3://{bucket}/provisa_ext_link_test/orders.parquet",
        federation_hints={
            "access_key_id": os.environ["AWS_ACCESS_KEY_ID"],
            "secret_access_key": os.environ["AWS_SECRET_ACCESS_KEY"],
            "endpoint": f"https://{acct}.r2.cloudflarestorage.com",
        },
    )
    # ONE call auto-provisions: S3-compatible connection + lakehouse + OneLake shortcut + view + validate
    runtime.attach_source(src)
    t = runtime.run_arrow("SELECT id, region, amount FROM [provisa_ext_it].[orders] ORDER BY id")
    assert t.num_rows == 3
    assert t.column("region").to_pylist() == ["west", "east", "west"]


if __name__ == "__main__":
    pytest.main([__file__, "-q"])
