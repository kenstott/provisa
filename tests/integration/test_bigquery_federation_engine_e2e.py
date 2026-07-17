# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""E2E: BigQuery as a federation engine through the REAL query pipeline + a zero-copy GCS external link.

Drives the actual Provisa primitives (compile → govern → catalog-physical → transpile(bigquery)),
MATERIALIZES demo rows into a per-source BigQuery dataset via ``materialize_source`` (columnar load
job), reads them back via the Arrow path (Storage Read API), and asserts RLS filters on the live
warehouse. A second test seeds a Parquet file on GCS and drives the ATTACH connector's
``attach_source`` — a BigQuery EXTERNAL TABLE over the GCS object (``table_type = EXTERNAL``), read
zero-copy. Skipped without GCP creds (GOOGLE_CLOUD_PROJECT + GOOGLE_APPLICATION_CREDENTIALS)."""

from __future__ import annotations

import os

import pytest

pytestmark = [pytest.mark.integration]

pytest.importorskip("google.cloud.bigquery", reason="google-cloud-bigquery required")

_HAVE = bool(
    os.environ.get("GOOGLE_CLOUD_PROJECT") and os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
)
pytestmark.append(
    pytest.mark.skipif(not _HAVE, reason="GCP creds not set (GOOGLE_CLOUD_PROJECT/…)")
)

from provisa.compiler.context import build_context  # noqa: E402
from provisa.compiler.introspect import ColumnMetadata  # noqa: E402
from provisa.compiler.parser import parse_query  # noqa: E402
from provisa.compiler.rls import RLSContext  # noqa: E402
from provisa.compiler.schema_gen import SchemaInput, generate_schema  # noqa: E402
from provisa.compiler.sql_gen import compile_query  # noqa: E402
from provisa.compiler.sql_rewrite import rewrite_semantic_to_catalog_physical  # noqa: E402
from provisa.compiler.stage2 import apply_governance, build_governance_context  # noqa: E402
from provisa.federation.bigquery_runtime import BigQueryFederationRuntime  # noqa: E402
from provisa.transpiler.transpile import transpile  # noqa: E402

_PROJ = os.environ.get("GOOGLE_CLOUD_PROJECT", "")
_ADMIN = {"id": "admin", "capabilities": ["admin"], "domain_access": ["*"]}
_SRC = "bq-e2e"
_DS = "provisa_e2e_it"


def _col(n, d="varchar", nl=True):
    return ColumnMetadata(column_name=n, data_type=d, is_nullable=nl)


def _si():
    return SchemaInput(
        tables=[
            {
                "id": 1,
                "source_id": _SRC,
                "domain_id": "s",
                "schema_name": _DS,
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
        source_types={_SRC: "bigquery"},
        source_catalogs={_SRC: _PROJ},  # BigQuery: catalog is the project (project.dataset.table)
    )


def _compile(rls=None):
    s = _si()
    ctx = build_context(s)
    compiled = compile_query(
        parse_query(generate_schema(s), "{ orders { id region amount } }", {}), ctx
    )[0]
    sql = compiled.sql
    if rls is not None:
        sql = apply_governance(
            sql, build_governance_context("admin", rls, {}, ctx, s.tables, role=_ADMIN)
        )
    return transpile(rewrite_semantic_to_catalog_physical(sql, ctx), "bigquery")


@pytest.fixture(scope="module")
def runtime():
    rt = BigQueryFederationRuntime(url=f"bigquery://{_PROJ}?location=US")
    try:
        yield rt
    finally:
        # Drop the whole per-source datasets (CASCADE takes their tables) — dropping only the
        # table would leak the dataset materialize_source auto-created. Covers both the landed
        # dataset and the external-link dataset.
        for ds in (_DS, "provisa_ext_it_ds"):
            rt.connection.query(f"DROP SCHEMA IF EXISTS `{_PROJ}`.`{ds}` CASCADE").result()
        rt.close()


@pytest.mark.asyncio
async def test_bigquery_engine_land_govern_read_and_rls(runtime):
    from types import SimpleNamespace

    from provisa.core.models import SourceType

    src = SimpleNamespace(id=_SRC, type=SourceType.bigquery, schema_name=_DS, table_name="orders")
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
    assert f"`{_PROJ}`.`{_DS}`.`orders`" in sql  # governed name == where the runtime landed
    table = runtime.run_arrow(sql)
    assert table.num_rows == 3

    rls = runtime.run_arrow(_compile(RLSContext(rules={1: "region = 'west'"}, domain_rules={})))
    assert rls.num_rows == 2  # RLS removed the 'east' row on the live warehouse
    assert set(rls.column("region").to_pylist()) == {"west"}


@pytest.mark.asyncio
async def test_bigquery_gcs_external_link_is_zero_copy(runtime):
    import io

    import pyarrow as pa
    import pyarrow.parquet as pq
    from types import SimpleNamespace

    from google.cloud import storage
    from provisa.core.models import SourceType

    bucket = os.environ.get("BIGQUERY_TEST_GCS_BUCKET")
    if not bucket:
        pytest.skip("BIGQUERY_TEST_GCS_BUCKET not set")
    buf = io.BytesIO()
    pq.write_table(
        pa.table(
            {"id": [1, 2, 3], "region": ["west", "east", "west"], "amount": [10.0, 20.0, 30.0]}
        ),
        buf,
    )
    blob = storage.Client(project=_PROJ).bucket(bucket).blob("provisa_ext_it/orders.parquet")
    blob.upload_from_string(buf.getvalue())
    uri = f"gs://{bucket}/provisa_ext_it/orders.parquet"
    ext_ds = "provisa_ext_it_ds"
    try:
        src = SimpleNamespace(
            id="bq-ext",
            type=SourceType.parquet,
            schema_name=ext_ds,
            table_name="orders",
            path=uri,
            federation_hints={},
        )
        runtime.attach_source(src)  # creates a BigQuery EXTERNAL TABLE over the GCS parquet
        table = runtime.run_arrow(
            f"SELECT id, region, amount FROM `{_PROJ}`.`{ext_ds}`.`orders` ORDER BY id"
        )
        assert table.num_rows == 3
        rows = list(
            runtime.connection.query(
                f"SELECT table_type FROM `{_PROJ}`.`{ext_ds}`.INFORMATION_SCHEMA.TABLES WHERE table_name='orders'"
            ).result()
        )
        assert rows and rows[0][0] == "EXTERNAL"  # a link, not a copy
    finally:
        runtime.connection.query(
            f"DROP EXTERNAL TABLE IF EXISTS `{_PROJ}`.`{ext_ds}`.`orders`"
        ).result()
        blob.delete()


if __name__ == "__main__":
    pytest.main([__file__, "-q"])
