# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""E2E: Databricks external-data-link over object storage — zero-copy SCAN (REQ-987).

Seeds a Parquet file on Cloudflare R2 (S3-compatible), then drives the Databricks ATTACH connector's
own ``attach_source``: it INSTALLS + VALIDATES the Unity Catalog storage credential + external
location via the UC REST API, creates a Databricks EXTERNAL TABLE (``USING PARQUET LOCATION 'r2://…'``)
at the compiler's physical name, and reads it back via Arrow. Asserts the table is ``EXTERNAL`` (a
link, not a copy). Skipped without Databricks + R2 creds; in a TLS-intercepting dev environment set
SSL_CERT_FILE at a CA bundle incl. the proxy CA.
"""

from __future__ import annotations

import io
import os

import pytest

pytestmark = [pytest.mark.integration]

pytest.importorskip("databricks.sql", reason="databricks-sql-connector required")
pytest.importorskip("boto3", reason="boto3 required to seed R2")

_DBX = ("DATABRICKS_SERVER_HOSTNAME", "DATABRICKS_HTTP_PATH", "DATABRICKS_TOKEN")
_R2 = (
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_ENDPOINT_OVERRIDE",
    "CLOUDFLARE_ACCOUNT_ID",
)
_HAVE = all(os.environ.get(v) for v in (*_DBX, *_R2))
pytestmark.append(pytest.mark.skipif(not _HAVE, reason="Databricks + R2 creds not set"))

from provisa.core.catalog import _to_catalog_name  # noqa: E402
from provisa.federation.databricks_runtime import DatabricksFederationRuntime  # noqa: E402

_BUCKET = os.environ.get("PROVISA_R2_TEST_BUCKET", "pubs")
_KEY = "provisa_ext_link_e2e/orders.parquet"
_SID = "r2-extlink-e2e"


def _s3():
    import boto3

    return boto3.client(
        "s3",
        endpoint_url=os.environ["AWS_ENDPOINT_OVERRIDE"],
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        region_name="auto",
    )


@pytest.fixture
def r2_parquet_url():
    import pyarrow as pa
    import pyarrow.parquet as pq

    s3 = _s3()
    t = pa.table(
        {"id": [1, 2, 3], "region": ["west", "east", "west"], "amount": [100.0, 200.0, 300.0]}
    )
    buf = io.BytesIO()
    pq.write_table(t, buf)
    s3.put_object(Bucket=_BUCKET, Key=_KEY, Body=buf.getvalue())
    acct = os.environ["CLOUDFLARE_ACCOUNT_ID"]
    url = f"r2://{_BUCKET}@{acct}.r2.cloudflarestorage.com/{_KEY}"
    try:
        yield url
    finally:
        s3.delete_object(Bucket=_BUCKET, Key=_KEY)


@pytest.fixture
def runtime():
    url = (
        f"databricks://token:{os.environ['DATABRICKS_TOKEN']}"
        f"@{os.environ['DATABRICKS_SERVER_HOSTNAME']}?http_path={os.environ['DATABRICKS_HTTP_PATH']}"
    )
    rt = DatabricksFederationRuntime(url=url)
    try:
        yield rt
    finally:
        cat = _to_catalog_name(_SID)
        cur = rt.connection.cursor()
        try:
            cur.execute(f"DROP TABLE IF EXISTS `{cat}`.`public`.`orders`")
        finally:
            cur.close()
        rt.close()


def test_databricks_external_link_over_r2_is_zero_copy(runtime, r2_parquet_url):
    from types import SimpleNamespace

    from provisa.core.models import SourceType

    src = SimpleNamespace(
        id=_SID,
        type=SourceType.parquet,
        schema_name="public",
        table_name="orders",
        path=r2_parquet_url,
        federation_hints={
            "access_key_id": os.environ["AWS_ACCESS_KEY_ID"],
            "secret_access_key": os.environ["AWS_SECRET_ACCESS_KEY"],
            "account_id": os.environ["CLOUDFLARE_ACCOUNT_ID"],
        },
    )
    # The connector installs+validates UC creds and creates the external table (no landing).
    runtime.attach_source(src)

    cat = _to_catalog_name(_SID)
    table = runtime.run_arrow(
        f"SELECT id, region, amount FROM `{cat}`.`public`.`orders` ORDER BY id"
    )
    assert table.num_rows == 3
    assert table.column("region").to_pylist() == ["west", "east", "west"]

    # Prove it's an external LINK, not a copied-in table.
    cur = runtime.connection.cursor()
    try:
        cur.execute(f"DESCRIBE TABLE EXTENDED `{cat}`.`public`.`orders`")
        meta = {r[0]: r[1] for r in cur.fetchall()}
    finally:
        cur.close()
    assert meta.get("Type") == "EXTERNAL"
    assert meta.get("Provider") == "parquet"


if __name__ == "__main__":
    pytest.main([__file__, "-q"])
