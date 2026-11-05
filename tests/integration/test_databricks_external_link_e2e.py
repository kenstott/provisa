# Copyright (c) 2026 Kenneth Stott
# Canary: c725246f-afb1-4416-bc4a-717df0231acf
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

from tests.env_creds import unset

pytestmark = [pytest.mark.integration, pytest.mark.requires_warehouse]

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
pytestmark.append(pytest.mark.skipif(not _HAVE, reason=f"not set: {unset(*_DBX, *_R2)}"))

from provisa.core.catalog import _to_catalog_name  # noqa: E402
from provisa.federation.databricks_runtime import DatabricksFederationRuntime  # noqa: E402
from tests.integration import databricks_uc, r2_objects  # noqa: E402
from tests.integration.databricks_warehouse import warehouse_running  # noqa: E402
from tests.integration.live_teardown import run_every, run_name  # noqa: E402


@pytest.fixture(scope="module", autouse=True)
def _warehouse_awake():
    """A suspended serverless warehouse rejects OpenSession outright -- wake it before anything in
    this module connects, and stop it again after the module however it ends, if this module
    started it (REQ-254). Module-scoped autouse: set up before, and torn down after, every other
    fixture of the module, so the stop never cuts a connection still in use."""
    with warehouse_running():
        yield


# Every name the module creates carries the run's own suffix (REQ-254): the source id (so its
# per-source Unity Catalog), and the R2 prefix (so the external location over it).
_SID = run_name("r2-extlink-e2e", "-")
_PREFIX = run_name("provisa_ext_link_e2e") + "/"
_KEY = f"{_PREFIX}orders.parquet"


def _r2_root() -> str:
    return f"r2://{r2_objects.bucket()}@{os.environ['CLOUDFLARE_ACCOUNT_ID']}.r2.cloudflarestorage.com/"


@pytest.fixture
def r2_parquet_url():
    """The run's parquet on R2. After the test: the run's catalog (its external table with it),
    the external locations over the run's prefix, the bucket's storage credential if this run
    created it, and the run's R2 objects -- every step attempted and read back (REQ-254)."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    bucket = r2_objects.bucket()
    credential_existed = databricks_uc.credential_exists(bucket)
    try:
        t = pa.table(
            {"id": [1, 2, 3], "region": ["west", "east", "west"], "amount": [100.0, 200.0, 300.0]}
        )
        buf = io.BytesIO()
        pq.write_table(t, buf)
        r2_objects.client().put_object(Bucket=bucket, Key=_KEY, Body=buf.getvalue())
        yield f"{_r2_root()}{_KEY}"
    finally:
        run_every(
            [
                (
                    f"drop catalog {_to_catalog_name(_SID)}",
                    lambda: _with_connection(
                        lambda conn: databricks_uc.drop_catalog(conn, _to_catalog_name(_SID))
                    ),
                ),
                (
                    f"delete external locations under {_r2_root()}{_PREFIX}",
                    lambda: databricks_uc.remove_locations_under(f"{_r2_root()}{_PREFIX}"),
                ),
                (
                    f"delete storage credential for {bucket} if this run created it",
                    lambda: databricks_uc.remove_credential(bucket, existed=credential_existed),
                ),
                (f"delete R2 objects under {_PREFIX}", lambda: r2_objects.remove_prefix(_PREFIX)),
            ]
        )


def _connect():
    from databricks import sql as dbsql

    from provisa.federation.databricks_tls import databricks_tls_kwargs

    return dbsql.connect(
        server_hostname=os.environ["DATABRICKS_SERVER_HOSTNAME"],
        http_path=os.environ["DATABRICKS_HTTP_PATH"],
        access_token=os.environ["DATABRICKS_TOKEN"],
        **databricks_tls_kwargs(),
    )


def _with_connection(work) -> None:
    conn = _connect()
    try:
        work(conn)
    finally:
        conn.close()


@pytest.fixture
def runtime(r2_parquet_url):  # noqa: ARG001 -- torn down before the R2 objects and UC objects
    url = (
        f"databricks://token:{os.environ['DATABRICKS_TOKEN']}"
        f"@{os.environ['DATABRICKS_SERVER_HOSTNAME']}?http_path={os.environ['DATABRICKS_HTTP_PATH']}"
    )
    rt = DatabricksFederationRuntime(url=url)
    try:
        yield rt
    finally:
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
