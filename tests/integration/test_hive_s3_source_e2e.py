# Copyright (c) 2026 Kenneth Stott
# Canary: 5f2a8d13-6c47-4e90-a1b2-9d3e6f04c7a8
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""E2E: S3-backed Hive (hive_s3) as a connector-source, read through the Provisa federation engine
(REQ-1097).

Same catalog seam as test_hive_source_e2e.py, but the table data lives on S3 (MinIO) instead of a
local warehouse. ``hive_s3`` maps to Trino's ``hive`` connector; TrinoHiveS3Connector emits
hive.metastore.uri PLUS the native S3 filesystem config (fs.native-s3.enabled + s3.endpoint/
credentials/path-style/region) taken from the source mapping.

hive_s3 specifics
-----------------
- The compose service ``hive-s3-metastore`` is a Thrift metastore whose warehouse is
  s3a://provisa-hive-s3/warehouse; it is given the bundled hadoop-aws + aws-sdk jars and the S3A
  settings so it can create the database dir on MinIO. Trino reads the same data through its NATIVE S3
  filesystem, configured from the Source mapping.
- MinIO is the CORE ``minio`` service (reused), reached in-network at minio:9000. The test creates the
  ``provisa-hive-s3`` bucket via the host-published MinIO port before seeding (S3A writes keys, it
  does not create buckets). Seeding is done THROUGH Trino (CREATE TABLE / INSERT) — the hive connector
  writes Parquet to s3a:// and that is itself the federation-engine write path.
"""

from __future__ import annotations

import os
import time

import pytest
import trino.dbapi
import trino.exceptions

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

_TRINO_HOST = os.environ.get("TRINO_HOST", "localhost")
_TRINO_PORT = int(os.environ.get("TRINO_PORT", "8080"))

_MINIO_HOST = os.environ.get("MINIO_HOST", "localhost")
_MINIO_PORT = int(os.environ.get("MINIO_PORT", "9000"))
_BUCKET = "provisa-hive-s3"

# The S3 config Provisa's hive_s3 connector forwards to Trino's native S3 filesystem. endpoint uses
# the compose service name (minio:9000) — Trino resolves it inside its own container, NOT the
# host-published ${MINIO_PORT} the test process itself uses to create the bucket.
_S3_MAPPING = {
    "endpoint": "http://minio:9000",
    "access_key_id": "minioadmin",
    "secret_access_key": "minioadmin",
    "region": "us-east-1",
}

_SCHEMA = "wh"
_TABLE = "widgets"
_WIDGETS = [(1, "Widget A"), (2, "Widget B"), (3, "Widget C")]


@pytest.fixture(scope="module", autouse=True)
def _wait_for_trino():
    """Wait for Trino to finish initializing before running Trino tests."""
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        try:
            conn = trino.dbapi.connect(
                host=_TRINO_HOST, port=_TRINO_PORT, user="itest", catalog="system"
            )
            cur = conn.cursor()
            cur.execute("SELECT 1")
            cur.fetchall()
            conn.close()
            return
        except Exception:
            time.sleep(2)
    raise RuntimeError("Trino did not become ready within 120s")


def _trino_cursor():
    conn = trino.dbapi.connect(host=_TRINO_HOST, port=_TRINO_PORT, user="itest", catalog="system")
    cur = conn.cursor()
    cur.execute("SELECT 1")
    cur.fetchall()
    return conn, cur


def _drop(cur, name):
    try:
        cur.execute(f"DROP CATALOG {name}")
        cur.fetchall()
    except Exception:
        pass


def _exec(cur, sql: str) -> None:
    cur.execute(sql)
    cur.fetchall()


def _ensure_bucket() -> None:
    """Create the s3a warehouse bucket on MinIO (host-published port). S3A writes keys under an
    EXISTING bucket; it never creates the bucket itself, so the test must."""
    import boto3
    from botocore.client import Config as BotoConfig

    s3 = boto3.client(
        "s3",
        endpoint_url=f"http://{_MINIO_HOST}:{_MINIO_PORT}",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
        region_name="us-east-1",
        config=BotoConfig(signature_version="s3v4", s3={"addressing_style": "path"}),
    )
    existing = {b["Name"] for b in s3.list_buckets().get("Buckets", [])}
    if _BUCKET not in existing:
        s3.create_bucket(Bucket=_BUCKET)


def _seed_hive_s3(cur, catalog: str) -> None:
    """Create the schema + widgets table on s3a:// and insert rows THROUGH Trino's hive connector."""
    deadline = time.monotonic() + 90
    last_exc: Exception | None = None
    while time.monotonic() < deadline:
        try:
            _exec(cur, f"CREATE SCHEMA IF NOT EXISTS {catalog}.{_SCHEMA}")
            break
        except trino.exceptions.TrinoQueryError as exc:
            last_exc = exc
            time.sleep(3)
    else:
        raise RuntimeError(f"hive_s3 CREATE SCHEMA never succeeded: {last_exc!r}")

    _exec(cur, f"DROP TABLE IF EXISTS {catalog}.{_SCHEMA}.{_TABLE}")
    _exec(
        cur,
        f"CREATE TABLE {catalog}.{_SCHEMA}.{_TABLE} (id integer, name varchar) "
        "WITH (format = 'PARQUET')",
    )
    values = ", ".join(f"({wid}, '{name}')" for wid, name in _WIDGETS)
    _exec(cur, f"INSERT INTO {catalog}.{_SCHEMA}.{_TABLE} (id, name) VALUES {values}")


@pytest.mark.requires_hive_s3
async def test_hive_s3_catalog_created_and_queryable():
    """Register a hive_s3 Source, project it as a live Trino catalog backed by S3, seed + query it.

    Drives the REAL registration path: create_catalog builds the catalog from
    TrinoHiveS3Connector.details() (hive.metastore.uri + fs.native-s3.enabled + s3.* from the source
    mapping — REQ-1097) and issues CREATE CATALOG against the live Trino coordinator. host=
    "hive-s3-metastore" is the compose service name; the S3 endpoint (minio:9000) is likewise the
    in-network service name — both resolved inside Trino's container.
    """
    pytest.importorskip("trino")
    pytest.importorskip("boto3")
    from provisa.core.catalog import create_catalog
    from provisa.core.models import Source, SourceType

    _ensure_bucket()

    conn, cur = _trino_cursor()

    catalog = "hive_s3_itest"
    _drop(cur, catalog)
    src = Source(
        id="hive-s3-itest",
        type=SourceType.hive_s3,
        host="hive-s3-metastore",
        port=9083,
        mapping=dict(_S3_MAPPING),
    )
    try:
        create_catalog(conn, src, "")

        _seed_hive_s3(cur, catalog)

        cur.execute(f"SHOW SCHEMAS FROM {catalog}")
        schemas = {r[0] for r in cur.fetchall()}
        assert _SCHEMA in schemas

        cur.execute(f"SHOW TABLES FROM {catalog}.{_SCHEMA}")
        tables = {r[0] for r in cur.fetchall()}
        assert _TABLE in tables

        # Querying <catalog>.<schema>.<table> through Trino IS reading through the federation engine —
        # Trino's hive connector reads the Parquet files on MinIO via its native S3 filesystem.
        cur.execute(f"SELECT id, name FROM {catalog}.{_SCHEMA}.{_TABLE} ORDER BY id")
        rows = cur.fetchall()
        assert sorted((r[0], r[1]) for r in rows) == _WIDGETS
    finally:
        _drop(cur, catalog)
        conn.close()
