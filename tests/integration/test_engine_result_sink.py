# Copyright (c) 2026 Kenneth Stott
# Canary: 2d858512-f782-4aef-9c8e-d7bde315b1cd
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1194 against a real object store: an engine other than Trino runs a statement with its
bound values and writes the result to the results bucket itself; what comes back is the
result's address and its row count, and the presigned link returns the rows as Parquet.

Proven here for the engines that can reach a store this module starts (a MinIO container on a
leased port): DuckDB, through httpfs, and ClickHouse, embedded (chdb), through its ``s3``
table function. Snowflake and Databricks write only to a bucket their own cloud can reach;
their statements are tested in tests/unit/test_result_sink.py and are not run here.

Everything here is the ``test`` instance: one MinIO container this module starts and removes."""

from __future__ import annotations

import io
import os
import subprocess
import time
from types import SimpleNamespace

import httpx
import pytest

pytestmark = [pytest.mark.integration]

_KEY, _SECRET, _BUCKET = "provisa-test", "provisa-test-secret", "provisa-results"


@pytest.fixture(scope="module")
def store():
    """A MinIO container, and the redirect settings that point the deployment at it."""
    import boto3
    from botocore.config import Config as BotoConfig

    from tests.port_lease import lease_ports

    (port,) = lease_ports(1)
    name = f"provisa-itest-sink-minio-{os.getpid()}"
    subprocess.run(
        ["docker", "run", "-d", "--rm", "--memory", "512m", "--name", name]
        + ["-e", f"MINIO_ROOT_USER={_KEY}", "-e", f"MINIO_ROOT_PASSWORD={_SECRET}"]
        # The image the core stack runs (docker-compose.core.yml): MinIO's own repositories refuse
        # anonymous pulls, and bitnamilegacy/minio keeps its data under /bitnami/minio/data.
        + ["-p", f"127.0.0.1:{port}:9000", "bitnamilegacy/minio:latest"]
        + ["server", "/bitnami/minio/data"],
        check=True,
        capture_output=True,
    )
    endpoint = f"http://127.0.0.1:{port}"
    settings = {
        "PROVISA_REDIRECT_ENDPOINT": endpoint,
        "PROVISA_REDIRECT_BUCKET": _BUCKET,
        "PROVISA_REDIRECT_ACCESS_KEY": _KEY,
        "PROVISA_REDIRECT_SECRET_KEY": _SECRET,
        "PROVISA_REDIRECT_REGION": "us-east-1",
    }
    before = {k: os.environ.get(k) for k in settings}
    os.environ.update(settings)
    try:
        client = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=_KEY,
            aws_secret_access_key=_SECRET,
            region_name="us-east-1",
            config=BotoConfig(signature_version="s3v4"),
        )
        deadline = time.monotonic() + 60
        while True:
            try:
                client.list_buckets()
                break
            except Exception:  # the container is still starting
                if time.monotonic() > deadline:
                    raise
                time.sleep(1)
        yield client
    finally:
        for key, value in before.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        subprocess.run(["docker", "rm", "-f", name], capture_output=True)


def _state() -> SimpleNamespace:
    """Nothing registered: the statement reads only what it computes itself."""
    return SimpleNamespace(config=None)


async def _rows_at(result: dict) -> list[dict]:
    """The rows behind the presigned link the caller is redirected to."""
    import pyarrow.parquet as pq

    from provisa.executor.redirect import RedirectConfig, presign_ctas_result

    url = await presign_ctas_result(result["s3_prefix"], RedirectConfig.from_env())
    body = httpx.get(url, timeout=30)
    assert body.status_code == 200, body.text
    return pq.read_table(io.BytesIO(body.content)).to_pylist()


async def test_duckdb_writes_the_result_with_its_bound_values(store):
    from provisa.federation.engine import build_engine

    backend = build_engine("duckdb").backend
    result = backend.ctas_redirect(
        _state(), "SELECT range AS id FROM range(10) WHERE range >= ? ORDER BY 1", "parquet", [7]
    )
    assert result["row_count"] == 3
    assert result["s3_prefix"].startswith(f"s3a://{_BUCKET}/results/")
    assert await _rows_at(result) == [{"id": 7}, {"id": 8}, {"id": 9}]
    listed = store.list_objects_v2(Bucket=_BUCKET, Prefix="results/")["Contents"]
    assert any(o["Key"].endswith("data.parquet") for o in listed)


async def test_duckdb_refuses_a_format_it_does_not_write(store):
    from provisa.federation.engine import build_engine
    from provisa.federation.result_sink import ResultFormatNotWritten

    with pytest.raises(ResultFormatNotWritten, match="does not write 'orc'"):
        build_engine("duckdb").backend.ctas_redirect(_state(), "SELECT 1", "orc", None)


async def test_clickhouse_writes_the_result_with_its_bound_values(store, monkeypatch):
    from provisa.federation.engine import build_engine

    # No engine URL: the embedded ClickHouse (chdb), in this process.
    monkeypatch.setattr("provisa.federation.engine.configured_engine_url", lambda: None)
    backend = build_engine("clickhouse").backend
    result = backend.ctas_redirect(
        _state(),
        "SELECT number AS id FROM numbers(10) WHERE number >= $1 ORDER BY 1",
        "parquet",
        [7],
    )
    assert result["row_count"] == 3
    assert await _rows_at(result) == [{"id": 7}, {"id": 8}, {"id": 9}]
