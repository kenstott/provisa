# Copyright (c) 2026 Kenneth Stott
# Canary: 6b2e9f14-8d37-4c51-a0e6-3f7b1d8c5a92
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A forced redirect, on every transport, on a real server (REQ-1194, REQ-1224 amended
2026-10-04).

One real server (DuckDB engine) over the stack's Postgres, with a MinIO container of this module's
own as the results store. Each transport asks for its result to be delivered there instead of
answered inline, in its own side-channel; the answer is the delivery's handle, and the presigned
link it names returns the rows as Parquet -- the rows the same read answers inline.

Lands on the TEST instance only: a database the harness creates, a MinIO container on a leased
port, both removed at the end."""

from __future__ import annotations

import io
import json
import os
import subprocess
import time
import urllib.error
import urllib.request

import httpx
import pytest

from tests.integration.worker_boot_harness import WorkerBoot

pytestmark = [pytest.mark.integration]

_PG_HOST = os.environ.get("PG_HOST", "localhost")
_PG_PORT = int(os.environ.get("PG_PORT", "5432"))
_KEY, _SECRET, _BUCKET = "provisa-test", "provisa-test-secret", "provisa-results"
_ROLE = "org_admin"
_IDS = {1, 2}  # the harness seeds two orders
_FORCE = {"X-Provisa-Redirect": "true", "X-Provisa-Redirect-Format": "parquet"}


@pytest.fixture(scope="module")
def results_store():
    """A MinIO container on a leased port, with the bucket the deliveries land in."""
    import boto3
    from botocore.config import Config as BotoConfig

    from tests.port_lease import lease_ports

    (port,) = lease_ports(1)
    name = f"provisa-itest-redirect-minio-{os.getpid()}"
    subprocess.run(
        ["docker", "run", "-d", "--rm", "--memory", "512m", "--name", name]
        + ["-e", f"MINIO_ROOT_USER={_KEY}", "-e", f"MINIO_ROOT_PASSWORD={_SECRET}"]
        + ["-p", f"127.0.0.1:{port}:9000", "minio/minio", "server", "/data"],
        check=True,
        capture_output=True,
    )
    endpoint = f"http://127.0.0.1:{port}"
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
            except Exception:  # noqa: BLE001 — the container is still starting
                assert time.monotonic() < deadline, "MinIO did not come up within 60s"
                time.sleep(1)
        client.create_bucket(Bucket=_BUCKET)
        yield {
            "PROVISA_REDIRECT_ENDPOINT": endpoint,
            "PROVISA_REDIRECT_BUCKET": _BUCKET,
            "PROVISA_REDIRECT_ACCESS_KEY": _KEY,
            "PROVISA_REDIRECT_SECRET_KEY": _SECRET,
            "PROVISA_REDIRECT_REGION": "us-east-1",
            # Forced deliveries only: the automatic threshold stays off, so every other read in
            # this module answers inline.
            "PROVISA_REDIRECT_ENABLED": "false",
        }
    finally:
        subprocess.run(["docker", "rm", "-f", name], capture_output=True)


@pytest.fixture(scope="module")
def server(results_store):
    boot = WorkerBoot(1, pg_host=_PG_HOST, pg_port=_PG_PORT, env=results_store)
    boot.create_database()
    try:
        boot.start()
        boot.wait_all_ready(timeout=300)
        yield boot
    finally:
        boot.cleanup()


def _http(boot, method: str, path: str, body: dict | None, headers: dict) -> dict:
    req = urllib.request.Request(
        f"http://127.0.0.1:{boot.ports['http']}{path}",
        data=None if body is None else json.dumps(body).encode(),
        headers={"Content-Type": "application/json", "x-provisa-role": _ROLE, **headers},
        method=method,
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            assert resp.status == 200
            return json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        raise AssertionError(f"{exc.code} {exc.read().decode()}") from exc


def _delivered_ids(handle: dict) -> set[int]:
    """The ids in the Parquet file the handle's presigned link returns."""
    import pyarrow.parquet as pq

    assert handle["row_count"] == len(_IDS), handle
    body = httpx.get(handle["redirect_url"], timeout=60)
    assert body.status_code == 200, body.text
    return set(pq.read_table(io.BytesIO(body.content)).column("id").to_pylist())


def _graphql(boot) -> dict:
    out = _http(boot, "POST", "/data/graphql", {"query": "{ s__orders { id } }"}, _FORCE)
    assert out["data"] == {"s__orders": None}, out
    return out["redirect"]


def _jsonapi(boot) -> dict:
    out = _http(boot, "GET", "/data/jsonapi/sales/orders", None, _FORCE)
    assert out["data"] is None, out
    return out["meta"]["redirect"]


def _rest(boot) -> dict:
    out = _http(boot, "GET", "/data/rest/sales/orders", None, _FORCE)
    assert out["data"] is None, out
    return out["meta"]["redirect"]


def _sql_http(boot) -> dict:
    out = _http(boot, "POST", "/data/sql", {"sql": "SELECT id FROM sales.orders"}, _FORCE)
    assert out["data"] == {"sql": None}, out
    return out["redirect"]


def _cypher_http(boot) -> dict:
    query = "MATCH (n:Orders) RETURN n.id AS id"
    out = _http(boot, "POST", "/data/cypher", {"query": query}, _FORCE)
    assert out["rows"] == [], out
    return out["redirect"]


_TRANSPORTS = {
    "graphql": _graphql,
    "jsonapi": _jsonapi,
    "rest": _rest,
    "sql_http": _sql_http,
    "cypher_http": _cypher_http,
}


@pytest.mark.parametrize("transport", list(_TRANSPORTS))
def test_a_forced_redirect_delivers_the_rows_and_answers_the_handle(server, transport):
    handle = _TRANSPORTS[transport](server)
    assert handle["redirect_url"].startswith("http"), handle
    assert _delivered_ids(handle) == _IDS


def test_without_the_header_the_same_read_is_answered_inline(server):
    out = _http(server, "POST", "/data/sql", {"sql": "SELECT id FROM sales.orders"}, {})
    assert {r["id"] for r in out["data"]["sql"]} == _IDS
    assert "redirect" not in out
