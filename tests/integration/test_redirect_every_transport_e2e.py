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

import asyncio
import io
import json
import os
import subprocess
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
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


# NL generation needs a model. The test serves one of its own: an OpenAI-style chat-completions
# endpoint that answers every prompt with the same SQL, registered as the org's custom endpoint
# (REQ-1790) for the NL operations. The SQL branch validates it and runs it with the delivery.
_NL_SQL = "SELECT id FROM sales.orders"
_LLM_KEY_ENV = "PROVISA_TEST_REDIRECT_LLM_KEY"


class _StubModel(BaseHTTPRequestHandler):
    def do_POST(self):  # noqa: N802 — http.server's handler name
        self.rfile.read(int(self.headers.get("Content-Length", "0")))
        body = json.dumps(
            {
                "id": "stub",
                "object": "chat.completion",
                "created": 0,
                "model": "stub",
                "choices": [
                    {
                        "index": 0,
                        "message": {"role": "assistant", "content": _NL_SQL},
                        "finish_reason": "stop",
                    }
                ],
                "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
            }
        ).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_args):
        return


@pytest.fixture(scope="module")
def stub_model():
    httpd = ThreadingHTTPServer(("127.0.0.1", 0), _StubModel)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{httpd.server_address[1]}/v1"
    finally:
        httpd.shutdown()
        httpd.server_close()


@pytest.fixture(scope="module")
def server(results_store, stub_model):
    env = {**results_store, _LLM_KEY_ENV: "stub-key"}
    boot = WorkerBoot(1, pg_host=_PG_HOST, pg_port=_PG_PORT, env=env)
    boot.create_database()
    try:
        boot.start()
        boot.wait_all_ready(timeout=300)
        model = {"vendor": "stub", "model": "stub"}
        _http(
            boot,
            "PUT",
            "/admin/ai-models",
            {
                "ai_endpoints": [
                    {
                        "id": "stub",
                        "style": "openai",
                        "base_url": stub_model,
                        "api_key_env": _LLM_KEY_ENV,
                    }
                ],
                "ai_models": {"sql_generation": model, "table_selection": model},
            },
            {},
        )
        yield boot
    finally:
        boot.cleanup()


def _http(
    boot, method: str, path: str, body: dict | None, headers: dict, *, status: int = 200
) -> dict:
    req = urllib.request.Request(
        f"http://127.0.0.1:{boot.ports['http']}{path}",
        data=None if body is None else json.dumps(body).encode(),
        headers={"Content-Type": "application/json", "x-provisa-role": _ROLE, **headers},
        method=method,
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            assert resp.status == status, resp.status
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


def _grpc(boot) -> dict:
    """A streamed Query RPC: the call's metadata forces the delivery; no message is streamed and
    the handle rides the trailing metadata."""
    import grpc
    from google.protobuf.message_factory import GetMessageClass

    from tests.grpc_proto_client import role_descriptor_pool

    _pool, svc = role_descriptor_pool(f"http://127.0.0.1:{boot.ports['http']}", _ROLE)
    method = next(
        m
        for m in svc.methods
        if m.name.startswith("Query")
        and "orders" in m.name.lower()
        and not m.name.endswith(("Aggregate", "GroupBy", "Batch"))
    )
    req_cls = GetMessageClass(method.input_type)
    resp_cls = GetMessageClass(method.output_type)
    channel = grpc.insecure_channel(f"127.0.0.1:{boot.ports['grpc']}")
    try:
        rpc = channel.unary_stream(
            f"/{svc.full_name}/{method.name}",
            request_serializer=req_cls.SerializeToString,
            response_deserializer=resp_cls.FromString,
        )
        call = rpc(
            req_cls(),
            metadata=(
                ("x-provisa-role", _ROLE),
                ("x-provisa-redirect", "true"),
                ("x-provisa-redirect-format", "parquet"),
            ),
            timeout=120,
        )
        assert list(call) == []  # nothing streamed: the rows were delivered
        trailers = dict(call.trailing_metadata())
        return json.loads(trailers["x-provisa-redirect"])
    finally:
        channel.close()


def _bolt(boot) -> dict:
    """Bolt: the transaction metadata forces the delivery; no record comes back and the handle is
    in the RUN's summary."""
    from neo4j import GraphDatabase, Query

    driver = GraphDatabase.driver(f"bolt://127.0.0.1:{boot.ports['bolt']}", auth=(_ROLE, ""))
    try:
        with driver.session() as sess:
            result = sess.run(
                Query(
                    "MATCH (n:Orders) RETURN n.id AS id",
                    metadata={"provisa_redirect": "true", "provisa_redirect_format": "parquet"},
                )
            )
            assert list(result) == []
            return result.consume().metadata["redirect"]
    finally:
        driver.close()


def _pgwire_row(conn) -> dict:
    """The one row a redirected pgwire statement answers, read as a handle."""
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM sales.orders")
        assert [d.name for d in cur.description] == ["url", "format", "row_count", "expires_at"]
        ((url, fmt, row_count, expires_at),) = cur.fetchall()
    assert fmt == "parquet" and expires_at.tzinfo is not None, (fmt, expires_at)
    return {"redirect_url": url, "row_count": row_count}


def _pgwire_connect(boot, **kwargs):
    import psycopg

    return psycopg.connect(
        host="127.0.0.1",
        port=boot.ports["pgwire"],
        user=_ROLE,
        password="provisa",
        dbname="provisa",
        autocommit=True,
        connect_timeout=30,
        **kwargs,
    )


def _pgwire_set(boot) -> dict:
    """pgwire: the session's SET turns the forced redirect on; RESET turns it off again."""
    with _pgwire_connect(boot) as conn:
        conn.execute("SET provisa.redirect = on")
        conn.execute("SET provisa.redirect_format = 'parquet'")
        handle = _pgwire_row(conn)
        conn.execute("RESET provisa.redirect")
        rows = conn.execute("SELECT id FROM sales.orders").fetchall()
        assert {r[0] for r in rows} == _IDS
        return handle


def _pgwire_startup(boot) -> dict:
    """pgwire: the startup packet's options name the forced redirect for the whole connection."""
    options = "-c provisa.redirect=on -c provisa.redirect_format=parquet"
    with _pgwire_connect(boot, options=options) as conn:
        return _pgwire_row(conn)


def _flight(boot, query: str) -> dict:
    """Flight: the ticket's options force the delivery; do_get answers one row naming it."""
    import pyarrow.flight as fl

    client = fl.connect(f"grpc://127.0.0.1:{boot.ports['flight']}")
    try:
        ticket = {"query": query, "role": _ROLE, "redirect": True, "redirect_format": "parquet"}
        table = client.do_get(fl.Ticket(json.dumps(ticket).encode())).read_all()
    finally:
        client.close()
    assert table.schema.names == ["url", "format", "row_count", "expires_at"], table.schema
    (row,) = table.to_pylist()
    assert row["format"] == "parquet" and row["expires_at"].tzinfo is not None, row
    return {"redirect_url": row["url"], "row_count": row["row_count"]}


def _flight_sql(boot) -> dict:
    return _flight(boot, "SELECT id FROM sales.orders")


def _flight_graphql(boot) -> dict:
    return _flight(boot, "{ s__orders { id } }")


def _flight_cypher(boot) -> dict:
    return _flight(boot, "MATCH (n:Orders) RETURN n.id AS id")


def _mcp(boot) -> dict:
    """MCP: run_sql's redirect arguments force the delivery; the tool answers the handle."""
    from mcp import ClientSession
    from mcp.client.streamable_http import streamablehttp_client

    async def _call() -> dict:
        url = f"http://127.0.0.1:{boot.ports['mcp']}/mcp"
        async with streamablehttp_client(url) as (read, write, _):
            async with ClientSession(read, write) as session:
                await session.initialize()
                result = await session.call_tool(
                    "run_sql",
                    {
                        "sql": "SELECT id FROM sales.orders",
                        "role": _ROLE,
                        "redirect": True,
                        "redirect_format": "parquet",
                    },
                )
                text = "".join(getattr(c, "text", "") for c in result.content)
                assert not result.isError, text
                return json.loads(text)

    out = asyncio.run(_call())
    assert out["rows"] == [], out
    return out["redirect"]


def _nl(boot) -> dict:
    """NL: the request's redirect option forces the delivery of every branch it runs; the SQL
    branch's result is the handle."""
    job = _http(
        boot,
        "POST",
        "/query/nl",
        {"q": "list the order ids", "redirect": True, "redirect_format": "parquet"},
        {},
        status=202,
    )
    deadline = time.monotonic() + 180
    while True:
        out = _http(boot, "GET", f"/query/nl/{job['job_id']}", None, {})
        if out["state"] in ("complete", "failed"):
            break
        assert time.monotonic() < deadline, out
        time.sleep(1)
    branch = out["branches"]["sql"]
    assert out["state"] == "complete" and branch["error"] is None, out
    # The branch reports the statement as the runner normalized it (quoted, aliased).
    assert branch["query"] == 'SELECT id FROM "sales"."orders" AS "orders"', branch
    return branch["result"]["redirect"]


_TRANSPORTS = {
    "graphql": _graphql,
    "jsonapi": _jsonapi,
    "rest": _rest,
    "sql_http": _sql_http,
    "cypher_http": _cypher_http,
    "grpc": _grpc,
    "bolt": _bolt,
    "pgwire_set": _pgwire_set,
    "pgwire_startup": _pgwire_startup,
    "flight_sql": _flight_sql,
    "flight_graphql": _flight_graphql,
    "flight_cypher": _flight_cypher,
    "mcp": _mcp,
    "nl": _nl,
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
