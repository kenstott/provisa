# Copyright (c) 2026 Kenneth Stott
# Canary: 6d2b9e41-8f3a-4c17-a5e0-2b7c9d4f1e63
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E (REQ-1901): an API-source result cache on the DuckDB engine with an embedded DuckDB-file
materialize store. The engine connection never ATTACHes that store, so the cache's schema, table
and drop statements must reach it through the store broker — never as ``mat_store.*`` SQL on the
engine connection, where the catalog does not exist.

A real Provisa server (DuckDB engine, SQLite control plane, DuckDB-file materialize store) registers
a remote GraphQL source served by this test's own HTTP server; GraphQL and Cypher reads of it land
the remote's rows in the store and serve them back.
"""

# Requirements: REQ-1901

from __future__ import annotations

import json
import os
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import duckdb
import httpx
import pytest
import pytest_asyncio

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

_ISOLATED_ORG = "duckdb_store_api_cache"
_CONFIG = "tests/fixtures/duckdb_gql_remote_union_config.yaml"


class _RemoteGraphQL:
    """Answers any ``goodItems`` query with two rows."""

    def __init__(self) -> None:
        class _Handler(BaseHTTPRequestHandler):
            def do_POST(self):  # noqa: N802 - http.server's handler name
                self.rfile.read(int(self.headers.get("Content-Length", 0)))
                payload = {"data": {"goodItems": [{"id": 1}, {"id": 2}]}}
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps(payload).encode())

            def log_message(self, format, *args):  # noqa: A002 - BaseHTTPRequestHandler's name
                del format, args

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
        self.url = f"http://127.0.0.1:{self._server.server_address[1]}/graphql"
        threading.Thread(target=self._server.serve_forever, daemon=True).start()

    def close(self) -> None:
        self._server.shutdown()


@pytest_asyncio.fixture(scope="module")
async def cache_server():
    from tests.integration.isolated_server import IsolatedServer

    remote = _RemoteGraphQL()
    os.environ["PROVISA_TEST_GQL_REMOTE_URL"] = remote.url
    store_dir = tempfile.TemporaryDirectory()
    store_path = Path(store_dir.name) / "materialize.duckdb"
    server = IsolatedServer(
        _ISOLATED_ORG,
        engine="duckdb",
        config=_CONFIG,
        control_plane="sqlite",
        materialize_store_url=f"duckdb:///{store_path}",
    )
    server.start()
    try:
        yield server, store_path
    finally:
        server.stop_process()
        store_dir.cleanup()
        remote.close()
        del os.environ["PROVISA_TEST_GQL_REMOTE_URL"]


async def _post(server, path: str, body: dict) -> httpx.Response:
    async with httpx.AsyncClient(base_url=server.base_url, timeout=60.0) as client:
        return await client.post(path, json=body, headers={"X-Provisa-Role": "org_admin"})


def _store_schemas(store_path: Path) -> set[str]:
    con = duckdb.connect(str(store_path), read_only=True)
    try:
        return {r[0] for r in con.execute("SELECT schema_name FROM duckdb_schemas()").fetchall()}
    finally:
        con.close()


async def test_a_graphql_read_of_a_remote_table_is_served_through_the_store(cache_server):
    server, _store_path = cache_server
    resp = await _post(server, "/data/graphql", {"query": "{ goodItems { id } }"})
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert "errors" not in body, body
    assert sorted(r["id"] for r in body["data"]["goodItems"]) == [1, 2], body


async def test_the_cache_schema_is_created_in_the_store_file(cache_server):
    server, store_path = cache_server
    resp = await _post(server, "/data/graphql", {"query": "{ goodItems { id } }"})
    assert resp.status_code == 200, resp.text
    # The broker holds the store file only for the length of one operation (REQ-1901), so a
    # read-only open between requests does not contend with the server.
    assert any(s.endswith("_gql_cache") for s in _store_schemas(store_path)), (
        "the API cache schema was not created in the materialize store"
    )
