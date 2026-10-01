# Copyright (c) 2026 Kenneth Stott
# Canary: 3a8e6f19-2c7d-4b50-9e14-7d0b5c2a6e81
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E (REQ-1661, amended 2026-09-30): a UNION over two remote tables, one of whose fetch fails,
errors -- the failing branch is never dropped to return the other branch's rows as complete.

A real Provisa server (DuckDB engine, SQLite control plane, DuckDB materialize store) registers a
remote GraphQL source served by this test's own HTTP server with two tables: ``good_items``
(answers) and ``bad_items`` (HTTP 500).
"""

# Requirements: REQ-1661

from __future__ import annotations

import json
import os
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import httpx
import pytest
import pytest_asyncio

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]

pytest.importorskip("duckdb")

_ISOLATED_ORG = "failed_remote_branch_union"
_CONFIG = "tests/fixtures/duckdb_gql_remote_union_config.yaml"


class _RemoteGraphQL:
    """Answers ``goodItems`` with two rows; any query naming ``badItems`` gets HTTP 500."""

    def __init__(self) -> None:
        class _Handler(BaseHTTPRequestHandler):
            def do_POST(self):  # noqa: N802 - http.server's handler name
                body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
                query = json.loads(body or b"{}").get("query", "")
                if "badItems" in query:
                    self.send_response(500)
                    self.end_headers()
                    return
                payload = {"data": {"goodItems": [{"id": 1}, {"id": 2}]}}
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps(payload).encode())

            def log_message(self, *_args):
                pass

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
        self.url = f"http://127.0.0.1:{self._server.server_address[1]}/graphql"
        threading.Thread(target=self._server.serve_forever, daemon=True).start()

    def close(self) -> None:
        self._server.shutdown()


@pytest_asyncio.fixture(scope="module")
async def union_server():
    from tests.integration.isolated_server import IsolatedServer

    remote = _RemoteGraphQL()
    os.environ["PROVISA_TEST_GQL_REMOTE_URL"] = remote.url
    store_dir = tempfile.TemporaryDirectory()
    server = IsolatedServer(
        _ISOLATED_ORG,
        engine="duckdb",
        config=_CONFIG,
        control_plane="sqlite",
        materialize_store_url=f"duckdb:///{Path(store_dir.name) / 'materialize.duckdb'}",
    )
    server.start()
    try:
        yield server
    finally:
        server.stop_process()
        store_dir.cleanup()
        remote.close()
        del os.environ["PROVISA_TEST_GQL_REMOTE_URL"]


async def _sql(server, sql: str) -> httpx.Response:
    async with httpx.AsyncClient(base_url=server.base_url, timeout=60.0) as client:
        return await client.post("/data/sql", json={"sql": sql, "role": "org_admin"})


async def test_the_reachable_remote_table_answers_alone(union_server):
    """Control: the wiring works -- the good table alone returns the remote's rows."""
    resp = await _sql(union_server, "SELECT id FROM good_items ORDER BY id")
    assert resp.status_code == 200, resp.text
    assert "1" in resp.text and "2" in resp.text, resp.text


async def test_a_union_with_a_failing_remote_branch_fails(union_server):
    resp = await _sql(union_server, "SELECT id FROM good_items UNION ALL SELECT id FROM bad_items")
    assert resp.status_code >= 400, f"returned the other branch as complete: {resp.text}"
    assert "500" in resp.text, resp.text  # the failing remote's own cause reaches the caller
