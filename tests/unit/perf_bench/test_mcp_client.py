# Copyright (c) 2026 Kenneth Stott
# Canary: f019271f-5208-4ee3-93c9-7d5cdeeb80b4
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""REQ-2092: the bench's MCP transport. Its client speaks Streamable HTTP to a real MCP SDK
server (FastMCP, as Provisa serves it): one session, then a run_sql tools/call per request."""

# Requirements: REQ-2092

from __future__ import annotations

import sys
import threading
import time
from collections.abc import Iterator
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

BENCH = Path(__file__).resolve().parents[3] / "demo" / "named" / "perf" / "bench"
sys.path.insert(0, str(BENCH))

import contract_model as cm  # noqa: E402
import optimistic_load as ol  # noqa: E402
import setup_contract as sc  # noqa: E402
from queries import Query  # noqa: E402

SETUP = BENCH / "setups" / "perf-stack.yaml"
ENV = {"PROVISA_HTTP_BASE_URL": "http://localhost:8001"}


def _load(path: Path) -> cm.Setup:
    return sc.load_setup(path, environ=ENV, known_transports=list(ol.TRANSPORTS))


@pytest.fixture
def mcp_server() -> Iterator[tuple[str, list[dict[str, Any]]]]:
    import uvicorn
    from mcp.server.fastmcp import FastMCP

    calls: list[dict[str, Any]] = []
    mcp = FastMCP("provisa")

    @mcp.tool()
    async def run_sql(sql: str, role: str | None = None) -> dict:
        calls.append({"sql": sql, "role": role})
        return {
            "columns": ["order_id", "amount"],
            "rows": [{"order_id": 1, "amount": 2.5}, {"order_id": 2, "amount": None}],
            "row_count": 2,
            "offset": 0,
            "total_rows": 2,
            "truncated": False,
        }

    # Port 0, bound by the server itself: a number picked by binding and closing a socket was
    # free for any other test's bind(0) to be given before the server took it -- and a TLS client
    # of that other test then reached this plain-HTTP server (WRONG_VERSION_NUMBER).
    server = uvicorn.Server(
        uvicorn.Config(mcp.streamable_http_app(), host="127.0.0.1", port=0, log_level="error")
    )
    t = threading.Thread(target=server.run, daemon=True)
    t.start()
    deadline = time.monotonic() + 20
    while not server.started:
        assert time.monotonic() < deadline, "the MCP server did not start"
        time.sleep(0.05)
    port = server.servers[0].sockets[0].getsockname()[1]
    try:
        yield f"http://127.0.0.1:{port}/mcp", calls
    finally:
        server.should_exit = True
        t.join(10)


def _endpoints(url: str | None) -> ol.Endpoints:
    return replace(ol.endpoints_from_setup(_load(SETUP)), mcp_url=url)


def test_a_request_is_one_run_sql_call_in_the_client_s_session(mcp_server) -> None:
    url, calls = mcp_server
    client = ol.CLIENTS["mcp"](_endpoints(url))
    try:
        sql = "SELECT order_id, amount FROM t -- @provisa cache=true"
        query = Query(id="q", category="point", description="a read", sql=sql)
        assert client.call(query, "org_admin") == (2, 2, None)
        assert client.call(query, "bench_gov_org_admin_d1") == (2, 2, None)
    finally:
        client.close()
    assert calls == [
        {"sql": "SELECT order_id, amount FROM t -- @provisa cache=true", "role": "org_admin"},
        {
            "sql": "SELECT order_id, amount FROM t -- @provisa cache=true",
            "role": "bench_gov_org_admin_d1",
        },
    ]


def test_the_mcp_transport_needs_the_deployment_s_mcp_endpoint(tmp_path: Path) -> None:
    import yaml

    raw = yaml.safe_load(SETUP.read_text())
    raw["deployment"]["endpoints"].pop("mcp", None)
    raw["transports"] = {"mcp": {}}
    path = tmp_path / "setup.yaml"
    path.write_text(yaml.safe_dump(raw))
    with pytest.raises(cm.SetupError, match="transports.mcp: needs deployment.endpoints.mcp.url"):
        _load(path)
