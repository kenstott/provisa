# Copyright (c) 2026 Kenneth Stott
# Canary: 1f6a3c8e-9d27-4b50-a4e1-7c0b5d2e8f96
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""E2E: a small relationship discovery run on a real server, its model a fake endpoint (REQ-413).

The ``test`` instance: an isolated server (DuckDB engine, its own control plane) over a SQLite
source with customers and orders, neither ever profiled. Relationship inference is assigned to an
OpenAI-compatible endpoint served by this test on the loopback, which answers every request with
orders.customer_id referring to customers.id. The request count is read first; the run is started
in the background, polled to its end, and makes exactly the requests counted; its suggestion is
stored and listed; accepting it registers the relationship and marks orders.customer_id a foreign
key, marking no key on customers.
"""

# Requirements: REQ-413, REQ-464

from __future__ import annotations

import json
import re
import sqlite3
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import httpx
import pytest
import yaml

pytestmark = [pytest.mark.integration, pytest.mark.local_platform]

_ROLE = "org_admin"
_HEADERS = {"X-Provisa-Role": _ROLE}
_MODEL = "fake-relationship-model"


class _Endpoint:
    """An OpenAI-compatible endpoint: every chat completion answers that orders.customer_id refers
    to customers.id, the tables named by the ids the request itself carries."""

    def __init__(self) -> None:
        self.prompts: list[str] = []
        endpoint = self

        class _Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:  # noqa: N802 -- http.server's name
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                prompt = body["messages"][-1]["content"]
                endpoint.prompts.append(prompt)
                ids = {
                    name: int(tid)
                    for tid, name in re.findall(r'"id":(\d+),"name":"(orders|customers)"', prompt)
                }
                answer: list[dict] = []
                if {"orders", "customers"} <= set(ids):
                    answer.append(
                        {
                            "source_table_id": ids["orders"],
                            "source_column": "customer_id",
                            "target_table_id": ids["customers"],
                            "target_column": "id",
                            "cardinality": "many-to-one",
                            "confidence": 0.95,
                            "reasoning": "each order names its customer",
                            "suggested_name": "customer-orders",
                        }
                    )
                payload = json.dumps(
                    {
                        "id": "chatcmpl-1",
                        "object": "chat.completion",
                        "created": 0,
                        "model": body["model"],
                        "choices": [
                            {
                                "index": 0,
                                "message": {"role": "assistant", "content": json.dumps(answer)},
                                "finish_reason": "stop",
                            }
                        ],
                        "usage": {
                            "prompt_tokens": 100,
                            "completion_tokens": 40,
                            "total_tokens": 140,
                        },
                    }
                ).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def log_message(self, format, *args) -> None:  # noqa: A002 -- the base method's name
                pass

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
        self.url = f"http://127.0.0.1:{self._server.server_address[1]}/v1"
        threading.Thread(target=self._server.serve_forever, daemon=True).start()

    def stop(self) -> None:
        self._server.shutdown()


@pytest.fixture(scope="module")
def endpoint():
    served = _Endpoint()
    yield served
    served.stop()


@pytest.fixture(scope="module")
def server(tmp_path_factory, endpoint):
    from tests.integration.isolated_server import IsolatedServer

    home = tmp_path_factory.mktemp("discovery_run")
    db = sqlite3.connect(home / "shop.sqlite")
    db.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT)")
    db.executemany(
        "INSERT INTO customers VALUES (?, ?)", [(i, f"customer {i}") for i in range(1, 11)]
    )
    db.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, note TEXT)")
    db.executemany(
        "INSERT INTO orders VALUES (?, ?, ?)", [(i, 1 + i % 10, f"order {i}") for i in range(1, 41)]
    )
    db.commit()
    db.close()
    readers = [_ROLE]

    def column(name: str, data_type: str, pk: bool = False) -> dict:
        return {"name": name, "data_type": data_type, "is_primary_key": pk, "visible_to": readers}

    config = {
        "auth": {"provider": "none"},
        "naming": {"domain_prefix": False, "rules": []},
        "domains": [{"id": "shop", "description": "A shop"}],
        "roles": [],
        "ai_endpoints": [
            {
                "id": "test-endpoint",
                "style": "openai",
                "base_url": endpoint.url,
                # The OpenAI client the endpoint is called through needs a key, any key.
                "api_key_env": "DISCOVERY_TEST_ENDPOINT_KEY",
            }
        ],
        "ai_models": {"relationship_inference": {"vendor": "test-endpoint", "model": _MODEL}},
        "sources": [{"id": "shop-db", "type": "sqlite", "path": str(home / "shop.sqlite")}],
        "tables": [
            {
                "source_id": "shop-db",
                "domain_id": "shop",
                "schema": "main",
                "table": "customers",
                "columns": [column("id", "integer", True), column("name", "varchar")],
            },
            {
                "source_id": "shop-db",
                "domain_id": "shop",
                "schema": "main",
                "table": "orders",
                "columns": [
                    column("id", "integer", True),
                    column("customer_id", "integer"),
                    column("note", "varchar"),
                ],
            },
        ],
        "relationships": [],
    }
    path = home / "config.yaml"
    path.write_text(yaml.safe_dump(config))
    srv = IsolatedServer(
        "discovery_run",
        engine="duckdb",
        config=str(path),
        control_plane="own",
        env={"DISCOVERY_TEST_ENDPOINT_KEY": "test-key"},
    )
    srv.start(timeout=240)
    try:
        yield srv
    finally:
        srv.stop_process()


def _ok(resp: httpx.Response) -> dict:
    assert resp.status_code == 200, resp.text
    return resp.json()


def _get(srv, path: str) -> dict:
    return _ok(httpx.get(f"{srv.base_url}{path}", headers=_HEADERS, timeout=60))


def _post(srv, path: str, body: dict | None = None) -> dict:
    return _ok(httpx.post(f"{srv.base_url}{path}", json=body, headers=_HEADERS, timeout=120))


def _columns(srv) -> dict[tuple[str, str], dict]:
    body = _ok(
        httpx.post(
            f"{srv.base_url}/admin/graphql",
            json={
                "query": "{ tables { tableName columns { columnName isForeignKey isPrimaryKey "
                "isAlternateKey } } }"
            },
            headers=_HEADERS,
            timeout=60,
        )
    )
    assert "errors" not in body, body
    return {
        (t["tableName"], c["columnName"]): c for t in body["data"]["tables"] for c in t["columns"]
    }


def test_a_small_discovery_runs_end_to_end(server, endpoint):
    scope = {
        "kinds": ["semantics"],
        "source_domains": ["shop"],
        "threshold": 0.7,
        "samples_per_column": 5,
    }

    models = _get(server, "/admin/discover/models")
    assert models["assigned"]["vendor"] == "test-endpoint"
    assert models["assigned"]["model"] == _MODEL
    assert (models["assigned"]["input_tokens"], models["assigned"]["output_tokens"]) == (
        32_000,
        4_096,
    )

    counted = _post(server, "/admin/discover/runs/count", scope)
    assert counted["requests"] >= 1
    assert sum(counted["by_group"].values()) == counted["requests"]
    assert endpoint.prompts == []

    run = _post(server, "/admin/discover/runs", scope)["run"]
    assert run["status"] == "running"
    deadline = time.monotonic() + 180
    while run["status"] == "running":
        assert time.monotonic() < deadline, run
        time.sleep(1)
        run = _get(server, f"/admin/discover/runs/{run['id']}")["run"]

    assert run["status"] == "done", run
    assert run["model"] == f"test-endpoint:{_MODEL}"
    assert len(endpoint.prompts) == counted["requests"], run["report"]
    assert run["requests_done"] == run["requests_total"] == counted["requests"]
    assert run["report"]["semantics"]["suggested"] == 1
    assert run["suggestions"] == 1
    assert "A shared pii tag alone is weak evidence" in endpoint.prompts[0]

    listed = _get(server, "/admin/discover/runs")["runs"]
    assert listed[0]["id"] == run["id"]

    candidates = httpx.get(
        f"{server.base_url}/admin/discover/candidates", headers=_HEADERS, timeout=60
    ).json()
    (suggestion,) = [c for c in candidates if c["source_column"] == "customer_id"]
    assert suggestion["target_column"] == "id"
    assert suggestion["cardinality"] == "many-to-one"
    assert suggestion["scope"] == f"run:{run['id']}"

    accepted = _post(server, f"/admin/discover/candidates/{suggestion['id']}/accept", {})
    assert accepted["relationship_id"] == "customer-orders"
    columns = _columns(server)
    assert columns[("orders", "customer_id")]["isForeignKey"] is True
    assert columns[("orders", "customer_id")]["isAlternateKey"] is False
    assert columns[("customers", "id")]["isAlternateKey"] is False
    assert columns[("customers", "id")]["isPrimaryKey"] is True  # declared, unchanged
