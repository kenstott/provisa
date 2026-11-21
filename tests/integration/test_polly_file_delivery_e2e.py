# Copyright (c) 2026 Kenneth Stott
# Canary: c82c7d52-3d7d-49e8-b85b-2fe1b293c822
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A file asked of Polly is a file the user can fetch, and Polly is told when there is none.

The report: asked for a CSV of 37 customers, Polly said four times that the download was in the
chat, and none ever appeared. A real server (DuckDB engine, SQLite source, simple auth, no object
store) with the real chat endpoint and the real run_sql; only the model is scripted, as an
OpenAI-compatible endpoint on the loopback that also keeps what it was told."""

# Requirements: REQ-1192, REQ-1194

from __future__ import annotations

import csv
import io
import json
import sqlite3
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import httpx
import pytest
import yaml

from tests.integration.isolated_server import IsolatedServer
from tests.integration.test_local_result_sink_e2e import _PASSWORD, _ROLE, _user

pytestmark = [pytest.mark.integration, pytest.mark.local_platform]

_REPO = Path(__file__).resolve().parents[2]
_ORG = "polly_file_delivery_e2e"
_ROWS = 37
_MODEL = "anthropic/claude-haiku-5.5"  # priced in polly.model_prices' defaults
_SQL = "SELECT id, name FROM store.items"


def _called(name: str, arguments: dict) -> dict:
    return {
        "message": {
            "role": "assistant",
            "content": None,
            "tool_calls": [
                {
                    "id": "call_1",
                    "type": "function",
                    "function": {"name": name, "arguments": json.dumps(arguments)},
                }
            ],
        },
        "finish_reason": "tool_calls",
    }


def _said(text: str) -> dict:
    return {"message": {"role": "assistant", "content": text}, "finish_reason": "stop"}


class _ScriptedModel:
    """An OpenAI-compatible endpoint answering each chat completion with the next of its script,
    keeping what it was sent."""

    def __init__(self) -> None:
        self.script: list[dict] = []
        self.received: list[dict] = []
        model = self

        class _Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:  # noqa: N802 -- http.server's name
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                model.received.append(body)
                answer = json.dumps(
                    {
                        "id": "chatcmpl-1",
                        "object": "chat.completion",
                        "created": 0,
                        "model": body["model"],
                        "choices": [{"index": 0, **model.script.pop(0)}],
                        "usage": {"prompt_tokens": 12, "completion_tokens": 4, "total_tokens": 16},
                    }
                ).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(answer)))
                self.end_headers()
                self.wfile.write(answer)

            def log_message(self, format, *args) -> None:  # noqa: A002 -- the base method's name
                pass

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
        self.url = f"http://127.0.0.1:{self._server.server_address[1]}/v1"
        threading.Thread(target=self._server.serve_forever, daemon=True).start()

    def stop(self) -> None:
        self._server.shutdown()


@pytest.fixture(scope="module")
def model():
    served = _ScriptedModel()
    yield served
    served.stop()


@pytest.fixture(scope="module")
def server(tmp_path_factory, model):
    work = tmp_path_factory.mktemp("polly_file_delivery")
    db = work / "items.sqlite"
    con = sqlite3.connect(db)
    con.execute("CREATE TABLE items (id INTEGER, name TEXT)")
    con.executemany(
        "INSERT INTO items VALUES (?, ?)", [(i, f"item {i}") for i in range(1, _ROWS + 1)]
    )
    con.commit()
    con.close()
    with open(_REPO / "tests/fixtures/sample_config.yaml") as f:
        base = yaml.safe_load(f)
    cfg = {
        "naming": base["naming"],
        "relationships": [],
        "roles": [
            {
                "id": _ROLE,
                "description": "Test role.",
                "capabilities": ["query_development", "full_results"],
                "domain_access": ["store"],
            }
        ],
        "sources": [{"id": "items-sqlite", "type": "sqlite", "path": str(db)}],
        "domains": [{"id": "store", "description": "polly file delivery e2e"}],
        "tables": [
            {
                "source_id": "items-sqlite",
                "domain_id": "store",
                "schema": "default",
                "table": "items",
                "columns": [
                    {"name": "id", "data_type": "integer", "visible_to": [_ROLE]},
                    {"name": "name", "data_type": "varchar", "visible_to": [_ROLE]},
                ],
            }
        ],
        "ai_endpoints": [{"id": "scripted", "style": "openai", "base_url": model.url}],
        "ai_models": {"mcp_chat": {"vendor": "scripted", "model": _MODEL}},
    }
    cfg_path = work / "config.yaml"
    cfg_path.write_text(yaml.safe_dump(cfg))
    srv = IsolatedServer(
        _ORG,
        engine="duckdb",
        config=str(cfg_path),
        control_plane="own",
        auth={
            "provider": "simple",
            "allow_simple_auth": True,
            "jwt_secret": "polly-file-delivery-e2e-signing-secret-0123456789",
            "default_role": _ROLE,
            "simple": {"users": [_user("ada")]},
        },
        env={
            "PROVISA_DATA_DIR": str(work / "data"),
            # No object store: no endpoint and no credentials, whatever the runner's env holds.
            "PROVISA_REDIRECT_ENDPOINT": "",
            "PROVISA_REDIRECT_ACCESS_KEY": "",
            "PROVISA_REDIRECT_SECRET_KEY": "",
        },
    )
    srv.start(timeout=300)
    try:
        yield srv
    finally:
        srv.stop_process()


@pytest.fixture(scope="module")
def headers(server) -> dict[str, str]:
    resp = httpx.post(
        f"{server.base_url}/auth/login",
        json={"username": "ada", "password": _PASSWORD},
        timeout=60,
    )
    assert resp.status_code == 200, resp.text
    return {"Authorization": f"Bearer {resp.json()['access_token']}"}


def _turn(server, headers: dict[str, str], model, script: list[dict], asked: str) -> list[dict]:
    """The chat's events for one user message, the model answering from ``script``."""
    model.script[:] = script
    model.received.clear()
    resp = httpx.post(
        f"{server.base_url}/admin/mcp/chat",
        json={"messages": [{"role": "user", "content": asked}]},
        headers=headers,
        timeout=120,
    )
    assert resp.status_code == 200, resp.text
    events = [
        json.loads(block[len("data:") :]) for block in resp.text.split("\n\n") if block.strip()
    ]
    assert not [e for e in events if e["type"] == "error"], events
    return events


def _told(model) -> dict:
    """The run_sql result as the model got it."""
    tool = [m for m in model.received[-1]["messages"] if m["role"] == "tool"]
    content = tool[-1]["content"]
    # The last message is sent as content parts: it carries the gateway's cache breakpoint.
    return json.loads(content if isinstance(content, str) else content[0]["text"])


def test_a_small_result_asked_for_as_a_file_is_one_the_user_can_download(server, headers, model):
    events = _turn(
        server,
        headers,
        model,
        [_called("run_sql", {"sql": _SQL, "file_format": "csv"}), _said("It is ready.")],
        "give me these items as a CSV",
    )
    artifacts = [e for e in events if e["type"] == "artifact"]
    assert len(artifacts) == 1, events
    artifact = artifacts[0]
    assert artifact["kind"] == "file" and artifact["row_count"] == _ROWS, artifact

    # What the chat panel fetches the file with: the signed-in user's own credential.
    fetched = httpx.get(f"{server.base_url}{artifact['redirect_url']}", headers=headers, timeout=60)
    assert fetched.status_code == 200, fetched.text
    rows = list(csv.DictReader(io.StringIO(fetched.text)))
    assert [int(r["id"]) for r in rows] == list(range(1, _ROWS + 1))

    told = _told(model)
    assert told["redirect"]["redirect_url"] == artifact["redirect_url"]
    assert "artifact" not in told


def test_a_download_link_followed_with_no_credential_is_refused(server, headers, model):
    """Why the panel fetches it: a link the browser follows carries no Authorization header."""
    events = _turn(
        server,
        headers,
        model,
        [_called("run_sql", {"sql": _SQL, "file_format": "csv"}), _said("It is ready.")],
        "give me these items as a CSV",
    )
    artifact = next(e for e in events if e["type"] == "artifact")
    assert httpx.get(f"{server.base_url}{artifact['redirect_url']}", timeout=60).status_code == 401


def test_a_turn_that_delivered_no_file_tells_the_model_so(server, headers, model):
    events = _turn(
        server,
        headers,
        model,
        [_called("run_sql", {"sql": _SQL}), _said("Here are the items.")],
        "give me these items as a CSV",
    )
    assert not [e for e in events if e["type"] == "artifact"], events
    told = _told(model)
    assert "redirect" not in told and "shown" not in told
    assert "No file was delivered" in told["file"], told
