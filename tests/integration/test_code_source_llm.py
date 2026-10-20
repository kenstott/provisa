# Copyright (c) 2026 Kenneth Stott
# Canary: b2400450-5dab-45dd-974b-71617889edac
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Integration: a sandboxed worker calls Provisa's OpenAI-compatible LLM service (REQ-2105).

The ``test`` instance: an isolated server on DuckDB with its own control plane. Its AI Models
point the sql_generation role at an AI endpoint served by this test (a small OpenAI-compatible
server on the loopback, so no vendor key is needed). For each language the code's ``ask`` table
POSTs ``$PROVISA_SERVICES/llm/v1/chat/completions`` through the egress proxy, with nothing on the
egress allow-list and no key, and returns what came back. The role's model answers; a model the
org does not assign is refused by name; what the endpoint received was redacted; and a source with
a cap of 0 is refused once it is reached."""

# Requirements: REQ-2105

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import httpx
import pytest
import yaml

from tests.helpers import registered_id, release_field
from tests.integration.test_code_sources_e2e import (
    _ROLE,
    _admin,
    _archive,
    _column,
    _config,
    _parameter,
    _rows,
    _sql,
)

pytestmark = [pytest.mark.integration]

_MODEL = "anthropic/claude-haiku-5.5"  # priced in polly.model_prices' defaults

_PYTHON = {
    "ask/ask.py": '''
import json
import os
import urllib.error
import urllib.request
from dataclasses import dataclass


@dataclass
class Answer:
    model: str
    status: int
    body: str


def ask(model: str, prompt: str) -> list[Answer]:
    """What the LLM service answers for one prompt.

    @table
    """
    request = urllib.request.Request(
        f"{os.environ['PROVISA_SERVICES']}/llm/v1/chat/completions",
        data=json.dumps({"model": model, "messages": [{"role": "user", "content": prompt}]}).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=60) as answered:
            return [Answer(model, answered.status, answered.read().decode())]
    except urllib.error.HTTPError as refused:
        return [Answer(model, refused.code, refused.read().decode())]
''',
}

_NODE = {
    "ask/package.json": json.dumps({"name": "ask", "type": "module", "main": "index.js"}),
    "ask/index.js": """
export async function ask(model, prompt) {
  const answered = await fetch(`${process.env.PROVISA_SERVICES}/llm/v1/chat/completions`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ model, messages: [{ role: "user", content: prompt }] }),
  });
  return [{ model, status: answered.status, body: await answered.text() }];
}
""",
    "ask/index.d.ts": """
export interface Answer { model: string; status: number; body: string; }
/**
 * What the LLM service answers for one prompt.
 * @table
 */
export declare function ask(model: string, prompt: string): Promise<Answer[]>;
""",
}


class _Endpoint:
    """An OpenAI-compatible endpoint on the loopback: answers every chat completion, keeping what
    it was sent."""

    def __init__(self) -> None:
        self.received: list[dict] = []
        endpoint = self

        class _Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:  # noqa: N802 -- http.server's name
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                endpoint.received.append(body)
                answer = json.dumps(
                    {
                        "id": "chatcmpl-1",
                        "object": "chat.completion",
                        "created": 0,
                        "model": body["model"],
                        "choices": [
                            {
                                "index": 0,
                                "message": {"role": "assistant", "content": "churn is 4%"},
                                "finish_reason": "stop",
                            }
                        ],
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
def endpoint():
    served = _Endpoint()
    yield served
    served.stop()


@pytest.fixture(scope="module")
def runtime_cache(tmp_path_factory):
    return tmp_path_factory.mktemp("runtime-deps")


@pytest.fixture(scope="module", params=["python", "node"])
def server(tmp_path_factory, runtime_cache, endpoint, request):
    from tests.integration.isolated_server import IsolatedServer

    work = tmp_path_factory.mktemp(f"code_llm_{request.param}")
    config = {
        **_config(),
        "ai_endpoints": [{"id": "test-endpoint", "style": "openai", "base_url": endpoint.url}],
        "ai_models": {"sql_generation": {"vendor": "test-endpoint", "model": _MODEL}},
    }
    path = work / "config.yaml"
    path.write_text(yaml.safe_dump(config))
    srv = IsolatedServer(
        "code_llm",
        engine="duckdb",
        config=str(path),
        control_plane="own",
        materialize_store_url=f"duckdb:///{work / 'store.duckdb'}",
        env={"PROVISA_RUNTIME_DEPS_CACHE": str(runtime_cache)},
    )
    srv.language = request.param
    srv.start(timeout=240)
    try:
        yield srv
    finally:
        srv.stop_process()


def _source(srv, source_id: str, mapping_extra: dict) -> None:
    language = srv.language
    upload = httpx.post(
        f"{srv.base_url}/admin/orgs/{srv.org_id}/code-uploads/archive",
        params={"filename": "ask.zip"},
        content=_archive(_PYTHON if language == "python" else _NODE),
        headers={"X-Provisa-Role": _ROLE, "Content-Type": "application/octet-stream"},
        timeout=srv.request_timeout + 30,
    )
    assert upload.status_code == 200, upload.text
    mapping = {
        "runtime": "python" if language == "python" else "node",
        "code": {"kind": "upload", "upload": upload.json()["content_hash"]},
        **mapping_extra,
    }
    _admin(
        srv,
        f'createSource(input: {{id: "{source_id}", type: "{language}", host: "", port: 0, '
        f'database: "", cacheTtl: 0, mappingJson: {json.dumps(json.dumps(mapping))}}})',
    )
    registered = _admin(
        srv,
        f'registerTable(input: {{sourceId: "{source_id}", domainId: "fx", '
        f'schemaName: "default", tableName: "ask", cacheTtl: 0, alias: "{source_id.replace("-", "_")}", '
        f"columns: [{_column('model', 'text')}, {_column('status', 'bigint')}, "
        f"{_column('body', 'text')}, {_parameter('model', 'text', required=True)}, "
        f"{_parameter('prompt', 'text', required=True)}]}})",
    )
    _admin(srv, release_field(registered_id(registered["message"])))


@pytest.fixture(scope="module")
def sources(server):
    """Two sources of the same code: one with no cap, one with a cap of 0."""
    _source(server, f"ask-{server.language}", {})
    _source(server, f"capped-{server.language}", {"llm": {"cap_usd": 0}})
    return server.language


def _ask(srv, table: str, model: str, prompt: str) -> dict:
    (row,) = _rows(
        _sql(
            srv,
            f"SELECT status, body FROM {table} WHERE _nf_model = '{model}' AND _nf_prompt = '{prompt}'",
        )
    )
    return {"status": row["status"], "body": json.loads(row["body"])}


def test_a_role_names_the_orgs_model_and_what_goes_out_is_redacted(server, sources, endpoint):
    card = "4111 1111 1111 1111"
    answered = _ask(server, f"ask_{sources}", "provisa:sql_generation", f"churn for {card}?")
    assert answered["status"] == 200, answered
    assert answered["body"]["choices"][0]["message"]["content"] == "churn is 4%"
    assert answered["body"]["model"] == _MODEL
    sent = endpoint.received[-1]
    assert sent["model"] == _MODEL
    assert card not in json.dumps(sent) and "[REDACTED:card_number]" in json.dumps(sent)


def test_a_model_the_org_does_not_assign_is_refused_by_name(server, sources):
    answered = _ask(server, f"ask_{sources}", "gpt-9", "hello")
    assert answered["status"] == 403
    assert "'gpt-9'" in answered["body"]["error"]["message"]


def test_a_source_at_its_cap_is_refused_by_name(server, sources, endpoint):
    before = len(endpoint.received)
    answered = _ask(server, f"capped_{sources}", "provisa:sql_generation", "hello")
    assert answered["status"] == 429
    assert f"'capped-{sources}'" in answered["body"]["error"]["message"]
    assert len(endpoint.received) == before  # refused before any call
