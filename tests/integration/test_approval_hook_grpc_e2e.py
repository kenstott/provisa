# Copyright (c) 2026 Kenneth Stott
# Canary: 0f6f0a8e-2d1b-4c55-9a43-7e2b8f1c9d64
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The gRPC approval hook holds one persistent channel per instance -- on a real server (REQ-555).

One server whose ``auth.approval_hook`` is a gRPC policy service this test serves on the loopback,
and a webhook command ``notify`` that requires approval (REQ-209), served by this test too. The
command is called several times; each call is put to the policy service before the webhook is
called. Every call reaches the policy service from the same client address -- one channel (one
connection) reused across the calls, never a channel per call. A call the service denies is
refused and the webhook is not called.

Lands on the TEST instance only: one real server over a database the harness creates."""

from __future__ import annotations

import json
import os
import threading
from concurrent import futures
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import grpc
import httpx
import pytest

from provisa.auth import approval_pb2, approval_pb2_grpc
from tests.integration.worker_boot_harness import WorkerBoot
from tests.port_lease import lease_ports

pytestmark = [pytest.mark.integration]

_PG_HOST = os.environ.get("PG_HOST", "localhost")
_PG_PORT = int(os.environ.get("PG_PORT", "5432"))
_ADMIN = "org_admin"
_CALLS = 5
_TIMEOUT = 120.0


class _Policy(approval_pb2_grpc.ApprovalServiceServicer):
    """Approves every call but one whose arguments say ``deny``; keeps each caller's address."""

    def __init__(self) -> None:
        self.peers: list[str] = []
        self.requests: list[approval_pb2.ApprovalRequest] = []  # pyright: ignore[reportAttributeAccessIssue]
        self._lock = threading.Lock()

    def Evaluate(self, request, context):  # noqa: N802 -- the service's method name
        with self._lock:
            self.peers.append(context.peer())
            self.requests.append(request)
        denied = "deny" in request.arguments_json
        return approval_pb2.ApprovalResponse(  # pyright: ignore[reportAttributeAccessIssue]
            approved=not denied, reason="denied by the test policy" if denied else "ok"
        )


@pytest.fixture(scope="module")
def policy():
    servicer = _Policy()
    (port,) = lease_ports(1)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    approval_pb2_grpc.add_ApprovalServiceServicer_to_server(servicer, server)
    server.add_insecure_port(f"127.0.0.1:{port}")
    server.start()
    servicer.address = f"127.0.0.1:{port}"  # pyright: ignore[reportAttributeAccessIssue]
    yield servicer
    server.stop(grace=None)


@pytest.fixture(scope="module")
def webhook():
    """An HTTP endpoint answering every POST with ``{"ok": true}``, keeping each body."""
    received: list[dict] = []

    class _Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:  # noqa: N802 -- http.server's name
            received.append(json.loads(self.rfile.read(int(self.headers["Content-Length"]))))
            answer = json.dumps({"ok": True}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(answer)))
            self.end_headers()
            self.wfile.write(answer)

        def log_message(self, format, *args) -> None:  # noqa: A002 -- the base method's name
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{server.server_address[1]}/notify", received
    server.shutdown()


@pytest.fixture(scope="module")
def server(policy, webhook):
    url, _ = webhook
    boot = WorkerBoot(
        1,
        pg_host=_PG_HOST,
        pg_port=_PG_PORT,
        extra_config={
            "auth": {
                "provider": "none",
                "approval_hook": {
                    "type": "grpc",
                    "url": policy.address,
                    "timeout_ms": 5000,
                    "fallback": "deny",
                },
            },
            "webhooks": [
                {
                    "name": "notify",
                    "url": url,
                    "method": "POST",
                    "kind": "mutation",
                    "domain_id": "sales",
                    "arguments": [{"name": "msg", "type": "text"}],
                    "inline_return_type": [
                        {"name": "ok", "type": "Boolean", "visible_to": [_ADMIN]}
                    ],
                    "visible_to": [_ADMIN],
                    "requires_approval": True,
                }
            ],
        },
        env={"PROVISA_REDIRECT_ENABLED": "false"},
    )
    boot.create_database()
    try:
        boot.start()
        boot.wait_all_ready(timeout=300)
        yield boot
    finally:
        boot.cleanup()


def _graphql(boot, query: str) -> dict:
    resp = httpx.post(
        f"http://127.0.0.1:{boot.ports['http']}/data/graphql",
        json={"query": query},
        headers={"X-Provisa-Role": _ADMIN},
        timeout=_TIMEOUT,
    )
    assert resp.status_code == 200, resp.text
    return resp.json()


def _notify_field(boot) -> str:
    schema = _graphql(boot, "{ __schema { mutationType { fields { name } } } }")
    fields = [
        f["name"]
        for f in schema["data"]["__schema"]["mutationType"]["fields"]
        if f["name"].lower().endswith("notify")
    ]
    assert len(fields) == 1, fields
    return fields[0]


def _ok(answer) -> list[bool]:
    rows = answer if isinstance(answer, list) else [answer]
    return [row["ok"] for row in rows]


def test_every_approval_reaches_the_policy_service_over_one_channel(server, policy, webhook):
    _, received = webhook
    field = _notify_field(server)
    before = len(policy.peers)
    for i in range(_CALLS):
        body = _graphql(server, f'mutation {{ {field}(msg: "call {i}") {{ ok }} }}')
        assert "errors" not in body, body
        assert _ok(body["data"][field]) == [True], body

    peers = policy.peers[before:]
    assert len(peers) == _CALLS, peers
    # One channel, one connection: every call arrives from the same client address.
    assert len(set(peers)) == 1, peers
    assert [r.command for r in policy.requests[before:]] == ["notify"] * _CALLS
    assert [r["msg"] for r in received[-_CALLS:]] == [f"call {i}" for i in range(_CALLS)]


def test_a_call_the_policy_service_denies_is_refused_and_not_made(server, policy, webhook):
    _, received = webhook
    field = _notify_field(server)
    made = len(received)
    # A denied command is refused with 403 functions.approval_denied, carrying the policy's reason.
    resp = httpx.post(
        f"http://127.0.0.1:{server.ports['http']}/data/graphql",
        json={"query": f'mutation {{ {field}(msg: "deny this") {{ ok }} }}'},
        headers={"X-Provisa-Role": _ADMIN},
        timeout=_TIMEOUT,
    )
    assert resp.status_code == 403, resp.text
    body = resp.json()
    assert body["code"] == "functions.approval_denied", body
    assert body["params"] == {"name": "notify", "reason": "denied by the test policy"}, body
    assert len(received) == made
    # The denial came over the same channel as every approval before it.
    assert len(set(policy.peers)) == 1, policy.peers
