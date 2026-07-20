# Copyright (c) 2026 Kenneth Stott
# Canary: 4b7e2c81-9a36-4d15-8c07-1e6f3a2d5b90
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1105: the Streamable-HTTP transport maps a request's bearer token to a Provisa role.

Covers the pure-ASGI ``_wrap_role_auth`` middleware (token -> role, fail-closed 401s, the
off-loopback require-token gate) and that the resolved role reaches the tools via the
``_request_role`` ContextVar that ``_role`` reads.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.api.mcp import server as mcp_server


class _Recorder:
    """Minimal ASGI downstream: records that it ran and the role visible at call time."""

    def __init__(self):
        self.called = False
        self.role_seen: str | None = None

    async def __call__(self, scope, receive, send):
        self.called = True
        self.role_seen = mcp_server._request_role.get()
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"ok"})


def _http_scope(headers: list[tuple[bytes, bytes]] | None = None) -> dict:
    return {"type": "http", "headers": headers or []}


async def _drain(app, scope):
    sent: list[dict] = []

    async def receive():
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(msg):
        sent.append(msg)

    await app(scope, receive, send)
    return sent


def _status(sent: list[dict]) -> int:
    return next(m["status"] for m in sent if m["type"] == "http.response.start")


@pytest.mark.asyncio
async def test_valid_bearer_sets_request_role(monkeypatch):
    async def _fake_resolve(token, state):
        assert token == "good-token"
        return "analyst"

    monkeypatch.setattr(mcp_server, "_resolve_token_role_async", _fake_resolve)
    downstream = _Recorder()
    app = mcp_server._wrap_role_auth(downstream, SimpleNamespace(), require_token=True)

    sent = await _drain(app, _http_scope([(b"authorization", b"Bearer good-token")]))

    assert downstream.called
    assert downstream.role_seen == "analyst"  # role visible to the tool coroutine
    assert _status(sent) == 200
    # ContextVar reset after the request — no role leaks to the next one
    assert mcp_server._request_role.get() is None


@pytest.mark.asyncio
async def test_invalid_bearer_is_401_fail_closed(monkeypatch):
    async def _reject(token, state):
        raise ValueError("Invalid credentials")

    monkeypatch.setattr(mcp_server, "_resolve_token_role_async", _reject)
    downstream = _Recorder()
    app = mcp_server._wrap_role_auth(downstream, SimpleNamespace(), require_token=True)

    sent = await _drain(app, _http_scope([(b"authorization", b"Bearer bad")]))

    assert not downstream.called  # never reached the tool
    assert _status(sent) == 401


@pytest.mark.asyncio
async def test_no_bearer_off_loopback_is_401():
    downstream = _Recorder()
    app = mcp_server._wrap_role_auth(downstream, SimpleNamespace(), require_token=True)

    sent = await _drain(app, _http_scope([]))

    assert not downstream.called
    assert _status(sent) == 401


@pytest.mark.asyncio
async def test_no_bearer_loopback_passes_through():
    downstream = _Recorder()
    app = mcp_server._wrap_role_auth(downstream, SimpleNamespace(), require_token=False)

    sent = await _drain(app, _http_scope([]))

    assert downstream.called  # loopback: pinned-role stdio posture, no 401
    assert downstream.role_seen is None  # no request role set; _role falls back to pinned
    assert _status(sent) == 200


def test_sync_resolve_token_role_wrapper(monkeypatch):
    # the sync wrapper delegates to the async core
    async def _fake(token, state):
        return "ops"

    monkeypatch.setattr(mcp_server, "_resolve_token_role_async", _fake)
    assert mcp_server.resolve_token_role("tok", SimpleNamespace()) == "ops"


def test_resolve_token_role_no_auth_config_raises():
    with pytest.raises(PermissionError):
        mcp_server.resolve_token_role("tok", SimpleNamespace(auth_config=None))
