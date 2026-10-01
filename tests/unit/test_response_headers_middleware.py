# Copyright (c) 2026 Kenneth Stott
# Canary: 1c7e5b93-8a2f-4d60-b3e4-6f9a0c2d7e51
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The response-header middleware is pure ASGI (REQ-1882 hand-off cost).

It used to be an ``@app.middleware("http")`` function — Starlette's ``BaseHTTPMiddleware``, which
on EVERY request (data path included) wraps the response in anyio streams, runs the app in a child
task and awaits ``receive()`` itself to watch for a disconnect; behind the request-thread
middleware each of those is another relay to the accepting loop. What it does is unchanged:
``X-Schema-Version`` on ``/admin/graphql``, the post-trial license notice header, and 499 for a
client that disconnected before a response started.
"""

# Requirements: REQ-1137, REQ-1882

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest
from starlette.requests import ClientDisconnect

from provisa.api.middleware.response_headers import ResponseHeadersMiddleware

_START = {
    "type": "http.response.start",
    "status": 200,
    "headers": [(b"content-type", b"application/json")],
}
_BODY = {"type": "http.response.body", "body": b'{"data":1}'}


def _serve(path: str, app=None, *, schema_version: int = 7):
    """Run one request through the middleware; return (messages sent, receive() calls made by
    anything other than the app)."""
    receives: list[str] = []

    async def inner(scope, receive, send):
        await send(dict(_START, headers=list(_START["headers"])))
        await send(dict(_BODY))

    async def receive():
        receives.append("receive")
        return {"type": "http.request", "body": b"", "more_body": False}

    sent: list[dict] = []

    async def send(message):
        sent.append(message)

    middleware = ResponseHeadersMiddleware(
        app or inner, state=SimpleNamespace(schema_version=schema_version)
    )
    asyncio.run(
        middleware({"type": "http", "path": path, "method": "POST", "headers": []}, receive, send)
    )
    return sent, receives


def _headers(message: dict) -> dict[bytes, bytes]:
    return dict(message["headers"])


@pytest.fixture(autouse=True)
def _no_nag(monkeypatch):
    monkeypatch.setattr("provisa.licensing.emit.should_nag", lambda: False)


def test_a_data_request_passes_through_untouched_and_the_middleware_reads_nothing():
    sent, receives = _serve("/data/graphql")
    assert sent == [_START, _BODY]  # same status, headers and body
    assert receives == []  # no receive() of its own: nothing extra relayed to the accepting loop


def test_the_admin_graphql_response_carries_the_schema_version():
    sent, _ = _serve("/admin/graphql", schema_version=42)
    assert _headers(sent[0])[b"x-schema-version"] == b"42"
    assert _headers(sent[0])[b"content-type"] == b"application/json"
    assert sent[1] == _BODY
    sent, _ = _serve("/admin/graphql/subpath", schema_version=42)
    assert _headers(sent[0])[b"x-schema-version"] == b"42"


def test_the_license_notice_rides_every_response_when_nagging(monkeypatch):
    monkeypatch.setattr("provisa.licensing.emit.should_nag", lambda: True)
    monkeypatch.setattr(
        "provisa.licensing.emit.current_state",
        lambda: SimpleNamespace(nag_text="Trial ended.\nPlease license."),
    )
    sent, _ = _serve("/data/rest/sales/orders")
    assert _headers(sent[0])[b"x-provisa-license-notice"] == b"Trial ended. Please license."
    assert sent[1] == _BODY  # the body is never touched


def test_no_notice_header_when_nagging_has_no_state(monkeypatch):
    monkeypatch.setattr("provisa.licensing.emit.should_nag", lambda: True)
    monkeypatch.setattr("provisa.licensing.emit.current_state", lambda: None)
    sent, _ = _serve("/data/graphql")
    assert sent == [_START, _BODY]


def test_a_client_that_disconnected_before_any_response_gets_499():
    async def gone(scope, receive, send):
        raise ClientDisconnect()

    sent, _ = _serve("/data/graphql", gone)
    assert [m["type"] for m in sent] == ["http.response.start", "http.response.body"]
    assert sent[0]["status"] == 499
    assert sent[1].get("body", b"") == b""


def test_a_disconnect_after_the_response_started_is_not_answered_twice():
    async def half(scope, receive, send):
        await send(dict(_START, headers=list(_START["headers"])))
        raise ClientDisconnect()

    with pytest.raises(ClientDisconnect):
        _serve("/data/graphql", half)


def test_non_http_scopes_are_not_wrapped():
    seen = []

    async def inner(scope, receive, send):
        seen.append((scope["type"], send))

    async def send(message):  # pragma: no cover - never called
        raise AssertionError

    middleware = ResponseHeadersMiddleware(inner, state=SimpleNamespace(schema_version=1))
    asyncio.run(middleware({"type": "websocket", "path": "/x"}, None, send))
    assert seen == [("websocket", send)]


def test_the_application_installs_it_as_a_class_not_a_base_http_middleware():
    import inspect

    import provisa.api.app as app_mod

    source = inspect.getsource(app_mod.create_app)
    assert "app.add_middleware(ResponseHeadersMiddleware, state=state)" in source
    assert '@app.middleware("http")' not in source
