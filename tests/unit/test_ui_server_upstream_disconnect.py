# Copyright (c) 2026 Kenneth Stott
# Canary: 2f6b9d03-41ce-4a7e-b8d2-95e0c1a4f7d6
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An upstream API that drops a request it accepted is answered as one that is unavailable
(REQ-2006).

A deploy recreates the sandbox tier's container: an open under way there loses its connection, the
API answering nothing. The proxy handled only a refused connection and a timeout, so the dropped
one escaped it as a bare ASGI 500 ("Internal Server Error"), and the groupexposure demo's open was
answered that on cloud. The tier is coming back: the visitor is told it is starting, as for a
refused connection."""

# Requirements: REQ-2006

from __future__ import annotations

import httpx
import pytest
from fastapi.testclient import TestClient

from provisa import sandbox_wake

INSTANCE = "projects/p/zones/z/instances/provisa-saas-sandbox"
_REAL_CLIENT = httpx.AsyncClient


@pytest.fixture(autouse=True)
def _no_use_seen(monkeypatch):
    from provisa.api import demo_sandbox

    monkeypatch.setattr(demo_sandbox, "_last_used", None)
    monkeypatch.setattr(demo_sandbox, "_opening", 0)


def _ui(monkeypatch, tmp_path, instance, dropped: type[httpx.TransportError]) -> TestClient:
    """The UI server, its sandbox tier accepting each request and dropping it with ``dropped``;
    the Compute Engine API finds the tier's VM running."""
    import provisa.ui_server as ui_server

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "metadata.google.internal":
            return httpx.Response(200, json={"access_token": "tok"})
        if request.url.host == "compute.googleapis.com":
            return httpx.Response(200, json={"status": "RUNNING"})
        raise dropped("Server disconnected without sending a response.", request=request)

    def _client(*args, **kwargs):
        kwargs.pop("transport", None)
        return _REAL_CLIENT(*args, transport=httpx.MockTransport(handler), **kwargs)

    async def _timeout():
        return 5.0

    (tmp_path / "index.html").write_text("<html></html>")
    monkeypatch.setattr(ui_server, "STATIC_DIR", tmp_path)
    monkeypatch.setattr(ui_server, "SANDBOX_API_URL", "http://sandbox.internal:8000")
    monkeypatch.setattr(ui_server, "proxy_timeout_s", _timeout)
    monkeypatch.setattr(httpx, "AsyncClient", _client)
    monkeypatch.setattr(sandbox_wake, "SANDBOX_INSTANCE", instance)
    monkeypatch.setattr(sandbox_wake, "_last_call", float("-inf"))
    return TestClient(ui_server.app, raise_server_exceptions=False)


@pytest.mark.parametrize("dropped", [httpx.RemoteProtocolError, httpx.ReadError])
def test_an_open_the_tier_dropped_is_told_the_tier_is_starting(monkeypatch, tmp_path, dropped):
    response = _ui(monkeypatch, tmp_path, INSTANCE, dropped).post("/public/demos/groupexposure")
    assert response.status_code == 503, response.text
    assert response.json()["code"] == "sandbox.starting"
    assert response.headers["Retry-After"] == "60"


@pytest.mark.parametrize("dropped", [httpx.RemoteProtocolError, httpx.ReadError])
def test_with_no_vm_to_start_a_dropped_request_is_a_502(monkeypatch, tmp_path, dropped):
    response = _ui(monkeypatch, tmp_path, None, dropped).post("/public/demos/groupexposure")
    assert response.status_code == 502, response.text
