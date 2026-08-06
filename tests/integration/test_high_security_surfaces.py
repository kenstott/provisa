# Copyright (c) 2026 Kenneth Stott
# Canary: 3c7dc29e-b67a-4c1a-ba20-0bd4a225b94b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Every surface refuses plaintext reads in high-security mode, over real transports (REQ-693).

The gate had been asserted only by unit tests calling boolean helpers, and six surfaces reached
production without one — /data/ingest, /data/subscribe, the /data/grpc proxies, Bolt, MCP and the
Flight and gRPC wire protocols. This file boots each surface for real and attempts the read a
client would attempt, so a seventh un-gated surface fails here the day it is added.

Refusal takes two shapes, by protocol. pgwire, Bolt and MCP never open their port: none of the
three can carry proof that the client decrypts for itself. HTTP, gRPC and Arrow Flight stay up and
demand that proof per call — a client-side decryption key — because they are the transports an
encrypting client actually uses.
"""

from __future__ import annotations

import asyncio
import json
import logging
import socket
import threading
from types import SimpleNamespace

import grpc
import grpc.aio
import httpx
import pyarrow.flight as flight
import pytest
import uvicorn
from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Route

from provisa.api.flight.server import ProvisaFlightServer
from provisa.grpc.auth import AuthInterceptor
from provisa.security.high_security import KMS_KEY_HEADER, HighSecurityMiddleware

pytestmark = pytest.mark.integration

_KMS_KEY = "arn:aws:kms:us-east-1:000000000000:key/high-security-surfaces"


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def _high_state() -> SimpleNamespace:
    """A deployment in high-security mode with authentication off.

    Auth off isolates what this file is about: every refusal below has to come from the
    high-security gate alone, never from a missing credential.
    """
    return SimpleNamespace(
        security_high=True,
        auth_config=None,
        auth_middleware_active=False,
        multitenancy=False,
        admin_db=None,
        roles={},
        wire_proto=None,
        server_cfg={},
        hostname="127.0.0.1",
        org_id="test",
    )


# -- HTTP data plane ---------------------------------------------------------------------------


def _data_app() -> Starlette:
    """The real middleware over stand-in handlers at the real /data paths.

    The handlers answer 200 unconditionally, so a 403 can only have come from the gate and a 200
    proves the request reached a handler that would have returned rows.
    """

    async def ok(_request):
        return JSONResponse({"rows": [{"ssn": "000-00-0000"}]})

    paths = (
        "/data/sql",
        "/data/graphql",
        "/data/rest/pets",
        "/data/jsonapi/pets",
        "/data/ingest/pg/orders",
        "/data/subscribe/orders",
        "/data/grpc/Orders",
        "/data/grpc-command/analyst",
        "/data/query",
        "/data/nl-to-sql",
        "/data/touch/orders",
        "/data/redirect/unwrap",
        "/data/sdl",
        "/data/introspection",
        "/data/domains",
    )
    app = Starlette(routes=[Route(p, ok, methods=["GET", "POST"]) for p in paths])
    app.add_middleware(HighSecurityMiddleware, state=_high_state())
    return app


@pytest.fixture(scope="module")
def http_base_url():
    """A real uvicorn listener, so every assertion below crosses a socket."""
    port = _free_port()
    config = uvicorn.Config(_data_app(), host="127.0.0.1", port=port, log_level="error")
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    deadline = 30
    while not server.started and deadline:
        threading.Event().wait(0.1)
        deadline -= 1
    assert server.started, "uvicorn did not come up"
    yield f"http://127.0.0.1:{port}"
    server.should_exit = True
    thread.join(timeout=10)


@pytest.mark.parametrize(
    "path",
    [
        "/data/sql",
        "/data/graphql",
        "/data/rest/pets",
        "/data/jsonapi/pets",
        "/data/ingest/pg/orders",
        "/data/subscribe/orders",
        "/data/grpc/Orders",
        "/data/grpc-command/analyst",
        "/data/query",
        "/data/nl-to-sql",
        "/data/touch/orders",
        "/data/redirect/unwrap",
    ],
)
def test_http_data_path_refused_without_a_decryption_key(http_base_url, path):
    response = httpx.post(f"{http_base_url}{path}", json={})
    assert response.status_code == 403, path
    assert "high-security" in response.json()["detail"]


@pytest.mark.parametrize("path", ["/data/sdl", "/data/introspection", "/data/domains"])
def test_http_schema_metadata_stays_reachable(http_base_url, path):
    """A client has to read the schema, and its @encrypted markings, before it can connect."""
    assert httpx.get(f"{http_base_url}{path}").status_code == 200


def test_http_data_path_served_with_a_decryption_key(http_base_url):
    response = httpx.post(f"{http_base_url}/data/sql", json={}, headers={KMS_KEY_HEADER: _KMS_KEY})
    assert response.status_code == 200


# -- Arrow Flight ------------------------------------------------------------------------------


@pytest.fixture(scope="module")
def flight_location():
    port = _free_port()
    loop = asyncio.new_event_loop()
    loop_thread = threading.Thread(target=loop.run_forever, daemon=True)
    loop_thread.start()
    server = ProvisaFlightServer(_high_state(), location=f"grpc://127.0.0.1:{port}", main_loop=loop)
    serve_thread = threading.Thread(target=server.serve, daemon=True)
    serve_thread.start()
    yield f"grpc://127.0.0.1:{port}"
    server.shutdown()
    serve_thread.join(timeout=10)
    loop.call_soon_threadsafe(loop.stop)
    loop_thread.join(timeout=5)


def test_flight_query_ticket_refused_without_a_decryption_key(flight_location):
    client = flight.connect(flight_location)
    ticket = flight.Ticket(json.dumps({"query": "SELECT 1", "role": "analyst"}).encode())
    with pytest.raises(flight.FlightServerError, match="high-security mode"):
        client.do_get(ticket)


# -- gRPC --------------------------------------------------------------------------------------

_METHOD = "/provisa.Provisa/QueryOrders"


class _GenericHandler(grpc.GenericRpcHandler):
    """Answers any method, so a refusal can only have come from the interceptor."""

    def service(self, handler_call_details):  # noqa: ARG002  # every method resolves the same way
        async def behavior(request, context):  # noqa: ARG001  # neither is read
            return b"rows"

        return grpc.unary_unary_rpc_method_handler(behavior)


@pytest.fixture
async def grpc_channel():
    port = _free_port()
    server = grpc.aio.server(interceptors=[AuthInterceptor(_high_state())])
    server.add_generic_rpc_handlers((_GenericHandler(),))
    server.add_insecure_port(f"127.0.0.1:{port}")
    await server.start()
    async with grpc.aio.insecure_channel(f"127.0.0.1:{port}") as channel:
        yield channel
    await server.stop(grace=None)


@pytest.mark.asyncio
async def test_grpc_call_refused_without_a_decryption_key(grpc_channel):
    call = grpc_channel.unary_unary(_METHOD, lambda b: b, lambda b: b)
    with pytest.raises(grpc.aio.AioRpcError) as excinfo:
        await call(b"")
    assert excinfo.value.code() == grpc.StatusCode.PERMISSION_DENIED
    assert KMS_KEY_HEADER in (excinfo.value.details() or "")


@pytest.mark.asyncio
async def test_grpc_call_served_with_a_decryption_key(grpc_channel):
    call = grpc_channel.unary_unary(_METHOD, lambda b: b, lambda b: b)
    assert await call(b"", metadata=((KMS_KEY_HEADER, _KMS_KEY),)) == b"rows"


# -- ports that never open ---------------------------------------------------------------------


def _listening(port: int) -> bool:
    with socket.socket() as probe:
        probe.settimeout(2)
        return probe.connect_ex(("127.0.0.1", port)) == 0


@pytest.mark.asyncio
async def test_pgwire_bolt_and_mcp_never_bind_in_high_mode(monkeypatch):
    """Boot the real optional-server startup in high mode and probe each port."""
    from provisa.api import app as app_module
    from provisa.api import app_startup

    ports = {name: _free_port() for name in ("PGWIRE", "BOLT", "MCP")}
    for name, port in ports.items():
        monkeypatch.setenv(f"PROVISA_{name}_PORT", str(port))
    # The remaining servers _start_servers touches are not what this test is about; give them no
    # port to bind and no proto to compile so only the three gated listeners are exercised.
    monkeypatch.setenv("PROVISA_AIRPORT_PORT", "0")
    monkeypatch.setenv("FLIGHT_PORT", str(_free_port()))
    monkeypatch.setattr(app_module, "state", _high_state(), raising=False)

    await app_startup._start_servers(logging.getLogger("test.high_security"))

    for name, port in ports.items():
        assert not _listening(port), f"{name} bound its port in high-security mode"
