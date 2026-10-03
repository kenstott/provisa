# Copyright (c) 2026 Kenneth Stott
# Canary: fa8c7af2-08ed-4971-ad8c-80fbd2b12adb
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1882 (amended 2026-09-29): the entire request runs on its connection thread.

Real Arrow Flight and Bolt servers, in process, on real sockets. For each transport:

- the request's coroutines execute with ``threading.get_ident()`` equal to the connection's own
  handler thread — not a shared loop's thread, and never via ``run_coroutine_threadsafe``;
- two concurrent connections run in parallel: each request BLOCKS its thread on a two-party
  barrier, which opens only if both are inside the request at the same moment. On one shared loop
  the first blocked request would stall the loop, the second could never arrive, and the barrier
  would break.

pgwire's counterpart lives in tests/integration/test_pgwire_integration.py
(TestPgwireConcurrentGovernanceIsolation).
"""

from __future__ import annotations

import asyncio
import json
import socket
import threading
import time
from types import SimpleNamespace
from unittest.mock import patch

import pyarrow.flight as flight
import pytest

from provisa.core.connection_loop import LOOP_POOL

pytestmark = [pytest.mark.integration]


def _free_port() -> int:
    from tests.port_lease import lease_port

    return lease_port()


def _no_cross_thread_hop(*args, **kwargs):
    del args, kwargs
    raise AssertionError("a connection-thread transport dispatched to another loop (REQ-1882)")


# -- Arrow Flight ------------------------------------------------------------------------------


def _flight_state(**extra) -> SimpleNamespace:
    """An unsecured single-org deployment — the data path is under test, not auth."""
    from provisa.cache import NoopCacheStore

    base = dict(
        auth_config=None,
        auth_middleware_active=False,
        multitenancy=False,
        security_high=False,
        rate_limiter=None,
        flight_global_cap=None,
        roles={"analyst": {}},
        response_cache_store=NoopCacheStore(),
    )
    base.update(extra)
    return SimpleNamespace(**base)


@pytest.fixture()
def flight_server():
    from provisa.api.flight.server import ProvisaFlightServer

    servers: list[ProvisaFlightServer] = []

    def _start(state) -> str:
        port = _free_port()
        location = f"grpc://127.0.0.1:{port}"
        server = ProvisaFlightServer(state, location=location)
        threading.Thread(target=server.serve, daemon=True).start()
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                    break
            except OSError:
                time.sleep(0.05)
        servers.append(server)
        return location

    yield _start
    for server in servers:
        server.shutdown()


def test_flight_governance_runs_on_the_handler_thread_in_parallel(flight_server):
    from provisa.api.flight.server import ProvisaFlightServer
    from provisa.executor.result import QueryResult

    location = flight_server(_flight_state())

    handler_idents: set[int] = set()
    govern_idents: list[int] = []
    both_governing = threading.Barrier(2)
    real_on_loop = ProvisaFlightServer._do_get_on_loop

    def _recording_do_get(self, request, ticket):
        handler_idents.add(threading.get_ident())
        return real_on_loop(self, request, ticket)

    async def _blocking_govern(sql, role_id, state, serve_cached=False):
        del sql, state
        govern_idents.append(threading.get_ident())
        both_governing.wait(timeout=10)  # blocks this RPC's thread AND its loop
        return QueryResult(rows=[(role_id,)], column_names=["role"])

    results: dict[str, object] = {}

    def _client(tag: str) -> None:
        client = flight.connect(location)
        try:
            ticket = flight.Ticket(json.dumps({"query": "SELECT 1", "role": "analyst"}).encode())
            results[tag] = client.do_get(ticket).read_all().to_pylist()
        except Exception as exc:  # surfaced by the assertion below with the real cause
            results[tag] = exc
        finally:
            client.close()

    with (
        patch.object(ProvisaFlightServer, "_do_get_on_loop", _recording_do_get),
        patch("provisa.pgwire._pipeline.govern_batch_final_plan_with_fn", _blocking_govern),
        patch("asyncio.run_coroutine_threadsafe", _no_cross_thread_hop),
    ):
        threads = [threading.Thread(target=_client, args=(t,)) for t in ("a", "b")]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)

    assert results == {"a": [{"role": "analyst"}], "b": [{"role": "analyst"}]}, results
    assert both_governing.broken is False
    assert len(govern_idents) == 2
    assert len(set(govern_idents)) == 2, "the two RPCs governed on one thread"
    assert set(govern_idents) <= handler_idents, (
        "governance ran on a thread that is not the RPC's handler thread"
    )


class _RecordingDirectStream:
    """A DIRECT server-side cursor that records which thread each fetch/close runs on."""

    column_names = ["id"]
    column_types = ["integer"]

    def __init__(self, seen: list[tuple[str, int]]) -> None:
        self._seen = seen
        self._batches = [[(1,), (2,)], [(3,)]]

    async def fetch(self, size: int) -> list[tuple]:
        del size
        self._seen.append(("fetch", threading.get_ident()))
        return self._batches.pop(0) if self._batches else []

    async def close(self) -> None:
        self._seen.append(("close", threading.get_ident()))


def test_flight_direct_stream_is_pumped_on_the_handler_thread_and_returns_its_loop(
    flight_server,
):
    """A DIRECT scan streams from the source cursor AFTER do_get returns (pyarrow drains it on
    the same handler thread). Every fetch and the cursor close must run on that thread, on the
    RPC's own loop, and the loop must be checked back in once the stream ends."""
    from provisa.api.flight.server import ProvisaFlightServer
    from provisa.federation.runtime import EngineRuntime
    from provisa.pgwire._pipeline import _mint_stamp, _Plan
    from provisa.transpiler.router import Route

    seen: list[tuple[str, int]] = []
    handler_idents: set[int] = set()

    class _Pools:
        def has(self, source_id: str) -> bool:
            return source_id == "src"

        def supports_stream(self, source_id: str) -> bool:
            return source_id == "src"

        async def open_stream(self, source_id, sql, params):
            del source_id, sql, params
            seen.append(("open", threading.get_ident()))
            return _RecordingDirectStream(seen)

    engine = EngineRuntime.__new__(EngineRuntime)  # only execute_native_stream is exercised
    location = flight_server(_flight_state(federation_engine=engine, source_pools=_Pools()))

    plan = _Plan(
        route=Route.DIRECT,
        sql="SELECT id FROM t",
        source_id="src",
        dialect="postgres",
        exec_params=[],
        stamp=_mint_stamp(),
    )

    async def _govern(sql, role_id, state, serve_cached=False):
        del sql, role_id, state
        return plan

    async def _finalize(plan_, status_code, state=None, *, cache_hit=False, defer_to_drain=False):
        del plan_, status_code, state, cache_hit, defer_to_drain

    real_on_loop = ProvisaFlightServer._do_get_on_loop

    def _recording_do_get(self, request, ticket):
        handler_idents.add(threading.get_ident())
        return real_on_loop(self, request, ticket)

    checked_out: list[int] = []
    checked_in: list[int] = []
    real_checkout, real_checkin = LOOP_POOL.checkout, LOOP_POOL.checkin

    def _checkout():
        cl = real_checkout()
        checked_out.append(id(cl))
        return cl

    def _checkin(cl):
        checked_in.append(id(cl))
        real_checkin(cl)

    with (
        patch.object(LOOP_POOL, "checkout", _checkout),
        patch.object(LOOP_POOL, "checkin", _checkin),
        patch.object(ProvisaFlightServer, "_do_get_on_loop", _recording_do_get),
        patch("provisa.pgwire._pipeline.govern_batch_final_plan_with_fn", _govern),
        patch("provisa.pgwire._pipeline.finalize_audit", _finalize),
        patch("asyncio.run_coroutine_threadsafe", _no_cross_thread_hop),
    ):
        client = flight.connect(location)
        try:
            ticket = flight.Ticket(json.dumps({"query": "SELECT 1", "role": "analyst"}).encode())
            rows = client.do_get(ticket).read_all().to_pylist()
        finally:
            client.close()

    assert rows == [{"id": 1}, {"id": 2}, {"id": 3}]
    assert [kind for kind, _ in seen] == ["open", "fetch", "fetch", "fetch", "close"]
    assert len(handler_idents) == 1
    (handler,) = handler_idents
    assert {ident for _, ident in seen} == {handler}, "the cursor was pumped off the handler thread"
    # The stream held the RPC's loop past do_get's return and handed it back once it ended.
    assert checked_out and sorted(checked_in) == sorted(checked_out)


# -- Bolt --------------------------------------------------------------------------------------


def test_bolt_connection_runs_on_its_own_thread_in_parallel():
    from provisa.bolt import server as bolt_server
    from provisa.bolt.messages import MAGIC

    serving_idents: list[int] = []
    thread_names: list[str] = []
    both_serving = threading.Barrier(2)

    async def _blocking_session(reader, writer):
        del reader
        serving_idents.append(threading.get_ident())
        thread_names.append(threading.current_thread().name)
        both_serving.wait(timeout=10)  # blocks this connection's thread AND its loop
        writer.write(b"ok")
        await writer.drain()

    listener = bolt_server.start_bolt_server("127.0.0.1", 0, None)
    replies: dict[str, bytes] = {}

    def _client(tag: str) -> None:
        with socket.create_connection(("127.0.0.1", listener.port), timeout=15) as s:
            s.sendall(MAGIC + b"\x00" * 16)
            replies[tag] = s.recv(2)

    try:
        with (
            patch.object(bolt_server, "_bolt_handshake_and_serve", _blocking_session),
            patch("asyncio.run_coroutine_threadsafe", _no_cross_thread_hop),
        ):
            threads = [threading.Thread(target=_client, args=(t,)) for t in ("a", "b")]
            for t in threads:
                t.start()
            for t in threads:
                t.join(timeout=30)
    finally:
        listener.close()

    assert replies == {"a": b"ok", "b": b"ok"}
    assert both_serving.broken is False
    assert len(set(serving_idents)) == 2, "the two Bolt connections ran on one thread"
    assert thread_names == ["bolt-conn", "bolt-conn"]
    assert threading.get_ident() not in serving_idents


# -- gRPC --------------------------------------------------------------------------------------


class _Msg:
    """A response message: what _handle_query meters (ByteSize) and the wire carries (bytes)."""

    def __init__(self, payload: bytes) -> None:
        self.payload = payload

    def ByteSize(self) -> int:  # noqa: N802  # protobuf's method name
        return len(self.payload)


def test_grpc_governance_runs_on_the_rpc_thread_in_parallel():
    """Each RPC's governance/execution body runs on the RPC's own pool thread, on its own loop,
    and two RPCs govern at the same moment (the barrier opens only if both are inside)."""
    from concurrent.futures import ThreadPoolExecutor

    import grpc

    from provisa.core.connection_loop import current_connection_loop
    from provisa.grpc.auth import AuthInterceptor
    from provisa.grpc.server import ProvisaServicer

    state = SimpleNamespace(
        auth_config=None, auth_middleware_active=False, multitenancy=False, security_high=False
    )
    servicer = ProvisaServicer(state, SimpleNamespace(), SimpleNamespace())
    behavior_idents: set[int] = set()
    govern_idents: list[int] = []
    both_governing = threading.Barrier(2)

    async def _blocking_govern(self, request, context, type_name, role_id):
        del self, request, context, type_name
        ident = threading.get_ident()
        assert current_connection_loop().owner == ident
        govern_idents.append(ident)
        both_governing.wait(timeout=10)  # blocks this RPC's thread AND its loop
        yield _Msg(role_id.encode())

    query_orders = servicer.QueryOrders

    def _recording_behavior(request, context):
        behavior_idents.add(threading.get_ident())
        yield from query_orders(request, context)

    handler = grpc.unary_stream_rpc_method_handler(
        _recording_behavior, response_serializer=lambda m: m.payload
    )
    server = grpc.server(ThreadPoolExecutor(max_workers=4), interceptors=[AuthInterceptor(state)])
    server.add_generic_rpc_handlers(
        (grpc.method_handlers_generic_handler("provisa.Provisa", {"QueryOrders": handler}),)
    )
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    results: dict[str, object] = {}

    def _client(tag: str) -> None:
        with grpc.insecure_channel(f"127.0.0.1:{port}") as channel:
            call = channel.unary_stream("/provisa.Provisa/QueryOrders")
            try:
                results[tag] = list(call(b"", metadata=(("x-provisa-role", "analyst"),)))
            except grpc.RpcError as exc:  # surfaced by the assertion below with the real cause
                results[tag] = exc

    try:
        with (
            patch.object(ProvisaServicer, "_handle_query_bound", _blocking_govern),
            patch("asyncio.run_coroutine_threadsafe", _no_cross_thread_hop),
        ):
            threads = [threading.Thread(target=_client, args=(t,)) for t in ("a", "b")]
            for t in threads:
                t.start()
            for t in threads:
                t.join(timeout=30)
    finally:
        server.stop(grace=None).wait()

    assert results == {"a": [b"analyst"], "b": [b"analyst"]}, results
    assert both_governing.broken is False
    assert len(set(govern_idents)) == 2, "the two RPCs governed on one thread"
    assert set(govern_idents) == behavior_idents, "governance ran off the RPC's handler thread"
    assert threading.get_ident() not in govern_idents


# -- HTTP / GraphQL ----------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_graphql_governance_runs_on_the_request_thread_in_parallel():
    """POST /data/graphql through the real data router behind RequestThreadMiddleware: the
    governed pipeline entry (_handle_query) runs on the request's own thread and loop — not the
    ASGI loop — and two requests govern at the same moment."""
    import httpx
    from fastapi import FastAPI
    from fastapi.responses import JSONResponse
    from graphql import build_schema

    from provisa.api.data import endpoint
    from provisa.core import request_thread
    from provisa.core.connection_loop import current_connection_loop
    from provisa.core.request_thread import RequestThreadMiddleware

    from provisa.api.app import AppState

    schema = build_schema("type Query { orders: Int }")
    # A real AppState, so every attribute the GraphQL path reads exists with its own default; only
    # what this request needs is set.
    state = AppState()
    state.schemas = {"analyst": schema}
    state.contexts = {"analyst": object()}  # type: ignore[dict-item]
    state.rls_contexts = {}
    state.roles = {"analyst": {"id": "analyst", "capabilities": [], "domain_access": ["*"]}}
    state.apq_cache = None
    request_idents: set[int] = set()
    govern_idents: list[int] = []
    both_governing = threading.Barrier(2)
    real_serve = request_thread._serve

    def _recording_serve(make_coro, ctx):
        request_idents.add(threading.get_ident())
        return real_serve(make_coro, ctx)

    async def _blocking_handle_query(document, ctx, rls, state_, variables, role, *args, **kw):
        del document, ctx, rls, state_, variables, role, args, kw
        ident = threading.get_ident()
        assert current_connection_loop().owner == ident
        govern_idents.append(ident)
        both_governing.wait(timeout=10)  # blocks this request's thread AND its loop
        return JSONResponse({"data": {"orders": 1}})

    async def _awake(_state) -> None:
        return None

    app = FastAPI()
    app.include_router(endpoint.router)
    app.add_middleware(RequestThreadMiddleware)
    front_ident = threading.get_ident()

    with (
        patch("provisa.api.app.state", state),
        patch.object(endpoint, "_check_role_capability", lambda role, cap: None),
        patch.object(endpoint, "ensure_engine_awake", _awake),
        patch.object(endpoint, "_handle_query", _blocking_handle_query),
        patch.object(request_thread, "_serve", _recording_serve),
    ):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:

            async def _post():
                return await client.post(
                    "/data/graphql",
                    json={"query": "{ orders }"},
                    headers={"X-Provisa-Role": "analyst"},
                )

            responses = await asyncio.gather(_post(), _post())

    assert [r.status_code for r in responses] == [200, 200], [r.text for r in responses]
    assert [r.json() for r in responses] == [{"data": {"orders": 1}}] * 2
    assert both_governing.broken is False
    assert len(set(govern_idents)) == 2, "the two requests governed on one thread"
    assert set(govern_idents) == request_idents, "governance ran off the request's thread"
    assert front_ident not in govern_idents, "governance ran on the ASGI loop's thread"


# -- MCP ---------------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_mcp_run_sql_governs_on_the_request_thread_in_parallel(monkeypatch):
    """An MCP run_sql call: the MCP loop only dispatches; the governed tool body runs on its own
    request thread and loop, and two calls govern at the same moment."""
    from provisa.api.mcp import server as mcp_server
    from provisa.api.mcp import tools
    from provisa.core import request_thread
    from provisa.core.connection_loop import current_connection_loop

    monkeypatch.setenv("PROVISA_MCP_ROLE", "analyst")
    request_idents: set[int] = set()
    govern_idents: list[int] = []
    both_governing = threading.Barrier(2)
    real_serve = request_thread._serve

    def _recording_serve(make_coro, ctx):
        request_idents.add(threading.get_ident())
        return real_serve(make_coro, ctx)

    async def _blocking_run_sql(state, role, sql, *, limit=None, offset=0):
        del state, sql, limit, offset
        ident = threading.get_ident()
        assert current_connection_loop().owner == ident
        govern_idents.append(ident)
        both_governing.wait(timeout=10)  # blocks this call's thread AND its loop
        return {"rows": [{"role": role}]}

    mcp = mcp_server.build_mcp_server(SimpleNamespace())
    front_ident = threading.get_ident()
    with (
        patch.object(tools, "run_sql", _blocking_run_sql),
        patch.object(request_thread, "_serve", _recording_serve),
    ):
        results = await asyncio.gather(
            mcp.call_tool("run_sql", {"sql": "SELECT 1"}),
            mcp.call_tool("run_sql", {"sql": "SELECT 1"}),
        )

    assert len(results) == 2
    assert both_governing.broken is False
    assert len(set(govern_idents)) == 2, "the two calls governed on one thread"
    assert set(govern_idents) == request_idents, "governance ran off the call's request thread"
    assert front_ident not in govern_idents, "governance ran on the MCP loop's thread"
