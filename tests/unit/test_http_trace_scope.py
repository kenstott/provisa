# Copyright (c) 2026 Kenneth Stott
# Canary: 4a8d2f17-6b3c-4e91-9d05-7e1c8a4b2f63
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An HTTP request's debug-trace window is resolved in middleware, before its body is read
(REQ-1910).

The ASGI ``receive`` span is opened when the framework reads the request body — before the
endpoint, and so before the pipeline's own request-entry resolution. A request an open window
covers was therefore traced in debug detail from the endpoint on, with its receive span already
dropped as a normal-detail child. The org-routing middleware is the first point where the
request's org and role are both bound; resolving there puts the whole request, receive span
included, under the window.
"""

# Requirements: REQ-1910

from __future__ import annotations

import inspect
from types import SimpleNamespace

import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import SpanKind

from provisa import otel_compat
from provisa.api.http_trace_scope import http_trace_scope
from provisa.api.otel_setup import _bind_http_request_span, _trace_detail_sampler
from provisa.core import trace_scope as ts
from provisa.core.database import Database, create_engine_from_url
from provisa.core.request_context import reset_current_org, set_current_org
from provisa.core.schema_admin import debug_trace_hint_roles, debug_trace_windows, metadata


@pytest.fixture(autouse=True)
def _no_request_scope_leak():
    span_token = otel_compat._request_span.set(None)
    transport_token = otel_compat._request_transport.set(None)
    detail_token = otel_compat._request_detail.set(None)
    ts.invalidate()
    yield
    ts.invalidate()
    otel_compat._request_detail.reset(detail_token)
    otel_compat._request_transport.reset(transport_token)
    otel_compat._request_span.reset(span_token)


@pytest.fixture
def served(tmp_path):
    """A deployment serving org ``acme`` with a real control plane, and a tracer that samples the
    way setup_otel's does."""
    db = Database(
        create_engine_from_url(f"sqlite+pysqlite:///{tmp_path / 'cp.db'}"), name="trace-scope"
    )
    with db.engine.begin() as conn:
        metadata.create_all(conn, tables=[debug_trace_windows, debug_trace_hint_roles])
    exporter = InMemorySpanExporter()
    provider = TracerProvider(sampler=_trace_detail_sampler(1.0))
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    yield SimpleNamespace(
        db=db,
        state=SimpleNamespace(admin_db=db, org_id="acme"),
        tracer=provider.get_tracer("test"),
        exporter=exporter,
    )
    provider.shutdown()


async def _request(served, *, role: str | None, org: str = "acme") -> list[str]:
    """One HTTP request as the stack serves it: the instrumentor opens the server span and binds
    it, the middleware runs, then the framework reads the body (the ``receive`` span) and the
    endpoint runs. Returns the names of the spans that were recorded."""
    served.exporter.clear()
    scope = {"type": "http", "path": "/data/graphql", "state": {}}
    if role is not None:
        scope["state"]["role"] = role
    with served.tracer.start_as_current_span("POST /data/graphql", kind=SpanKind.SERVER) as server:
        _bind_http_request_span(server, scope)
        token = set_current_org(org)  # as the org-routing middleware binds it (REQ-1266)
        try:
            async with http_trace_scope(served.state, scope):
                with served.tracer.start_as_current_span("POST /data/graphql http receive"):
                    pass
            after_dispatch.append(otel_compat._request_detail.get())
        finally:
            reset_current_org(token)
    return [s.name for s in served.exporter.get_finished_spans()]


async def _window(db, scope: str, org_id: str, target: str | None = None) -> None:
    await ts.start_window(
        db, scope=scope, org_id=org_id, target=target, minutes=15, created_by=None
    )


_SERVER = "POST /data/graphql"
after_dispatch: list[str | None] = []  # the bound detail once each request's dispatch returned
_RECEIVE = "POST /data/graphql http receive"


@pytest.mark.asyncio
async def test_a_request_no_window_covers_records_no_receive_span(served):
    assert await _request(served, role="analyst") == [_SERVER]


@pytest.mark.asyncio
async def test_an_org_window_covers_the_receive_span(served):
    await _window(served.db, "org", "acme")
    ts.invalidate()
    assert await _request(served, role="analyst") == [_RECEIVE, _SERVER]


@pytest.mark.asyncio
async def test_a_role_window_covers_only_that_roles_requests(served):
    await _window(served.db, "role", "acme", "analyst")
    ts.invalidate()
    assert await _request(served, role="analyst") == [_RECEIVE, _SERVER]
    assert await _request(served, role="steward") == [_SERVER]


@pytest.mark.asyncio
async def test_a_window_on_another_org_does_not_cover_the_request(served):
    await _window(served.db, "org", "globex")
    ts.invalidate()
    assert await _request(served, role="analyst") == [_SERVER]
    # ...and the org the request is routed to is the one the window is matched against.
    assert await _request(served, role="analyst", org="globex") == [_RECEIVE, _SERVER]


@pytest.mark.asyncio
async def test_a_debug_request_does_not_leave_the_level_for_the_next_request(served):
    await _window(served.db, "role", "acme", "analyst")
    ts.invalidate()
    assert await _request(served, role="analyst") == [_RECEIVE, _SERVER]
    assert await _request(served, role="steward") == [_SERVER]


@pytest.mark.asyncio
async def test_a_request_with_no_resolved_role_is_left_to_the_endpoint(served):
    """Health, docs and unauthenticated paths resolve no role; nothing is decided for them here."""
    await _window(served.db, "org", "acme")
    ts.invalidate()
    assert await _request(served, role=None) == [_SERVER]


@pytest.mark.asyncio
async def test_the_detail_is_unbound_when_the_dispatch_returns(served):
    await _window(served.db, "org", "acme")
    ts.invalidate()
    after_dispatch.clear()
    assert await _request(served, role="analyst") == [_RECEIVE, _SERVER]
    assert after_dispatch == [None]  # debug inside the block, nothing bound after it


def test_the_org_routing_middleware_serves_every_dispatch_inside_the_scope():
    """Every org-bound dispatch of the org-routing middleware — the deployment's own org included
    (REQ-1266) — runs inside the request's trace scope. The one dispatch before it serves a request
    with no org, which no org's debug window covers."""
    import provisa.api.app as app_mod

    source = inspect.getsource(app_mod.create_app)
    middleware = source[
        source.index("class _OrgRoutingMiddleware") : source.index(
            "app.add_middleware(_OrgRoutingMiddleware)"
        )
    ]
    routed = middleware[middleware.index("selected_env = await resolve_selected_env") :]
    dispatches = routed.split("await self.app(scope, receive, send)")[:-1]
    assert len(dispatches) == 1
    for chunk in dispatches:
        assert chunk.rstrip().endswith("async with http_trace_scope(state, scope):")


# REQ-1910 (amended 2026-10-05): a small body is read on the accepting loop before this middleware
# resolves the window (REQ-1882). No span is opened at that read; its timing travels with the
# hand-off and the receive span is emitted here, with those times, when the detail is debug.


async def _preread_request(served, monkeypatch, *, role: str) -> list:
    """A request whose small body the accepting loop already read: the hand-off carries the
    read's record and nothing reads the body again. Returns the recorded spans."""
    monkeypatch.setattr(otel_compat, "_http_tracer", lambda: served.tracer)
    served.exporter.clear()
    scope = {"type": "http", "path": "/data/graphql", "state": {"role": role}}
    with served.tracer.start_as_current_span(_SERVER, kind=SpanKind.SERVER) as server:
        _bind_http_request_span(server, scope)
        scope["state"][otel_compat.RECEIVE_RECORD] = otel_compat.ReceiveRecord(
            server, 1_000, 2_000, 42
        )
        token = set_current_org("acme")  # as the org-routing middleware binds it (REQ-1266)
        try:
            async with http_trace_scope(served.state, scope):
                pass
        finally:
            reset_current_org(token)
    assert otel_compat.RECEIVE_RECORD not in scope["state"]  # emitted (or not) once
    return list(served.exporter.get_finished_spans())


@pytest.mark.asyncio
async def test_a_preread_body_under_a_window_gets_its_receive_span_with_the_measured_times(
    served, monkeypatch
):
    await _window(served.db, "org", "acme")
    ts.invalidate()
    spans = await _preread_request(served, monkeypatch, role="analyst")
    (receive,) = [s for s in spans if s.name == _RECEIVE]
    (server,) = [s for s in spans if s.name == _SERVER]
    assert (receive.start_time, receive.end_time) == (1_000, 2_000)
    assert receive.parent is not None and receive.parent.span_id == server.context.span_id
    assert receive.attributes["http.request.body.size"] == 42


@pytest.mark.asyncio
async def test_a_preread_body_no_window_covers_gets_no_receive_span(served, monkeypatch):
    spans = await _preread_request(served, monkeypatch, role="analyst")
    assert [s.name for s in spans] == [_SERVER]
