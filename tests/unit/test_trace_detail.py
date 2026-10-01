# Copyright (c) 2026 Kenneth Stott
# Canary: 4f1b9e63-28a7-4c05-b3d9-7e6a0c5d2f18
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1910: trace detail — one span per request in normal detail, the waterfall in debug."""

# Requirements: REQ-1910

from __future__ import annotations

import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import SpanKind

from provisa import otel_compat
from provisa.api.otel_setup import _trace_detail_sampler
from provisa.otel_compat import (
    annotate_request,
    record_query,
    record_stage,
    register_query_instruments,
    request_span,
    reset_trace_detail,
    set_trace_detail,
    stage,
    timed_stage,
    trace_detail,
)


@pytest.fixture
def traced():
    """A tracer on a provider that samples the way setup_otel's does, and its exporter."""
    exporter = InMemorySpanExporter()
    provider = TracerProvider(sampler=_trace_detail_sampler(1.0))
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    yield provider.get_tracer("test"), exporter
    provider.shutdown()


@pytest.fixture(autouse=True)
def _no_request_scope_leak():
    """The HTTP hook binds the request scope for the life of the request's task and never resets
    it; a test that calls it must not hand its scope to the next test."""
    span_token = otel_compat._request_span.set(None)
    transport_token = otel_compat._request_transport.set(None)
    detail_token = otel_compat._request_detail.set(None)
    yield
    otel_compat._request_detail.reset(detail_token)
    otel_compat._request_transport.reset(transport_token)
    otel_compat._request_span.reset(span_token)


@pytest.fixture
def debug_detail():
    token = set_trace_detail("debug")
    yield
    reset_trace_detail(token)


def _by_name(exporter) -> dict:
    return {s.name: s for s in exporter.get_finished_spans()}


# -- the switch ------------------------------------------------------------------------------------


def test_detail_defaults_to_normal_and_binds_per_request():
    assert trace_detail() == "normal"
    token = set_trace_detail("debug")
    try:
        assert trace_detail() == "debug"
    finally:
        reset_trace_detail(token)
    assert trace_detail() == "normal"


def test_an_unknown_detail_is_refused():
    with pytest.raises(ValueError, match="verbose"):
        set_trace_detail("verbose")
    with pytest.raises(ValueError, match="verbose"):
        otel_compat.configure_trace_detail("verbose")


def test_the_process_default_applies_when_no_request_binds_one(monkeypatch):
    monkeypatch.setattr(otel_compat, "_process_detail", "debug")
    assert trace_detail() == "debug"
    token = set_trace_detail("normal")
    try:
        assert trace_detail() == "normal"
    finally:
        reset_trace_detail(token)


# -- stage(): normal detail ------------------------------------------------------------------------


def test_normal_stage_creates_no_span_and_reports_on_the_request_span(traced):
    tracer, exporter = traced
    with request_span(tracer, "pgwire.query", transport="pgwire"):
        with stage(tracer, "provisa.query.direct", name="execute") as span:
            span.set_attribute("db.row_count", 3)
            span.set_attribute("db.statement", "SELECT secret FROM t WHERE id = 1")
    spans = exporter.get_finished_spans()
    assert [s.name for s in spans] == ["pgwire.query"]
    attrs = spans[0].attributes
    assert attrs["provisa.transport"] == "pgwire"
    assert attrs["db.row_count"] == 3
    assert attrs["stage.execute.ms"] >= 0
    assert "db.statement" not in attrs
    assert spans[0].kind is SpanKind.SERVER


def test_normal_stage_that_runs_twice_accumulates(traced):
    tracer, exporter = traced
    with request_span(tracer, "pgwire.query", transport="pgwire"):
        for _ in range(3):
            with stage(tracer, "rls.inject", name="govern"):
                pass
    attrs = exporter.get_finished_spans()[0].attributes
    assert attrs["stage.govern.count"] == 3
    assert attrs["stage.govern.ms"] >= 0


def test_normal_stage_that_raises_names_the_error_and_reraises(traced):
    tracer, exporter = traced
    with request_span(tracer, "pgwire.query", transport="pgwire"):
        with pytest.raises(ConnectionError):
            with stage(tracer, "direct.execute", name="execute"):
                raise ConnectionError("source down")
    attrs = exporter.get_finished_spans()[0].attributes
    assert attrs["stage.execute.error"] == "ConnectionError"
    assert attrs["stage.execute.ms"] >= 0


def test_record_stage_and_timed_stage_report_without_a_span(traced):
    tracer, exporter = traced

    @timed_stage("route")
    def decide() -> str:
        return "direct"

    with request_span(tracer, "pgwire.query", transport="pgwire"):
        assert decide() == "direct"
        record_stage("encode", 10.0, 10.25)
    spans = exporter.get_finished_spans()
    assert [s.name for s in spans] == ["pgwire.query"]
    assert spans[0].attributes["stage.encode.ms"] == 250.0
    assert spans[0].attributes["stage.route.ms"] >= 0


def test_annotate_request_sets_facts_and_keeps_statement_text_out_of_normal_detail(traced):
    tracer, exporter = traced
    with request_span(tracer, "pgwire.query", transport="pgwire"):
        annotate_request(provisa__route="direct", db__row_count=1, db__statement="SELECT 1")
        annotate_request(provisa__engine=None)
    attrs = exporter.get_finished_spans()[0].attributes
    assert attrs["provisa.route"] == "direct"
    assert attrs["db.row_count"] == 1
    assert "db.statement" not in attrs
    assert "provisa.engine" not in attrs


# -- stage(): debug detail -------------------------------------------------------------------------


def test_debug_stage_is_a_child_span_with_statement_text(traced, debug_detail):
    tracer, exporter = traced
    with request_span(tracer, "pgwire.query", transport="pgwire"):
        with stage(tracer, "provisa.query.direct", name="execute") as span:
            span.set_attribute("db.statement", "SELECT 1")
        annotate_request(db__statement="SELECT 1")
    spans = _by_name(exporter)
    assert set(spans) == {"pgwire.query", "provisa.query.direct"}
    child, request = spans["provisa.query.direct"], spans["pgwire.query"]
    assert child.parent.span_id == request.context.span_id
    assert child.attributes["db.statement"] == "SELECT 1"
    assert request.attributes["db.statement"] == "SELECT 1"
    # The request record reports the stage's duration in debug detail too.
    assert request.attributes["stage.execute.ms"] >= 0


# -- outside a request -----------------------------------------------------------------------------


def test_a_stage_outside_a_request_is_a_span_in_normal_detail(traced):
    """Background work (a refresh, a scheduler job) has no request span to report into."""
    tracer, exporter = traced
    with stage(tracer, "duckdb.execute", name="execute") as span:
        span.set_attribute("db.statement", "SELECT 1")
    spans = exporter.get_finished_spans()
    assert [s.name for s in spans] == ["duckdb.execute"]
    assert spans[0].attributes["db.statement"] == "SELECT 1"


def test_background_child_spans_are_recorded_in_normal_detail(traced):
    tracer, exporter = traced
    with tracer.start_as_current_span("discovery.analyze"):
        with tracer.start_as_current_span("discovery.collect_metadata"):
            pass
    assert set(_by_name(exporter)) == {"discovery.analyze", "discovery.collect_metadata"}


# -- the sampler: instrumentor spans ---------------------------------------------------------------


def test_normal_detail_drops_an_instrumentor_child_span_inside_a_request(traced):
    tracer, exporter = traced
    with request_span(tracer, "pgwire.query", transport="pgwire"):
        with tracer.start_as_current_span("GET", kind=SpanKind.CLIENT) as redis_span:
            assert not redis_span.is_recording()
            with tracer.start_as_current_span("nested"):
                pass
    assert [s.name for s in exporter.get_finished_spans()] == ["pgwire.query"]


def test_debug_detail_keeps_an_instrumentor_child_span_inside_a_request(traced, debug_detail):
    tracer, exporter = traced
    with request_span(tracer, "pgwire.query", transport="pgwire"):
        with tracer.start_as_current_span("GET", kind=SpanKind.CLIENT):
            pass
    assert set(_by_name(exporter)) == {"pgwire.query", "GET"}


def test_normal_detail_drops_a_server_spans_child_before_the_request_scope_is_bound(traced):
    """ASGI receive/send spans are children of the HTTP server span, opened by the instrumentor
    in a context nothing of ours has run in yet."""
    tracer, exporter = traced
    with tracer.start_as_current_span("POST /data/graphql", kind=SpanKind.SERVER):
        with tracer.start_as_current_span("POST /data/graphql http receive"):
            pass
    assert [s.name for s in exporter.get_finished_spans()] == ["POST /data/graphql"]


def test_the_http_server_span_hook_binds_the_request_scope(traced):
    from provisa.api.otel_setup import _bind_http_request_span

    tracer, exporter = traced
    with tracer.start_as_current_span("POST /data/graphql", kind=SpanKind.SERVER) as server:
        _bind_http_request_span(server, {"path": "/data/graphql"})
        with stage(tracer, "cache.get", name="cache") as span:
            span.set_attribute("cache.hit", True)
    spans = exporter.get_finished_spans()
    assert [s.name for s in spans] == ["POST /data/graphql"]
    assert spans[0].attributes["provisa.transport"] == "graphql"
    assert spans[0].attributes["cache.hit"] is True
    assert spans[0].attributes["stage.cache.ms"] >= 0


# -- metrics do not depend on the detail -----------------------------------------------------------


class _Instrument:
    def __init__(self) -> None:
        self.calls: list[tuple] = []

    def add(self, value, attrs) -> None:
        self.calls.append((value, attrs))

    def record(self, value, attrs) -> None:
        self.calls.append((value, attrs))


@pytest.mark.parametrize("detail", ["normal", "debug"])
def test_record_query_counts_and_times_in_either_detail(monkeypatch, detail):
    counter, duration = _Instrument(), _Instrument()
    monkeypatch.setattr(otel_compat, "_query_counter", None)
    monkeypatch.setattr(otel_compat, "_query_duration", None)
    register_query_instruments(counter, duration)
    token = set_trace_detail(detail)
    try:
        record_query(
            transport="pgwire", route="direct", engine="postgres", status_code=200, duration_ms=1.5
        )
    finally:
        reset_trace_detail(token)
    labels = {"transport": "pgwire", "route": "direct", "engine": "postgres", "status": 200}
    assert counter.calls == [(1, labels)]
    assert duration.calls == [(1.5, labels)]


# -- request facts: the audit seam, not spans ------------------------------------------------------


def _plan(**over):
    import time
    from types import SimpleNamespace

    from provisa.transpiler.router import Route

    base = dict(
        route=Route.DIRECT,
        dialect="postgres",
        role_id="analyst",
        sources=frozenset({"sales-pg"}),
        source_id="sales-pg",
        span_attrs={
            "provisa.table": "sales.orders",
            "provisa.domain": "sales",
            "provisa.role": "analyst",
            "provisa.query_text": "SELECT * FROM sales.orders",
        },
        audit=SimpleNamespace(surface="pgwire", started=time.monotonic()),
    )
    base.update(over)
    return SimpleNamespace(**base)


@pytest.fixture
def instruments(monkeypatch):
    counter, duration = _Instrument(), _Instrument()
    monkeypatch.setattr(otel_compat, "_query_counter", counter)
    monkeypatch.setattr(otel_compat, "_query_duration", duration)
    return counter, duration


def test_observe_plan_puts_the_statements_facts_on_the_request_span(traced, instruments):
    from provisa.observability.request_facts import observe_plan

    tracer, exporter = traced
    with request_span(tracer, "pgwire.query", transport="pgwire"):
        observe_plan(_plan(), 200)
    attrs = exporter.get_finished_spans()[0].attributes
    assert attrs["provisa.route"] == "direct"
    assert attrs["provisa.engine"] == "postgres"
    assert attrs["provisa.role"] == "analyst"
    assert attrs["provisa.sources"] == ("sales-pg",)
    assert attrs["provisa.status"] == 200
    # What the ops queries report reads, minus the statement text.
    assert attrs["provisa.table"] == "sales.orders"
    assert "provisa.query_text" not in attrs
    counter, duration = instruments
    labels = {"transport": "pgwire", "route": "direct", "engine": "postgres", "status": 200}
    assert counter.calls == [(1, labels)]
    assert duration.calls[0][1] == labels


def test_observe_plan_names_the_federation_engine_for_the_engine_route(traced, instruments):
    from provisa.observability.request_facts import observe_plan
    from provisa.transpiler.router import Route

    tracer, exporter = traced
    with request_span(tracer, "pgwire.query", transport="pgwire"):
        observe_plan(_plan(route=Route.ENGINE, dialect="duckdb", source_id=None), 500)
    attrs = exporter.get_finished_spans()[0].attributes
    assert attrs["provisa.route"] == "engine"
    assert attrs["provisa.engine"] == "duckdb"
    assert attrs["error"] is True
    assert instruments[0].calls[0][1]["status"] == 500


def test_observe_plan_in_debug_detail_also_records_the_statement_text(
    traced, instruments, debug_detail
):
    from provisa.observability.request_facts import observe_plan

    tracer, exporter = traced
    with request_span(tracer, "pgwire.query", transport="pgwire"):
        observe_plan(_plan(), 200)
    attrs = exporter.get_finished_spans()[0].attributes
    assert attrs["provisa.table"] == "sales.orders"
    assert attrs["provisa.query_text"] == "SELECT * FROM sales.orders"
    assert len(instruments[0].calls) == 1


def test_a_request_of_several_statements_is_one_record_listing_every_table(traced, instruments):
    from provisa.observability.request_facts import observe_plan

    tracer, exporter = traced
    second = {"provisa.table": "sales.customers", "provisa.domain": "sales", "provisa.role": "a"}
    with request_span(tracer, "POST /data/graphql", transport="graphql"):
        observe_plan(_plan(), 200)
        observe_plan(_plan(span_attrs=second), 200)
    attrs = exporter.get_finished_spans()[0].attributes
    assert attrs["provisa.table"] == "sales.orders"  # the first statement's
    assert attrs["provisa.tables"] == ("sales.orders", "sales.customers")
    assert attrs["provisa.statements"] == 2
    assert len(instruments[0].calls) == 2  # metrics count statements


def test_a_raw_sql_response_cache_hit_is_reported_as_the_cache_route(traced, instruments):
    from provisa.observability.request_facts import observe_plan

    tracer, exporter = traced
    with request_span(tracer, "pgwire.query", transport="pgwire"):
        observe_plan(_plan(), 200, cache_hit=True)
    attrs = exporter.get_finished_spans()[0].attributes
    assert attrs["provisa.route"] == "cache"
    assert attrs["cache.hit"] is True
    assert instruments[0].calls[0][1] == {
        "transport": "pgwire",
        "route": "cache",
        "engine": "none",
        "status": 200,
    }


def test_a_statement_outside_a_request_with_no_principal_is_not_counted(instruments):
    from provisa.observability.request_facts import observe_plan

    observe_plan(_plan(audit=None), 200)
    assert instruments[0].calls == []


def test_a_surface_without_a_request_span_is_counted_under_its_audit_surface(instruments):
    from provisa.observability.request_facts import observe_plan

    observe_plan(_plan(audit=_plan().audit.__class__(surface="bolt", started=0.0)), 200)
    assert instruments[0].calls[0][1]["transport"] == "bolt"


def test_a_graphql_cache_hit_is_reported_as_the_cache_route(traced, instruments):
    from provisa.api.otel_setup import _bind_http_request_span
    from provisa.observability.request_facts import TimedJSONResponse, observe_cache_hit

    tracer, exporter = traced
    with tracer.start_as_current_span("POST /data/graphql", kind=SpanKind.SERVER) as server:
        _bind_http_request_span(server, {"path": "/data/graphql"})
        observe_cache_hit(role_id="analyst", sources={"sales-pg"}, rows=4, started=0.0)
        TimedJSONResponse(content={"data": {"orders": []}})
    attrs = exporter.get_finished_spans()[0].attributes
    assert attrs["provisa.route"] == "cache"
    assert attrs["db.row_count"] == 4
    assert attrs["stage.encode.ms"] >= 0
    assert instruments[0].calls[0][1] == {
        "transport": "graphql",
        "route": "cache",
        "engine": "none",
        "status": 200,
    }


def test_stage_events_are_debug_detail_only_inside_a_request(traced):
    from provisa.observability.stage_trace import trace_stage

    tracer, exporter = traced
    with request_span(tracer, "pgwire.query", transport="pgwire"):
        trace_stage("govern", "SELECT 1")
        token = set_trace_detail("debug")
        try:
            trace_stage("transpile", "SELECT 1")
        finally:
            reset_trace_detail(token)
            otel_compat.clear_trace_detail()
    events = [e.name for e in exporter.get_finished_spans()[0].events]
    assert events == ["pipeline.stage:transpile"]
