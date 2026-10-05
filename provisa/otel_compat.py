# Copyright (c) 2026 Kenneth Stott
# Canary: 25e523a2-8c94-41af-b521-1f8176333c1a
# Canary: PLACEHOLDER
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holders.

"""No-op OpenTelemetry shim.

Provides ``get_tracer(name)`` that returns the real OTel tracer when the
``opentelemetry`` package is installed, or a no-op tracer otherwise.
Unit tests run without opentelemetry installed; production uses the real SDK.
"""

# Requirements: REQ-302, REQ-303, REQ-886, REQ-1910

from __future__ import annotations

import functools
import inspect
import time
import uuid
import weakref
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal, Protocol, cast

if TYPE_CHECKING:
    from collections.abc import Iterator
    from contextvars import Token


class _NoopSpan:
    def __enter__(self):
        return self

    def __exit__(self, *_):
        pass

    def set_attribute(self, *_):
        pass

    def record_exception(self, *_):
        pass

    def set_status(self, *_):
        pass

    def end(self, *_):
        pass


class TracerProtocol(Protocol):  # REQ-545
    def start_as_current_span(self, name: str, **kwargs) -> _NoopSpan: ...
    def start_span(self, name: str, **kwargs) -> _NoopSpan: ...


class _NoopTracer:
    def start_as_current_span(self, name: str, **kwargs) -> _NoopSpan:  # noqa: ARG002
        return _NoopSpan()

    def start_span(self, name: str, **kwargs) -> _NoopSpan:  # noqa: ARG002
        return _NoopSpan()


@contextmanager
def detached_trace_context() -> Iterator[None]:
    """Run the body with NO active OpenTelemetry span, so work started inside it roots its own trace.

    ``asyncio.create_task`` and ``loop.call_later`` copy the caller's contextvars, the active span
    among them. A long-lived loop started from inside a request -- an engine prewarm, an APScheduler
    wakeup chain re-rooted by ``add_job`` -- therefore parented every span it ever emitted under
    that request: one ``POST /auth/redeem-invite`` carried 1338 spans over 47 minutes. Wrapping the
    spawn (or the scheduler wakeup) in this detaches the span for exactly that body, and the
    caller's own context is restored on exit. A no-op without the OTel SDK installed.
    """
    try:
        from opentelemetry import context as _context
    except ImportError:
        yield
        return
    token = _context.attach(_context.Context())
    try:
        yield
    finally:
        _context.detach(token)


def get_tracer(name: str) -> TracerProtocol:  # REQ-302, REQ-303
    """Return the OTel tracer for *name*, or a no-op tracer if OTel is absent."""
    try:
        from opentelemetry import trace as _trace

        return cast(TracerProtocol, _trace.get_tracer(name))
    except ImportError:
        return _NoopTracer()


# ---------------------------------------------------------------------------
# REQ-1910: trace detail. Every request is traced; ``normal`` detail gives it ONE span — the
# request span — carrying each pipeline stage's duration and facts as attributes, and ``debug``
# detail gives it today's waterfall: a span per stage and per Redis, source, engine and outbound
# call, with SQL text.
#
# The detail is resolved per request: ``set_trace_detail`` binds it at request entry, and a request
# that binds nothing gets the process default (``configure_trace_detail``, from
# ``observability.trace_detail`` / ``$PROVISA_TRACE_DETAIL``, default ``normal``).
# ---------------------------------------------------------------------------

TraceDetail = Literal["normal", "debug"]
TRACE_DETAILS: tuple[TraceDetail, ...] = ("normal", "debug")

_process_detail: TraceDetail = "normal"
_request_detail: ContextVar[TraceDetail | None] = ContextVar("provisa_trace_detail", default=None)
# The span of the request this context is serving: set by ``request_span`` (pgwire, Flight) and by
# the HTTP server-span hook (provisa.api.otel_setup). None outside a request — startup, scheduler
# jobs, discovery, MV refresh — where spans are created exactly as before in either detail.
_request_span: ContextVar[Any] = ContextVar("provisa_request_span", default=None)
_request_transport: ContextVar[str | None] = ContextVar("provisa_request_transport", default=None)

# Attributes normal detail does not record on the request span: statement text or a plan, and the
# statement's identity (table, domain, role) — whose one home is the statement's audit row, where
# the ops `queries` report reads them. Debug detail records them on the statement's own span.
DEBUG_ONLY_ATTRIBUTES = frozenset(
    {
        "provisa.table",
        "provisa.domain",
        "provisa.role",
        "db.statement",
        "db.query.text",
        "flight.sql",
        "flight.gql_query",
        "provisa.query_text",
        "provisa.plan",
    }
)


def _checked_detail(detail: str) -> TraceDetail:
    if detail not in TRACE_DETAILS:
        raise ValueError(f"trace detail {detail!r} is not one of {', '.join(TRACE_DETAILS)}")
    return cast(TraceDetail, detail)


def configure_trace_detail(detail: str) -> None:
    """Set the process default detail — what a request gets when nothing binds one for it."""
    global _process_detail
    _process_detail = _checked_detail(detail)


# The detail bound for a request, keyed by its request span. A transport serves one request as
# several tasks — pgwire runs govern, execute and audit each through ``run_until_complete`` — and
# every task gets a COPY of the connection's context, so a ContextVar bound inside one task is
# seen by neither the next task nor the transport's own stage reporting. The request span object
# is what they all share, so the binding hangs off it and ends with it (weakly held: no cleanup).
# Only a RECORDING span carries one: with no SDK every request gets the same non-recording span
# object, which is not a request's own, and nothing is exported for the detail to shape.
_span_detail: "weakref.WeakKeyDictionary[Any, TraceDetail]" = weakref.WeakKeyDictionary()


def trace_detail() -> TraceDetail:
    """The trace detail of the request this context is serving."""
    span = _live_request_span()
    if span is not None:
        bound = _span_detail.get(span)
        if bound is not None:
            return bound
    return _request_detail.get() or _process_detail


def set_trace_detail(detail: str) -> "Token[TraceDetail | None]":
    """Bind the detail for the current request — on its request span when it has one, so every
    task of the request sees it, and in this context. Call at request entry; pass the token to
    :func:`reset_trace_detail` when the request ends."""
    checked = _checked_detail(detail)
    span = _live_request_span()
    if span is not None:
        _span_detail[span] = checked
    return _request_detail.set(checked)


def reset_trace_detail(token: "Token[TraceDetail | None]") -> None:
    _request_detail.reset(token)


def clear_trace_detail() -> None:
    """Unbind the request detail, so this context is traced at the process default again. For a
    caller that resolves the detail on every request and holds no token from the previous bind
    (the debug-trace scope, ``provisa.core.trace_scope``): a request nothing covers must not
    inherit the detail an earlier request on the same context was given."""
    span = _live_request_span()
    if span is not None:
        _span_detail.pop(span, None)
    _request_detail.set(None)


def in_request_scope() -> bool:
    """True while this context is serving a request that has a request span."""
    return _request_span.get() is not None


def bind_request_span(span: Any, transport: str) -> None:
    """Mark ``span`` as this context's request span. For a transport whose server span is opened
    by an instrumentor (HTTP): the binding lives as long as the request's task, so it is not reset."""
    _request_span.set(span)
    _request_transport.set(transport)
    span.set_attribute("provisa.transport", transport)


def request_transport() -> str | None:
    """The transport of the request this context is serving, while its request span is open. None
    outside a request, and for work that outlives the request it was started from."""
    return _request_transport.get() if _live_request_span() is not None else None


def _live_request_span() -> Any:
    """The request span while it is still recording, else None."""
    span = _request_span.get()
    if span is not None and span.is_recording():
        return span
    return None


def _server_kind_kwargs() -> dict[str, Any]:
    try:
        from opentelemetry.trace import SpanKind
    except ImportError:
        return {}
    return {"kind": SpanKind.SERVER}


def _current_server_span() -> Any:
    """The recording SERVER-kind span current in this context (an instrumentor's), else None."""
    try:
        from opentelemetry import trace as _trace
        from opentelemetry.trace import SpanKind
    except ImportError:
        return None
    span = _trace.get_current_span()
    if span.is_recording() and getattr(span, "kind", None) is SpanKind.SERVER:
        return span
    return None


# ---------------------------------------------------------------------------
# REQ-1910 (amended 2026-10-05): an HTTP request's receive span. The ASGI instrumentation's own
# receive span is turned off (otel_setup), because the accepting loop reads a small body before
# the request reaches the middleware that resolves its debug-trace window, and a span opened then
# could not follow that window. The read's timing is recorded on the hand-off instead and the span
# is emitted once the window is known, with the measured times; a body read as it arrives (large or
# chunked, and every later receive) keeps a live span.
RECEIVE_RECORD = "provisa_receive"


@dataclass(frozen=True)
class ReceiveRecord:
    """A pre-read request body: the server span it belongs to, when the read began and ended
    (epoch ns), and how many bytes it carried."""

    server_span: Any
    start_ns: int
    end_ns: int
    body_bytes: int


def http_server_span() -> Any:
    """The HTTP server span current at request entry (the ASGI instrumentation's), or None."""
    return _current_server_span()


def _http_tracer() -> Any:
    """The tracer of the process's provider -- the one the ASGI instrumentation's server span
    comes from."""
    from opentelemetry import trace as _trace

    return _trace.get_tracer("provisa.http")


def _receive_span_name(server_span: Any, scope_type: str) -> str:
    return f"{server_span.name} {scope_type} receive"


@contextmanager
def live_receive_span(server_span: Any, scope_type: str) -> Iterator[Any]:
    """A receive span around a body read as it arrives, a child of the request's server span;
    None (no span) when the request has no recording server span."""
    if server_span is None:
        yield None
        return
    from opentelemetry import trace as _trace

    with _http_tracer().start_as_current_span(
        _receive_span_name(server_span, scope_type),
        context=_trace.set_span_in_context(server_span),
    ) as span:
        yield span


def emit_recorded_receive(record: ReceiveRecord, scope_type: str) -> None:
    """Emit a pre-read body's receive span with its measured times, as a child of the request's
    server span -- in debug detail only (normal detail gives a request one span)."""
    if record.server_span is None or trace_detail() != "debug":
        return
    from opentelemetry import trace as _trace

    span = _http_tracer().start_span(
        _receive_span_name(record.server_span, scope_type),
        context=_trace.set_span_in_context(record.server_span),
        start_time=record.start_ns,
    )
    span.set_attribute("asgi.event.type", "http.request")
    span.set_attribute("http.request.body.size", record.body_bytes)
    span.end(end_time=record.end_ns)


class _RequestFacts:
    """What a pipeline stage sees in normal detail: its attributes go onto the request span, and
    statement text is not recorded."""

    __slots__ = ("_span", "_stage")

    def __init__(self, span: Any, stage_key: str | None = None) -> None:
        self._span = span
        self._stage = stage_key

    def set_attribute(self, key: str, value: Any) -> None:
        if key in DEBUG_ONLY_ATTRIBUTES and trace_detail() == "normal":
            return
        self._span.set_attribute(key, value)

    def record_exception(self, exception: BaseException, *_: Any, **__: Any) -> None:
        self._span.set_attribute(_stage_attr(self._stage, "error"), type(exception).__name__)

    def set_status(self, *_: Any, **__: Any) -> None:
        # The request span's status is the transport's to set: a stage that failed and was
        # retried or recovered must not mark the request failed.
        pass

    def add_event(self, name: str, attributes: Any = None, *_: Any, **__: Any) -> None:
        self._span.add_event(name, attributes=attributes)

    def get_span_context(self) -> Any:
        return self._span.get_span_context()

    def is_recording(self) -> bool:
        return self._span.is_recording()


def _stage_attr(stage_key: str | None, what: str) -> str:
    return f"stage.{stage_key}.{what}" if stage_key else f"request.{what}"


def current_trace_context() -> Any:
    """The OpenTelemetry context current on this thread (None without the SDK) — what
    :func:`request_span` is given as ``parent`` when the request runs in a context of its own."""
    try:
        from opentelemetry import context as _context
    except ImportError:
        return None
    return _context.get_current()


@contextmanager
def request_span(
    tracer: TracerProtocol, name: str, *, transport: str, parent: Any = None
) -> "Iterator[Any]":
    """Open the request span of a transport that has no instrumentor to open one (pgwire, Flight).

    Every stage of the request reports into this span in normal detail and hangs a child span off
    it in debug detail. Yields the span's facts handle: ``set_attribute`` as on a span, with
    statement text recorded only in debug detail.

    A request has ONE request span. Inside a request that already has one this opens nothing and
    yields that span's handle; and when an instrumentor has already opened the transport's server
    span (gRPC), that span is the request span and is bound rather than wrapped in a second.

    ``parent`` is the OpenTelemetry context the transport received the call in, for a transport
    that runs the request in a fresh context (gRPC's per-RPC context): it is attached for the
    request, so an instrumentor's server span opened out there is found and bound here."""
    if parent is not None:
        from opentelemetry import context as _context

        token = _context.attach(parent)
        try:
            with request_span(tracer, name, transport=transport) as facts:
                yield facts
        finally:
            _context.detach(token)
        return
    existing = _live_request_span()
    if existing is not None:
        yield _RequestFacts(existing)
        return
    server = _current_server_span()
    if server is not None:
        server.set_attribute("provisa.transport", transport)
        span_token = _request_span.set(server)
        transport_token = _request_transport.set(transport)
        try:
            yield _RequestFacts(server)
        finally:
            _request_transport.reset(transport_token)
            _request_span.reset(span_token)
        return
    with tracer.start_as_current_span(name, **_server_kind_kwargs()) as span:
        span.set_attribute("provisa.transport", transport)
        span_token = _request_span.set(span)
        transport_token = _request_transport.set(transport)
        try:
            yield _RequestFacts(span)
        finally:
            _request_transport.reset(transport_token)
            _request_span.reset(span_token)


def in_request_span(tracer: TracerProtocol, name: str, *, transport: str) -> Any:
    """Decorate a transport's request handler (a function or a coroutine function) so each call
    runs inside its request span (see :func:`request_span`)."""

    def _decorate(fn: Any) -> Any:
        if inspect.iscoroutinefunction(fn):

            @functools.wraps(fn)
            async def _async_in_span(*args: Any, **kwargs: Any) -> Any:
                with request_span(tracer, name, transport=transport):
                    return await fn(*args, **kwargs)

            return _async_in_span

        @functools.wraps(fn)
        def _in_span(*args: Any, **kwargs: Any) -> Any:
            with request_span(tracer, name, transport=transport):
                return fn(*args, **kwargs)

        return _in_span

    return _decorate


@contextmanager
def stage(
    tracer: TracerProtocol,
    span_name: str,
    *,
    name: str | None = None,
    request_only: bool = False,
) -> "Iterator[Any]":
    """One pipeline stage of a request.

    Debug detail: a child span named ``span_name``, exactly as before, and the stage's duration
    on the request span as in normal detail.
    Normal detail: NO span. The stage's duration is added to ``stage.<name>.ms`` on the request
    span (a stage that runs more than once in a request accumulates, with ``stage.<name>.count``),
    and the attributes the stage sets land on the request span — statement text excepted.

    Outside a request (startup, scheduler, discovery, MV refresh) there is no request span to
    report into, so the stage is a span in either detail.
    ``request_only`` is for a stage that exists only to time part of a request (routing, response
    encoding) and was never a span of its own: outside a request it is no span at all.

    ``name`` is the stage the duration is reported under (``govern``, ``compile``, ``route``,
    ``execute``, ``encode``, ``cache``); it defaults to ``span_name``.
    """
    request = _live_request_span()
    if request is None:
        if request_only:
            yield _NoopSpan()
            return
        with tracer.start_as_current_span(span_name) as span:
            yield span
        return
    key = name or span_name
    started = time.perf_counter()
    if trace_detail() == "debug":
        # The child span is the detail; the request span still reports the stage's duration, so
        # the request record stands on its own in either detail.
        try:
            with tracer.start_as_current_span(span_name) as span:
                yield span
        finally:
            _add_stage_time(request, key, (time.perf_counter() - started) * 1000)
        return
    try:
        yield _RequestFacts(request, key)
    except BaseException as exc:
        request.set_attribute(_stage_attr(key, "error"), type(exc).__name__)
        raise
    finally:
        _add_stage_time(request, key, (time.perf_counter() - started) * 1000)


def _add_stage_time(request: Any, key: str, elapsed_ms: float) -> None:
    ms_attr, count_attr = _stage_attr(key, "ms"), _stage_attr(key, "count")
    recorded = request.attributes
    previous = recorded.get(ms_attr)
    if previous is None:
        request.set_attribute(ms_attr, round(elapsed_ms, 3))
        return
    request.set_attribute(ms_attr, round(previous + elapsed_ms, 3))
    request.set_attribute(count_attr, recorded.get(count_attr, 1) + 1)


def timed_stage(tracer: TracerProtocol, span_name: str, *, name: str | None = None) -> Any:
    """Decorate a synchronous function so each call inside a request is a :func:`stage` of it.
    Outside a request the function runs with no span."""

    def _decorate(fn: Any) -> Any:
        @functools.wraps(fn)
        def _timed(*args: Any, **kwargs: Any) -> Any:
            with stage(tracer, span_name, name=name, request_only=True):
                return fn(*args, **kwargs)

        return _timed

    return _decorate


class HeldRequestSpan:
    """The request span of a request that is several protocol messages long, held by the
    connection handler that serves it.

    :func:`request_span` is a block, and a pgwire extended-protocol request has no block to put it
    around: Parse, Bind, Describe, Execute and Sync arrive as separate messages, each dispatched
    on its own. The handler holds one of these instead — ``open()`` at every message of a request
    (the first one opens the span, the rest find it open), ``close()`` when the request ends.
    """

    __slots__ = ("_name", "_scope", "_tracer", "_transport")

    def __init__(self, tracer: TracerProtocol, name: str, *, transport: str) -> None:
        self._tracer = tracer
        self._name = name
        self._transport = transport
        self._scope: Any = None

    def open(self) -> None:
        if self._scope is None:
            scope = request_span(self._tracer, self._name, transport=self._transport)
            scope.__enter__()
            self._scope = scope

    def close(self) -> None:
        scope, self._scope = self._scope, None
        if scope is not None:
            scope.__exit__(None, None, None)


def request_fact(attr: str) -> Any:
    """The value a request fact currently has on the request span; None when it has not been set
    or there is no request."""
    request = _live_request_span()
    return None if request is None else request.attributes.get(attr)


def annotate_request(**facts: Any) -> None:
    """Set facts about the request (route, engine, source ids, cache result, row count, status) on
    its request span. Keys are attribute names with ``.`` written as ``__``. A no-op outside a
    request."""
    request = _live_request_span()
    if request is None:
        return
    normal = trace_detail() == "normal"
    for key, value in facts.items():
        attr = key.replace("__", ".")
        if value is None or (normal and attr in DEBUG_ONLY_ATTRIBUTES):
            continue
        request.set_attribute(attr, value)


# Request metrics (REQ-1910): recorded for every statement in either detail, from the audit seam
# every transport reaches — never derived from spans, so dashboards and alerts do not depend on the
# trace detail. The instruments exist once setup_otel has a collector to export to.
_query_counter: Any = None
_query_duration: Any = None


def register_query_instruments(counter: Any, duration: Any) -> None:
    global _query_counter, _query_duration
    _query_counter, _query_duration = counter, duration


def record_query(
    *, transport: str, route: str, engine: str, status_code: int, duration_ms: float
) -> None:
    """Count one executed statement and record its latency, per transport, route and engine."""
    if _query_counter is None:
        return  # no collector configured: metrics are not exported at all (setup_otel)
    attrs = {"transport": transport, "route": route, "engine": engine, "status": status_code}
    _query_counter.add(1, attrs)
    _query_duration.record(duration_ms, attrs)


# ---------------------------------------------------------------------------
# REQ-886: non-bypassable UDF/transformer I/O-boundary invocation tracing.
#
# Every function dispatch (all REQ-885 implementation kinds) is wrapped by
# ``udf_invocation_trace``; the dispatcher — not the UDF — emits the trace, so no
# kind can bypass it. The correlation id is stamped into any pgwire session the UDF
# mints (``mint_udf_session``) so data-access audit rows join back to the invocation.
# ---------------------------------------------------------------------------

# transport type recorded per implementation kind (REQ-886)
TRANSPORT_BY_KIND: dict[str, str] = {
    "source_procedure": "sql",
    "source_operation": "source",  # REQ-1924: a source's write operation, over its own protocol
    "script": "script",
    "http": "http",
    "grpc": "grpc",
    "python": "python",
}


@dataclass
class UdfTrace:
    """The engine-emitted invocation trace — the mandatory observability floor (REQ-886)."""

    udf_name: str
    transport: str  # sql | script | http | grpc | python
    identity: str  # "definer" (admin) | "invoker" (user)
    input_refs: list[str]  # declared relation/input refs
    correlation_id: str  # stamped into the UDF's minted pgwire session
    role_id: str | None = None
    output_cardinality: int = 0  # rows returned
    output_bytes: int = 0  # serialized size
    duration_ms: int = 0
    status: str = "ok"  # "ok" | "error"


class UdfTraceSink(Protocol):  # REQ-886
    def record(self, trace: UdfTrace) -> None: ...


@dataclass
class MemoryUdfTraceSink:
    """In-process sink retaining emitted traces (default sink + test observation)."""

    records: list[UdfTrace] = field(default_factory=list)

    def record(self, trace: UdfTrace) -> None:
        self.records.append(trace)


# Default sink used when the caller supplies none — tracing is never optional (REQ-886).
_DEFAULT_UDF_TRACE_SINK = MemoryUdfTraceSink()


def default_udf_trace_sink() -> MemoryUdfTraceSink:
    return _DEFAULT_UDF_TRACE_SINK


def new_correlation_id() -> str:
    return uuid.uuid4().hex


# REQ-886: the ambient UDF invocation correlation id. Set for the duration of a UDF dispatch so
# any audit row written under the UDF's minted session (log_query) adopts it into trace_id — the
# join key from an audit row back to the engine-side invocation trace, with no call-site threading.
_current_udf_correlation: ContextVar[str | None] = ContextVar("udf_correlation", default=None)


def current_udf_correlation_id() -> str | None:
    """The correlation id of the UDF invocation currently in scope, or None outside one (REQ-886)."""
    return _current_udf_correlation.get()


@dataclass
class MintedSession:
    """A scoped, short-TTL pgwire session minted for a UDF invocation (REQ-885/886).

    The ``correlation_id`` equals the invocation trace's id: any audit row written under
    this session joins back to the engine-side invocation trace (REQ-886)."""

    correlation_id: str
    identity: str  # "definer" | "invoker"
    role_id: str | None
    token: str


def mint_udf_session(correlation_id: str, identity: str, role_id: str | None) -> MintedSession:
    """Mint a session carrying the invocation ``correlation_id`` (REQ-886)."""
    return MintedSession(
        correlation_id=correlation_id,
        identity=identity,
        role_id=role_id,
        token=uuid.uuid4().hex,
    )


@contextmanager
def udf_invocation_trace(  # REQ-886
    *,
    udf_name: str,
    transport: str,
    identity: str,
    input_refs: list[str],
    role_id: str | None = None,
    correlation_id: str | None = None,
    sink: UdfTraceSink | None = None,
) -> "Iterator[UdfTrace]":
    """Wrap one UDF dispatch in a non-bypassable trace emission.

    Yields a mutable :class:`UdfTrace`; the dispatcher fills ``output_cardinality`` /
    ``output_bytes`` after execution. On exit the trace's ``duration_ms`` is stamped, its
    ``status`` reflects success/exception, it is mirrored onto an OTel span, and it is
    recorded to ``sink`` (the default in-process sink when none is supplied). Any exception
    is re-raised after the trace is recorded — a failed invocation is still traced."""
    trace = UdfTrace(
        udf_name=udf_name,
        transport=transport,
        identity=identity,
        input_refs=list(input_refs),
        correlation_id=correlation_id or new_correlation_id(),
        role_id=role_id,
    )
    tracer = get_tracer("provisa.udf")
    start = time.monotonic()
    span = tracer.start_span("udf.invoke")
    # Publish the correlation id as ambient context so audit rows written during the invocation
    # adopt it into trace_id (REQ-886) — reset on exit to not leak into the caller's context.
    _corr_token = _current_udf_correlation.set(trace.correlation_id)
    try:
        yield trace
    except BaseException:
        trace.status = "error"
        raise
    finally:
        _current_udf_correlation.reset(_corr_token)
        trace.duration_ms = int((time.monotonic() - start) * 1000)
        span.set_attribute("udf.name", trace.udf_name)
        span.set_attribute("udf.transport", trace.transport)
        span.set_attribute("udf.identity", trace.identity)
        span.set_attribute("udf.correlation_id", trace.correlation_id)
        span.set_attribute("udf.output_cardinality", trace.output_cardinality)
        span.set_attribute("udf.output_bytes", trace.output_bytes)
        span.set_attribute("udf.duration_ms", trace.duration_ms)
        span.set_attribute("udf.status", trace.status)
        span.end()
        (sink or _DEFAULT_UDF_TRACE_SINK).record(trace)
