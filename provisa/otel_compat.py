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

# Requirements: REQ-302, REQ-303, REQ-886

from __future__ import annotations

import time
import uuid
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Protocol, cast

if TYPE_CHECKING:
    from collections.abc import Iterator


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


def get_tracer(name: str) -> TracerProtocol:  # REQ-302, REQ-303
    """Return the OTel tracer for *name*, or a no-op tracer if OTel is absent."""
    try:
        from opentelemetry import trace as _trace

        return cast(TracerProtocol, _trace.get_tracer(name))
    except ImportError:
        return _NoopTracer()


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
