# Copyright (c) 2026 Kenneth Stott
# Canary: 3a7c1f9e-4d2b-4e8f-9c0a-1b5e6d2f7a8c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""OpenTelemetry tracing and metrics initialisation."""

from __future__ import annotations

import os
import time
from collections import deque
from threading import Lock
from typing import Any

import yaml


class SpanBuffer:
    """Thread-safe circular buffer of the last N completed spans."""

    def __init__(self, maxlen: int = 100) -> None:
        self._buf: deque[dict[str, Any]] = deque(maxlen=maxlen)
        self._lock = Lock()

    def push(self, span: Any) -> None:
        ctx = span.get_span_context()
        entry = {
            "ts": time.time(),
            "trace_id": format(ctx.trace_id, "032x") if ctx else "",
            "span_id": format(ctx.span_id, "016x") if ctx else "",
            "name": span.name,
            "status": span.status.status_code.name if span.status else "UNSET",
            "duration_ms": round((span.end_time - span.start_time) / 1e6, 2)
            if span.end_time and span.start_time
            else None,
            "attrs": dict(span.attributes or {}),
        }
        with self._lock:
            self._buf.appendleft(entry)

    def recent(self, limit: int = 50) -> list[dict[str, Any]]:
        with self._lock:
            return list(self._buf)[:limit]


# Module-level singleton — imported by settings_router
span_buffer = SpanBuffer()


def setup_otel(app: "object") -> None:
    """Initialize OpenTelemetry tracing unconditionally.

    Always creates a TracerProvider so module-level tracers work everywhere.
    Only attaches the OTLP exporter when OTEL_EXPORTER_OTLP_ENDPOINT is set —
    without it, spans are created but silently dropped (NoOpSpanExporter).
    This lets the airgapped release emit traces by default; users opt-in to
    collection by pointing OTEL_EXPORTER_OTLP_ENDPOINT at a collector.
    """
    import logging
    _log = logging.getLogger(__name__)
    config_path = os.environ.get("PROVISA_CONFIG", "config/provisa.yaml")
    _otel_cfg: dict = {}
    try:
        with open(config_path) as _f:
            _otel_cfg = (yaml.safe_load(_f) or {}).get("observability", {})
    except Exception:
        pass
    endpoint = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT") or _otel_cfg.get("endpoint", "")
    service_name = os.environ.get("OTEL_SERVICE_NAME") or _otel_cfg.get("service_name", "provisa")
    sample_rate = float(_otel_cfg.get("sample_rate", 1.0))
    try:
        from opentelemetry import trace
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.sampling import TraceIdRatioBased, ParentBased
        from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
        resource = Resource.create({"service.name": service_name})
        sampler = ParentBased(TraceIdRatioBased(sample_rate)) if sample_rate < 1.0 else None
        provider = TracerProvider(resource=resource, **({} if sampler is None else {"sampler": sampler}))
        # Always buffer spans in-memory for the live trace panel
        from opentelemetry.sdk.trace import SpanProcessor
        _buf = span_buffer

        class _BufferProcessor(SpanProcessor):
            def on_start(self, span: Any, parent_context: Any = None) -> None:  # noqa: ARG002
                pass
            def on_end(self, span: Any) -> None:
                _buf.push(span)
            def shutdown(self) -> None:
                pass
            def force_flush(self, timeout_millis: int = 30000) -> bool:
                return True

        provider.add_span_processor(_BufferProcessor())
        if endpoint:
            from opentelemetry.sdk.trace.export import BatchSpanProcessor
            from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
            provider.add_span_processor(
                BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint, insecure=True))
            )
            _log.info("OTel tracing → %s (service=%s)", endpoint, service_name)
        else:
            _log.debug(
                "OTel tracing active (no collector; spans dropped). "
                "Set OTEL_EXPORTER_OTLP_ENDPOINT to export."
            )
        trace.set_tracer_provider(provider)

        # ── Metrics ──────────────────────────────────────────────────────────
        if endpoint:
            from opentelemetry import metrics
            from opentelemetry.sdk.metrics import MeterProvider
            from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
            from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
            metric_reader = PeriodicExportingMetricReader(
                OTLPMetricExporter(endpoint=endpoint, insecure=True),
                export_interval_millis=15000,
            )
            meter_provider = MeterProvider(resource=resource, metric_readers=[metric_reader])
            metrics.set_meter_provider(meter_provider)
            _log.info("OTel metrics → %s (service=%s)", endpoint, service_name)

        FastAPIInstrumentor.instrument_app(app)
        try:
            from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
            HTTPXClientInstrumentor().instrument()
        except ImportError:
            pass
        try:
            from opentelemetry.instrumentation.asyncpg import AsyncPGInstrumentor
            AsyncPGInstrumentor().instrument()
        except ImportError:
            pass
        try:
            from opentelemetry.instrumentation.redis import RedisInstrumentor
            RedisInstrumentor().instrument()
        except ImportError:
            pass
        try:
            from opentelemetry.instrumentation.pymongo import PymongoInstrumentor
            PymongoInstrumentor().instrument()
        except ImportError:
            pass
        try:
            from opentelemetry.instrumentation.elasticsearch import ElasticsearchInstrumentor
            ElasticsearchInstrumentor().instrument()
        except ImportError:
            pass
        try:
            from opentelemetry.instrumentation.grpc import GrpcInstrumentorClient, GrpcInstrumentorServer
            GrpcInstrumentorClient().instrument()
            GrpcInstrumentorServer().instrument()
        except ImportError:
            pass
    except ImportError:
        _log.warning("OTel packages missing; skipping instrumentation")
