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

# Custom query instruments — None until setup_otel() initialises metrics
query_counter: Any = None
query_duration: Any = None


def _is_http_endpoint(endpoint: str) -> bool:
    """Return True when endpoint uses an http:// or https:// scheme (OTLP/HTTP)."""
    return endpoint.startswith("http://") or endpoint.startswith("https://")


def _make_span_exporter(endpoint: str):
    if _is_http_endpoint(endpoint):
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter as HTTPSpanExporter
        return HTTPSpanExporter(endpoint=endpoint + "/v1/traces")
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
    return OTLPSpanExporter(endpoint=endpoint, insecure=True)


def _make_metric_exporter(endpoint: str):
    if _is_http_endpoint(endpoint):
        from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter as HTTPMetricExporter
        return HTTPMetricExporter(endpoint=endpoint + "/v1/metrics")
    from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
    return OTLPMetricExporter(endpoint=endpoint, insecure=True)


def _make_log_exporter(endpoint: str):
    if _is_http_endpoint(endpoint):
        from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter as HTTPLogExporter
        return HTTPLogExporter(endpoint=endpoint + "/v1/logs")
    from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter
    return OTLPLogExporter(endpoint=endpoint, insecure=True)


def attach_otlp_exporters(endpoint: str, service_name: str = "provisa") -> None:
    """Attach OTLP exporters to existing providers when endpoint is set at runtime."""
    import logging
    import provisa.api.otel_setup as _self
    _log = logging.getLogger(__name__)
    try:
        from opentelemetry import trace, metrics
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace.export import BatchSpanProcessor
        from opentelemetry.sdk.metrics import MeterProvider
        from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
        from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
        from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
        from opentelemetry._logs import set_logger_provider

        resource = Resource.create({"service.name": service_name})

        provider = trace.get_tracer_provider()
        if hasattr(provider, "add_span_processor"):
            _delay = int(os.environ.get("OTEL_SPAN_EXPORT_DELAY_MILLIS", 1000))
            provider.add_span_processor(
                BatchSpanProcessor(_make_span_exporter(endpoint), schedule_delay_millis=_delay)
            )

        metric_reader = PeriodicExportingMetricReader(
            _make_metric_exporter(endpoint),
            export_interval_millis=15000,
        )
        meter_provider = MeterProvider(resource=resource, metric_readers=[metric_reader])
        metrics.set_meter_provider(meter_provider)
        _meter = metrics.get_meter("provisa")
        _self.query_counter = _meter.create_counter("provisa.query.executed", description="Total queries executed")
        _self.query_duration = _meter.create_histogram("provisa.query.duration_ms", description="Query execution time in milliseconds", unit="ms")

        import logging as _logging
        log_provider = LoggerProvider(resource=resource)
        log_provider.add_log_record_processor(
            BatchLogRecordProcessor(_make_log_exporter(endpoint))
        )
        set_logger_provider(log_provider)
        handler = LoggingHandler(level=_logging.WARNING, logger_provider=log_provider)
        _logging.getLogger().addHandler(handler)

        _log.info("OTel exporters attached → %s (service=%s)", endpoint, service_name)
    except Exception as e:
        _log.warning("Failed to attach OTel exporters: %s", e)


def _write_otlp2parquet_toml(max_age_secs: int, config_path: str) -> None:
    """Regenerate observability/otlp2parquet.toml from provisa config values."""
    import logging
    _log = logging.getLogger(__name__)
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(config_path)))
    toml_path = os.path.join(project_root, "observability", "otlp2parquet.toml")
    content = (
        "[storage]\nbackend = \"s3\"\n\n"
        "[storage.s3]\nbucket = \"provisa-otel\"\n"
        "endpoint = \"http://minio:9000\"\nregion = \"us-east-1\"\n\n"
        f"[batch]\nmax_rows = 200000\nmax_bytes = 134217728\nmax_age_secs = {max_age_secs}\n"
    )
    try:
        with open(toml_path, "w") as _f:
            _f.write(content)
    except Exception as exc:
        _log.debug("Could not write otlp2parquet.toml: %s", exc)


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
    log_level_name = os.environ.get("OTEL_LOG_LEVEL") or _otel_cfg.get("log_level", "WARNING")
    compact_batch_size = int(os.environ.get("OTEL_COMPACT_BATCH_SIZE") or _otel_cfg.get("compact_batch_size", 10))
    span_export_delay_millis = int(os.environ.get("OTEL_SPAN_EXPORT_DELAY_MILLIS") or _otel_cfg.get("span_export_delay_millis", 1000))
    otlp2parquet_max_age_secs = int(os.environ.get("OTLP2PARQUET_MAX_AGE_SECS") or _otel_cfg.get("otlp2parquet_max_age_secs", 5))
    _write_otlp2parquet_toml(otlp2parquet_max_age_secs, config_path)
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
            provider.add_span_processor(
                BatchSpanProcessor(_make_span_exporter(endpoint), schedule_delay_millis=span_export_delay_millis)
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
            metric_reader = PeriodicExportingMetricReader(
                _make_metric_exporter(endpoint),
                export_interval_millis=15000,
            )
            meter_provider = MeterProvider(resource=resource, metric_readers=[metric_reader])
            metrics.set_meter_provider(meter_provider)
            _log.info("OTel metrics → %s (service=%s)", endpoint, service_name)

            import provisa.api.otel_setup as _self
            _meter = metrics.get_meter("provisa")
            _self.query_counter = _meter.create_counter(
                "provisa.query.executed",
                description="Total queries executed",
            )
            _self.query_duration = _meter.create_histogram(
                "provisa.query.duration_ms",
                description="Query execution time in milliseconds",
                unit="ms",
            )

        # ── Logs ─────────────────────────────────────────────────────────────
        if endpoint:
            import logging as _logging
            from opentelemetry.sdk._logs import LoggerProvider
            from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
            from opentelemetry._logs import set_logger_provider
            from opentelemetry.sdk._logs import LoggingHandler
            log_provider = LoggerProvider(resource=resource)
            log_provider.add_log_record_processor(
                BatchLogRecordProcessor(_make_log_exporter(endpoint))
            )
            set_logger_provider(log_provider)
            handler = LoggingHandler(level=getattr(_logging, log_level_name, _logging.WARNING), logger_provider=log_provider)
            _logging.getLogger().addHandler(handler)
            _log.info("OTel logs → %s (service=%s)", endpoint, service_name)

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
