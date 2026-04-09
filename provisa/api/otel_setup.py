# Copyright (c) 2026 Kenneth Stott
# Canary: 3a7c1f9e-4d2b-4e8f-9c0a-1b5e6d2f7a8c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""OpenTelemetry tracing initialisation."""

from __future__ import annotations

import os

import yaml


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
