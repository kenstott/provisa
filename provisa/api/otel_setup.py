# Copyright (c) 2026 Kenneth Stott
# Canary: 3a7c1f9e-4d2b-4e8f-9c0a-1b5e6d2f7a8c
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""OpenTelemetry tracing and metrics initialisation."""

# Requirements: REQ-302, REQ-303, REQ-330, REQ-545, REQ-546, REQ-547, REQ-548, REQ-549, REQ-1910

from __future__ import annotations

import os
import re
import time
from collections import deque
from threading import Lock
from typing import Any


from provisa.core.config_location import config_path_str
from provisa.otel_compat import (
    bind_request_span,
    configure_trace_detail,
    in_request_scope,
    register_query_instruments,
    trace_detail,
)

# Matches SQL string literals ('...') and bare numeric literals outside identifiers.
_SQL_LITERAL_RE = re.compile(r"'[^']*'|\"[^\"]*\"|\b\d+(\.\d+)?\b")

_log_provider: "object | None" = None


def shutdown_otel() -> None:  # REQ-545
    """Stop every OTel export -- logs, spans, metrics -- before interpreter teardown."""
    _shutdown_log_pipeline()
    # The trace and meter providers stay installed (OTel accepts one per process); what stops is
    # their exporting -- each export thread and its HTTP client.
    if _span_exports is not None:
        _span_exports.clear()
    if _metric_exports is not None:
        _metric_exports.stop()


def _shutdown_log_pipeline() -> None:
    """Remove the OTLP log handler and shut down the log provider (its thread and exporter)."""
    global _log_provider
    # Detach the OTLP LoggingHandler from the root logger first. Otherwise Python's
    # atexit logging.shutdown() flushes it during interpreter teardown, and the
    # BatchLogRecordProcessor's flush() tries to start a thread — which raises
    # "can't create new thread at interpreter shutdown".
    try:
        import logging as _logging

        from opentelemetry.sdk._logs import LoggingHandler

        _root = _logging.getLogger()
        for _h in list(_root.handlers):
            if isinstance(_h, LoggingHandler):
                _root.removeHandler(_h)
    except Exception:
        pass
    if _log_provider is not None:
        try:
            _log_provider.shutdown()  # type: ignore[union-attr]
        except Exception:
            pass
        _log_provider = None


# ── The process's one trace and meter pipeline (REQ-302, REQ-303, REQ-549) ──────────────────────
# OpenTelemetry installs ONE trace and ONE meter provider per process; a provider built after the
# first is never installed, but its export thread runs all the same. So the providers are built
# once, by the first setup, and every later setup or endpoint change replaces only what exports:
# the span processor of a channel, the metric exporter. The one replaced is shut down -- its thread
# and its HTTP client with it.

_tracer_provider: "object | None" = None
_span_exports: "_SpanExports | None" = None
_metric_exports: "_MetricExports | None" = None


class _SpanExports:
    """The process provider's export processors, one per channel ("internal", "support")."""

    def __init__(self) -> None:
        self._by_channel: dict[str, Any] = {}
        self._lock = Lock()

    def set(self, channel: str, processor: Any) -> None:
        with self._lock:
            old = self._by_channel.pop(channel, None)
            if processor is not None:
                self._by_channel[channel] = processor
        if old is not None:
            old.shutdown()

    def clear(self) -> None:
        with self._lock:
            old, self._by_channel = list(self._by_channel.values()), {}
        for processor in old:
            processor.shutdown()

    def current(self) -> list[Any]:
        with self._lock:
            return list(self._by_channel.values())


def _span_switch(exports: "_SpanExports") -> Any:
    """The one span processor the process provider holds: it hands each span to the channels'
    current export processors."""
    from opentelemetry.sdk.trace import SpanProcessor

    class _Switch(SpanProcessor):
        def on_start(self, span: Any, parent_context: Any = None) -> None:
            for processor in exports.current():
                processor.on_start(span, parent_context)

        def on_end(self, span: Any) -> None:
            for processor in exports.current():
                processor.on_end(span)

        def shutdown(self) -> None:
            exports.clear()

        def force_flush(self, timeout_millis: int = 30000) -> bool:
            return all(p.force_flush(timeout_millis) for p in exports.current())

    return _Switch()


class _MetricExports:
    """The process meter provider's one reader, collected by an export thread whose exporter is
    replaced on an endpoint change. A meter provider's readers are fixed when it is built, so the
    reader stays and what it exports through is what changes."""

    def __init__(self, exporter: Any, interval_millis: int) -> None:
        from opentelemetry.sdk.metrics.export import MetricReader

        exports = self

        class _Reader(MetricReader):
            def _receive_metrics(
                self, metrics_data: Any, timeout_millis: float = 10_000, **_kw: Any
            ) -> None:
                current = exports._exporter
                if current is not None and metrics_data is not None:
                    current.export(metrics_data, timeout_millis=timeout_millis)

            def shutdown(self, timeout_millis: float = 30_000, **_kw: Any) -> None:
                exports.stop()

        self.reader = _Reader(
            preferred_temporality=exporter._preferred_temporality,
            preferred_aggregation=exporter._preferred_aggregation,
        )
        self._exporter: Any = None
        self._interval = interval_millis / 1000
        self._thread: Any = None
        self._stop: Any = None
        self._lock = Lock()

    def start(self, exporter: Any) -> None:
        """Export through ``exporter`` from now on; the exporter it replaces is shut down."""
        import threading

        self.stop()
        with self._lock:
            self._exporter = exporter
            stop = threading.Event()
            reader, interval = self.reader, self._interval

            def _tick() -> None:
                while not stop.wait(interval):
                    try:
                        reader.collect()
                    except Exception:  # allow-ble: an export thread's boundary -- next tick retries
                        import logging

                        logging.getLogger(__name__).exception("OTel metric export failed")

            self._stop = stop
            self._thread = threading.Thread(target=_tick, name="provisa-otel-metrics", daemon=True)
            self._thread.start()

    def stop(self) -> None:
        with self._lock:
            stop, thread, exporter = self._stop, self._thread, self._exporter
            self._stop = self._thread = self._exporter = None
        if stop is not None:
            stop.set()
        if thread is not None:
            thread.join(timeout=5)
        if exporter is not None:
            exporter.shutdown()


def _process_tracer_provider(sampler: Any, resource: Any) -> Any:
    """The process's trace provider, built and installed by the first call. It always buffers
    spans in memory for the live trace panel, and exports through ``_span_exports``."""
    global _tracer_provider, _span_exports
    if _tracer_provider is not None:
        return _tracer_provider
    from opentelemetry import trace
    from opentelemetry.sdk.trace import SpanProcessor, TracerProvider

    provider = TracerProvider(sampler=sampler, resource=resource)
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
    _span_exports = _SpanExports()
    provider.add_span_processor(_span_switch(_span_exports))
    trace.set_tracer_provider(provider)
    _tracer_provider = provider
    return provider


def _export_metrics(exporter: Any, resource: Any) -> None:
    """Export the process's metrics through ``exporter``: the meter provider is built and
    installed on the first call, and every call replaces the exporter (the old one shut down)."""
    global _metric_exports
    if _metric_exports is None:
        from opentelemetry import metrics
        from opentelemetry.sdk.metrics import MeterProvider

        _metric_exports = _MetricExports(exporter, _metric_export_interval_millis())
        metrics.set_meter_provider(
            MeterProvider(resource=resource, metric_readers=[_metric_exports.reader])
        )
    _metric_exports.start(exporter)


class SpanBuffer:  # REQ-302, REQ-303
    """Thread-safe circular buffer of the last N completed spans."""

    def __init__(self, maxlen: int = 100) -> None:
        self._buf: deque[dict[str, Any]] = deque(maxlen=maxlen)
        self._lock = Lock()

    def push(self, span: Any) -> None:
        from provisa.core.request_context import current_org

        ctx = span.get_span_context()
        entry = {
            "ts": time.time(),
            # REQ-1349: the org whose request produced this span, stamped so the trace view can be
            # scoped. A span ended outside a request (startup, background refresh) belongs to no
            # org and records None — it is therefore invisible to an org-scoped reader, which is
            # the intended reading: it is not that org's activity.
            "org": current_org.get(),
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

    def recent(self, limit: int = 50, *, org_id: str | None = None) -> list[dict[str, Any]]:
        """The most recent spans; ``org_id`` restricts them to one org's requests (REQ-1349)."""
        with self._lock:
            entries = list(self._buf)
        if org_id is not None:
            entries = [e for e in entries if e.get("org") == org_id]
        return entries[:limit]


# Module-level singleton — imported by settings_router
span_buffer = SpanBuffer()

# Custom query instruments — None until setup_otel() initialises metrics
query_counter: Any = None
query_duration: Any = None


def _metric_export_interval_millis() -> int:
    """How often metrics are exported: $OTEL_METRIC_EXPORT_INTERVAL (the SDK's own variable), else
    15 seconds."""
    from provisa.core import settings_registry  # REQ-1913: declared in settings_catalog

    return settings_registry.value("otel.metric_export_interval")


# The transport a data route is reported under on the request span and in request metrics; any
# other HTTP route is "http".
_HTTP_TRANSPORT_BY_PATH = {
    "/data/graphql": "graphql",
    "/data/sql": "sql",
    "/data/cypher": "cypher",
}


def _bind_http_request_span(span: Any, scope: dict) -> None:  # REQ-1910
    """FastAPI server-span hook: the span the instrumentor just opened is this request's span."""
    if span is None or not span.is_recording():
        return
    bind_request_span(span, _HTTP_TRANSPORT_BY_PATH.get(scope.get("path", ""), "http"))


def _trace_detail_sampler(sample_rate: float) -> Any:  # REQ-1910
    """The sampler that holds a request to ONE span in normal trace detail.

    A root span (no parent, or a parent in another process) is sampled as before: always, or at
    ``sample_rate``. A span with a parent in this process is a child, and inside a request a child
    is recorded only in debug detail — which is what keeps every instrumentor (Redis, httpx, ASGI
    receive/send, database drivers) to no span at all in normal detail without each one needing a
    switch of its own. Outside a request (startup, scheduler, discovery, MV refresh) children are
    recorded as before in either detail.
    """
    from opentelemetry.sdk.trace.sampling import (
        ALWAYS_ON,
        Decision,
        ParentBased,
        Sampler,
        SamplingResult,
        TraceIdRatioBased,
    )
    from opentelemetry.trace import SpanKind, get_current_span

    class _TraceDetailSampler(Sampler):
        def __init__(self) -> None:
            self._base = ParentBased(
                TraceIdRatioBased(sample_rate) if sample_rate < 1.0 else ALWAYS_ON
            )

        def should_sample(
            self,
            parent_context: Any,
            trace_id: int,
            name: str,
            kind: Any = None,
            attributes: Any = None,
            links: Any = None,
            trace_state: Any = None,
        ) -> "SamplingResult":
            parent = get_current_span(parent_context)
            parent_ctx = parent.get_span_context()
            local_child = (
                parent_ctx.is_valid and not parent_ctx.is_remote and parent_ctx.trace_flags.sampled
            )
            if (
                local_child
                and trace_detail() == "normal"
                # A request's server span is opened before anything can bind the request scope, so
                # its direct children (ASGI receive/send) are recognised by their parent's kind.
                and (in_request_scope() or getattr(parent, "kind", None) is SpanKind.SERVER)
            ):
                return SamplingResult(Decision.DROP, trace_state=parent_ctx.trace_state)
            return self._base.should_sample(
                parent_context, trace_id, name, kind, attributes, links, trace_state
            )

        def get_description(self) -> str:
            return f"TraceDetail({self._base.get_description()})"

    return _TraceDetailSampler()


def _otlp_protocol(configured: str = "") -> str:
    """The OTLP transport: the one the caller resolved for its endpoint, else the operator
    setting ``otel.protocol`` (REQ-1913: stored, then OTEL_EXPORTER_OTLP_PROTOCOL, then the
    config's, then grpc)."""
    from provisa.core import settings_registry

    protocol = (configured or settings_registry.value("otel.protocol")).strip().lower()
    if protocol not in ("grpc", "http/protobuf"):
        raise ValueError(
            f"OTLP protocol {protocol!r} is not a transport; use 'grpc' or 'http/protobuf'"
        )
    return protocol


def _is_http_endpoint(endpoint: str, protocol: str = "") -> bool:
    """Return True when the OTLP transport for `endpoint` is OTLP/HTTP rather than gRPC.

    The URL scheme does not answer this. The OTLP spec writes a gRPC endpoint as
    ``http://collector:4317`` exactly as it writes an HTTP one as ``http://collector:4318`` —
    ``OTEL_EXPORTER_OTLP_PROTOCOL`` is what distinguishes them, and it is what every producer
    of an endpoint in this repo (scripts/provisa, start-ui.sh, the Helm chart) points at a
    gRPC port with an http:// scheme. Reading the scheme therefore built an HTTP exporter that
    POSTed every batch to the gRPC port, where the collector resets the connection and the
    BatchSpanProcessor drops the span — the SaaS node exported nothing at all while its
    endpoint, its collector and its parquet writer were all healthy.

    Default is grpc, the spec's default; a deployment pointed at an HTTP-only receiver (the
    otlp2parquet port config/provisa.yaml names, say) declares http/protobuf — in the env var
    or as `observability.protocol`, which arrives here as `protocol`.
    """
    if not endpoint:
        return False
    return _otlp_protocol(protocol) == "http/protobuf"


def _make_span_exporter(endpoint: str, protocol: str = ""):
    if _is_http_endpoint(endpoint, protocol):
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import (
            OTLPSpanExporter as HTTPSpanExporter,
        )

        return HTTPSpanExporter(endpoint=endpoint + "/v1/traces")
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

    return OTLPSpanExporter(endpoint=endpoint, insecure=True)


def _make_metric_exporter(endpoint: str, protocol: str = ""):
    if _is_http_endpoint(endpoint, protocol):
        from opentelemetry.exporter.otlp.proto.http.metric_exporter import (
            OTLPMetricExporter as HTTPMetricExporter,
        )

        return HTTPMetricExporter(endpoint=endpoint + "/v1/metrics")
    from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter

    return OTLPMetricExporter(endpoint=endpoint, insecure=True)


def _make_log_exporter(endpoint: str, protocol: str = ""):
    if _is_http_endpoint(endpoint, protocol):
        from opentelemetry.exporter.otlp.proto.http._log_exporter import (
            OTLPLogExporter as HTTPLogExporter,
        )

        return HTTPLogExporter(endpoint=endpoint + "/v1/logs")
    from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter

    return OTLPLogExporter(endpoint=endpoint, insecure=True)


# The settings sources, highest precedence first.
_SOURCE_ORDER = ("stored", "env", "config", "default")
# What this process's exporters were last attached for: (endpoint, service name, protocol).
_attached: "tuple[str, str, str] | None" = None


def exporter_settings() -> "tuple[str | None, str, str]":
    """(endpoint, service name, protocol) this deployment exports telemetry with (REQ-1913).

    REQ-549: the transport belongs to whichever endpoint won. ``observability.protocol`` in the
    config file describes the config file's endpoint (config/provisa.yaml names otlp2parquet,
    HTTP-only); carried onto an endpoint from a higher source it would speak HTTP at whatever
    receiver the deployment actually pointed at. So a protocol stated by a LOWER source than the
    endpoint's is not that endpoint's, and the endpoint gets the declared default instead.
    """
    from provisa.core import settings_registry

    endpoint = settings_registry.resolve("otel.endpoint")
    protocol = settings_registry.resolve("otel.protocol")
    transport = protocol.value
    if protocol.source != "default" and _SOURCE_ORDER.index(protocol.source) > _SOURCE_ORDER.index(
        endpoint.source
    ):
        transport = settings_registry.setting("otel.protocol").default
    return endpoint.value, settings_registry.value("otel.service_name"), transport


def apply_exporter_settings() -> None:
    """Attach this process's exporters for the endpoint now in force, if it has changed.

    THE apply step for the exporter settings: called by the worker that saves them and by every
    other process when it learns the stored settings changed (the settings registry's
    ``on_change``). With no endpoint there is nothing to export to.
    """
    global _attached
    current = exporter_settings()
    endpoint, service_name, protocol = current
    if endpoint is None or current == _attached:
        return
    attach_otlp_exporters(endpoint, service_name, protocol)
    _attached = (endpoint, service_name, protocol)


def _register_exporter_settings() -> None:
    from provisa.core import settings_registry

    settings_registry.on_change(
        ("otel.endpoint", "otel.protocol", "otel.service_name"),
        lambda *_values: apply_exporter_settings(),
    )


_register_exporter_settings()


def attach_otlp_exporters(
    endpoint: str, service_name: str = "provisa", otlp_protocol: str = ""
) -> None:  # REQ-302, REQ-303, REQ-549
    """Attach OTLP exporters to existing providers when endpoint is set at runtime."""
    import logging
    import provisa.api.otel_setup as _self

    _log = logging.getLogger(__name__)
    try:
        from opentelemetry import metrics
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace.export import BatchSpanProcessor
        from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
        from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
        from opentelemetry._logs import set_logger_provider

        resource = Resource.create({"service.name": service_name})

        # The endpoint now in force replaces the one the process exported to: its span processor
        # and metric exporter are swapped, the old ones shut down.
        from provisa.core import settings_registry

        _process_tracer_provider(
            _trace_detail_sampler(settings_registry.value("otel.sample_rate")), resource
        )
        assert _span_exports is not None  # built with the provider
        _span_exports.set(
            "internal",
            BatchSpanProcessor(
                _make_span_exporter(endpoint, otlp_protocol),
                schedule_delay_millis=settings_registry.value("otel.span_export_delay_millis"),
            ),
        )
        _export_metrics(_make_metric_exporter(endpoint, otlp_protocol), resource)
        _meter = metrics.get_meter("provisa")
        _self.query_counter = _meter.create_counter(
            "provisa.query.executed", description="Total queries executed"
        )
        _self.query_duration = _meter.create_histogram(
            "provisa.query.duration_ms",
            description="Query execution time in milliseconds",
            unit="ms",
        )
        register_query_instruments(_self.query_counter, _self.query_duration)  # REQ-1910

        import logging as _logging

        # The process's one log pipeline: the one in place (set up at start or by an earlier
        # change) is shut down and its handler removed before this endpoint's replaces it.
        global _log_provider
        _shutdown_log_pipeline()
        log_provider = LoggerProvider(resource=resource)
        log_provider.add_log_record_processor(
            BatchLogRecordProcessor(_make_log_exporter(endpoint, otlp_protocol))
        )
        set_logger_provider(log_provider)
        _log_provider = log_provider
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
        '[storage]\nbackend = "s3"\n\n'
        '[storage.s3]\nbucket = "provisa-otel"\n'
        'endpoint = "http://minio:9000"\nregion = "us-east-1"\n\n'
        f"[batch]\nmax_rows = 200000\nmax_bytes = 134217728\nmax_age_secs = {max_age_secs}\n"
    )
    try:
        with open(toml_path, "w") as _f:
            _f.write(content)
    except Exception as exc:
        _log.debug("Could not write otlp2parquet.toml: %s", exc)


def _make_filtering_exporter(  # REQ-545, REQ-546, REQ-547, REQ-548
    inner: "Any",
    redact_sql_literals: bool,
    redact_attributes: list[str],
) -> "Any":
    """Wrap *inner* SpanExporter — redacts spans before delegating. Never mutates originals."""
    try:
        from opentelemetry.sdk.trace.export import SpanExporter, SpanExportResult
        from opentelemetry.sdk.trace import ReadableSpan

        _drop = frozenset(redact_attributes)
        _redact_sql = redact_sql_literals

        def _scrub(attrs: dict) -> dict:
            out = dict(attrs)
            if _redact_sql and "db.statement" in out:
                out["db.statement"] = _SQL_LITERAL_RE.sub("?", out["db.statement"])
            for key in _drop:
                out.pop(key, None)
            return out

        class _FilteringExporter(SpanExporter):
            def export(self, spans: Any) -> "SpanExportResult":
                scrubbed = []
                for span in spans:
                    attrs = dict(span.attributes or {})
                    clean = _scrub(attrs)
                    scrubbed.append(
                        ReadableSpan(
                            name=span.name,
                            context=span.context,
                            parent=span.parent,
                            resource=span.resource,
                            attributes=clean,
                            events=span.events,
                            links=span.links,
                            kind=span.kind,
                            instrumentation_scope=span.instrumentation_scope,
                            status=span.status,
                            start_time=span.start_time,
                            end_time=span.end_time,
                        )
                    )
                return inner.export(scrubbed)

            def shutdown(self) -> None:
                inner.shutdown()

            def force_flush(self, timeout_millis: int = 30000) -> bool:
                return inner.force_flush(timeout_millis)

        return _FilteringExporter()
    except ImportError:
        return inner


def setup_otel(
    app: "Any",
) -> None:  # REQ-302, REQ-303, REQ-330, REQ-545, REQ-546, REQ-547, REQ-548, REQ-549
    """Initialize OpenTelemetry tracing unconditionally.

    Always creates a TracerProvider so module-level tracers work everywhere.
    Only attaches the OTLP exporter when OTEL_EXPORTER_OTLP_ENDPOINT is set —
    without it, spans are created but silently dropped (NoOpSpanExporter).
    This lets the airgapped release emit traces by default; users opt-in to
    collection by pointing OTEL_EXPORTER_OTLP_ENDPOINT at a collector.
    """
    import logging

    _log = logging.getLogger(__name__)
    config_path = config_path_str()
    _config: dict = {}
    try:
        # REQ-1669: includes-aware, so a wrapper config's fragments are seen.
        from provisa.core.config_loader import read_config_with_includes

        _config = read_config_with_includes(config_path)
    except Exception:
        pass
    _otel_cfg: dict = _config.get("observability", {}) if isinstance(_config, dict) else {}
    # REQ-1913: the telemetry settings are operator settings, resolved by the settings registry.
    # This runs while the app object is created — before the control plane is bound — so what is
    # resolved here is environment, then this config file, then the declared defaults; a value
    # stored through the settings page is applied when the process applies its stored settings
    # (apply_exporter_settings), and the settings fixed at start are reported pending until the
    # next start.
    from provisa.core import settings_registry

    if isinstance(_config, dict):
        settings_registry.bind_config(_config)
    _setting = settings_registry.value
    # Which OTLP transport the endpoint speaks is declared, never inferred from the URL — see
    # _is_http_endpoint — and belongs to whichever endpoint won (exporter_settings).
    _endpoint, service_name, otlp_protocol = exporter_settings()
    endpoint = _endpoint if _endpoint is not None else ""
    sample_rate = _setting("otel.sample_rate")
    # REQ-1910: the process default trace detail; a request may bind its own (set_trace_detail).
    configure_trace_detail(_setting("otel.trace_detail"))
    log_level_name = _setting("otel.log_level")
    span_export_delay_millis = _setting("otel.span_export_delay_millis")
    otlp2parquet_max_age_secs = _setting("otel.otlp2parquet_max_age_secs")
    _internal_filter = _otel_cfg.get("telemetry_filter", {})
    _internal_redact_sql = bool(_internal_filter.get("redact_sql_literals", False))
    _internal_redact_attrs = list(_internal_filter.get("redact_attributes", []))
    _support_endpoint = _setting("otel.support_endpoint")
    support_endpoint = _support_endpoint if _support_endpoint is not None else ""
    _support_redact_sql = _setting("otel.support_redact_sql_literals")
    _support_redact_attrs = _setting("otel.support_redact_attributes")
    # REQ-1432: per-subsystem trace switches. SubsystemTracesConfig owns the names and the
    # defaults — catalog_database off, everything else on — so the config file only has to state
    # the departures from them.
    from provisa.core.models import SubsystemTracesConfig

    _subsystems = SubsystemTracesConfig(
        **{
            name: _setting(f"otel.subsystem_traces.{name}")
            for name in SubsystemTracesConfig.model_fields
        }
    )
    global _attached
    _attached = (endpoint, service_name, otlp_protocol) if endpoint else None
    _write_otlp2parquet_toml(otlp2parquet_max_age_secs, config_path)
    try:
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

        resource = Resource.create({"service.name": service_name})
        # The process's provider: built by the first setup, reused by every later one.
        _process_tracer_provider(_trace_detail_sampler(sample_rate), resource)
        assert _span_exports is not None  # built with the provider
        if endpoint:
            from opentelemetry.sdk.trace.export import BatchSpanProcessor

            _internal_exporter = _make_filtering_exporter(
                _make_span_exporter(endpoint, otlp_protocol),
                _internal_redact_sql,
                _internal_redact_attrs,
            )
            _span_exports.set(
                "internal",
                BatchSpanProcessor(
                    _internal_exporter, schedule_delay_millis=span_export_delay_millis
                ),
            )
            _log.info(
                "OTel tracing → %s (service=%s, redact_sql=%s)",
                endpoint,
                service_name,
                _internal_redact_sql,
            )
        if support_endpoint:
            from opentelemetry.sdk.trace.export import BatchSpanProcessor

            _support_exporter = _make_filtering_exporter(
                _make_span_exporter(support_endpoint, otlp_protocol),
                _support_redact_sql,
                _support_redact_attrs,
            )
            _span_exports.set(
                "support",
                BatchSpanProcessor(
                    _support_exporter, schedule_delay_millis=span_export_delay_millis
                ),
            )
            _log.info(
                "OTel support tracing → %s (redact_sql=%s)", support_endpoint, _support_redact_sql
            )
        else:
            _log.debug(
                "OTel tracing active (no collector; spans dropped). "
                "Set OTEL_EXPORTER_OTLP_ENDPOINT to export."
            )
        # ── Metrics ──────────────────────────────────────────────────────────
        if endpoint:
            from opentelemetry import metrics

            _export_metrics(_make_metric_exporter(endpoint, otlp_protocol), resource)
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
            register_query_instruments(_self.query_counter, _self.query_duration)  # REQ-1910

        # ── Logs ─────────────────────────────────────────────────────────────
        if endpoint:
            global _log_provider
            import logging as _logging
            from opentelemetry.sdk._logs import LoggerProvider
            from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
            from opentelemetry._logs import set_logger_provider
            from opentelemetry.sdk._logs import LoggingHandler

            # A process holds ONE log pipeline: one set up before (an app built again in the same
            # process) is shut down and its root handler removed, never left exporting beside this.
            _shutdown_log_pipeline()
            log_provider = LoggerProvider(resource=resource)
            log_provider.add_log_record_processor(
                BatchLogRecordProcessor(_make_log_exporter(endpoint, otlp_protocol))
            )
            set_logger_provider(log_provider)
            _log_provider = log_provider
            handler = LoggingHandler(
                level=getattr(_logging, log_level_name, _logging.WARNING),
                logger_provider=log_provider,
            )
            _logging.getLogger().addHandler(handler)
            _log.info("OTel logs → %s (service=%s)", endpoint, service_name)

        # REQ-1432: each block is one subsystem, instrumented only when its switch is on.
        # REQ-1910: every instrumentor below stays installed in either trace detail, because the
        # detail is resolved per request; _trace_detail_sampler is what gives a normal-detail
        # request no per-command and no per-message span from any of them.
        if _subsystems.http_api:
            FastAPIInstrumentor.instrument_app(app, server_request_hook=_bind_http_request_span)
        if _subsystems.outbound_http:
            try:
                from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor

                HTTPXClientInstrumentor().instrument()
            except ImportError:
                pass
        if _subsystems.catalog_database:
            try:
                # The catalog database is reached through psycopg 3 (provisa.core.database).
                from opentelemetry.instrumentation.psycopg import PsycopgInstrumentor

                PsycopgInstrumentor().instrument()
            except ImportError:
                pass
        if _subsystems.result_cache:
            try:
                from opentelemetry.instrumentation.redis import RedisInstrumentor

                RedisInstrumentor().instrument()
            except ImportError:
                pass
        if _subsystems.document_sources:
            try:
                from opentelemetry.instrumentation.pymongo import PymongoInstrumentor

                PymongoInstrumentor().instrument()
            except ImportError:
                pass
        if _subsystems.search_sources:
            try:
                from opentelemetry.instrumentation.elasticsearch import ElasticsearchInstrumentor

                ElasticsearchInstrumentor().instrument()
            except ImportError:
                pass
        if _subsystems.grpc_services:
            try:
                from opentelemetry.instrumentation.grpc import (
                    GrpcInstrumentorClient,
                    GrpcInstrumentorServer,
                )

                GrpcInstrumentorClient().instrument()
                GrpcInstrumentorServer().instrument()
            except ImportError:
                pass
    except ImportError:
        _log.warning("OTel packages missing; skipping instrumentation")
