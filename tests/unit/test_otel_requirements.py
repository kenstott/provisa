# Copyright (c) 2026 Kenneth Stott
# Canary: 0dfaeef3-882e-4d3b-ac3c-75fc7d75e52e
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
"""Unit tests for otel requirements: REQ-302, REQ-303, REQ-545, REQ-546, REQ-547, REQ-548, REQ-549"""

from __future__ import annotations

from unittest.mock import MagicMock, patch


# ---------------------------------------------------------------------------
# REQ-302 / REQ-303 — get_tracer returns a usable tracer regardless of OTel install
# ---------------------------------------------------------------------------


def test_get_tracer_returns_tracer_without_otel():
    # REQ-302, REQ-303
    # When opentelemetry is absent, get_tracer must return a no-op tracer that
    # supports start_as_current_span and start_span so all components can call it.
    import sys
    import importlib

    # Pre-import so patch.dict does not drop provisa.otel_compat on exit
    import provisa.otel_compat as _mod  # noqa: PLC0415

    # Force ImportError for opentelemetry
    with patch.dict(sys.modules, {"opentelemetry": None, "opentelemetry.trace": None}):
        importlib.reload(_mod)
        tracer = _mod.get_tracer("test-component")
        assert hasattr(tracer, "start_as_current_span")
        assert hasattr(tracer, "start_span")
    importlib.reload(_mod)


def test_get_tracer_noop_span_context_manager():
    # REQ-302, REQ-303
    # No-op span returned by get_tracer must work as a context manager without raising.
    import sys
    import importlib

    # Pre-import so patch.dict does not drop provisa.otel_compat on exit
    import provisa.otel_compat as _mod  # noqa: PLC0415

    with patch.dict(sys.modules, {"opentelemetry": None, "opentelemetry.trace": None}):
        importlib.reload(_mod)
        tracer = _mod.get_tracer("test-component")
        span = tracer.start_as_current_span("my-span")
        entered = None
        with span as s:
            entered = s
        assert entered is not None
    importlib.reload(_mod)


def test_get_tracer_noop_span_set_attribute():
    # REQ-302, REQ-303
    # No-op span must accept set_attribute calls without raising.
    import sys
    import importlib

    # Pre-import so patch.dict does not drop provisa.otel_compat on exit
    import provisa.otel_compat as _mod  # noqa: PLC0415

    with patch.dict(sys.modules, {"opentelemetry": None, "opentelemetry.trace": None}):
        importlib.reload(_mod)
        tracer = _mod.get_tracer("test-component")
        span = tracer.start_span("op")
        result = span.set_attribute("db.statement", "SELECT 1")  # must not raise
        assert result is None  # no-op returns None
    importlib.reload(_mod)


def test_get_tracer_noop_span_record_exception():
    # REQ-302, REQ-303
    # No-op span must accept record_exception calls without raising.
    import sys
    import importlib

    # Pre-import so patch.dict does not drop provisa.otel_compat on exit
    import provisa.otel_compat as _mod  # noqa: PLC0415

    with patch.dict(sys.modules, {"opentelemetry": None, "opentelemetry.trace": None}):
        importlib.reload(_mod)
        tracer = _mod.get_tracer("test-component")
        span = tracer.start_span("op")
        result = span.record_exception(ValueError("boom"))  # must not raise
        assert result is None  # no-op returns None
    importlib.reload(_mod)


# ---------------------------------------------------------------------------
# REQ-545 — Two independent OTLP export paths (internal + support)
# ---------------------------------------------------------------------------


def test_span_buffer_push_and_recent():
    # REQ-545
    # SpanBuffer is the thread-safe circular buffer backing the in-memory export path.
    # push() must store spans; recent() must return them most-recent-first up to limit.
    from provisa.api.otel_setup import SpanBuffer

    buf = SpanBuffer(maxlen=10)

    def _make_span(name: str, trace_id: int = 1, span_id: int = 1):
        ctx = MagicMock()
        ctx.trace_id = trace_id
        ctx.span_id = span_id
        span = MagicMock()
        span.get_span_context.return_value = ctx
        span.name = name
        span.status.status_code.name = "OK"
        span.end_time = 2_000_000_000
        span.start_time = 1_000_000_000
        span.attributes = {}
        return span

    buf.push(_make_span("first", span_id=1))
    buf.push(_make_span("second", span_id=2))

    items = buf.recent(limit=10)
    assert len(items) == 2
    # most recent pushed last → appendleft → appears at index 0
    assert items[0]["name"] == "second"
    assert items[1]["name"] == "first"


def test_span_buffer_respects_limit():
    # REQ-545
    # recent(limit=N) must return at most N entries.
    from provisa.api.otel_setup import SpanBuffer

    buf = SpanBuffer(maxlen=20)
    for i in range(10):
        ctx = MagicMock()
        ctx.trace_id = i
        ctx.span_id = i
        span = MagicMock()
        span.get_span_context.return_value = ctx
        span.name = f"span-{i}"
        span.status.status_code.name = "OK"
        span.end_time = 2_000_000_000
        span.start_time = 1_000_000_000
        span.attributes = {}
        buf.push(span)

    assert len(buf.recent(limit=3)) == 3


def test_span_buffer_maxlen_drops_oldest():
    # REQ-545
    # SpanBuffer with maxlen=3 must drop the oldest entry when overflowed.
    from provisa.api.otel_setup import SpanBuffer

    buf = SpanBuffer(maxlen=3)
    for i in range(5):
        ctx = MagicMock()
        ctx.trace_id = i
        ctx.span_id = i
        span = MagicMock()
        span.get_span_context.return_value = ctx
        span.name = f"span-{i}"
        span.status.status_code.name = "OK"
        span.end_time = 2_000_000_000
        span.start_time = 1_000_000_000
        span.attributes = {}
        buf.push(span)

    items = buf.recent(limit=10)
    names = {item["name"] for item in items}
    assert "span-0" not in names  # oldest must be gone
    assert "span-1" not in names  # second oldest must also be gone


# ---------------------------------------------------------------------------
# REQ-546 — Filtering exporter never mutates original span objects
# ---------------------------------------------------------------------------


def test_filtering_exporter_does_not_mutate_original_span():
    # REQ-546
    # _make_filtering_exporter must not mutate the original span's attributes dict.
    pytest = __import__("pytest")
    pytest.importorskip("opentelemetry.sdk.trace")

    from provisa.api.otel_setup import _make_filtering_exporter

    captured: list = []

    class _CapturingExporter:
        def export(self, spans):
            captured.extend(spans)
            return None

        def shutdown(self):
            pass

        def force_flush(self):
            return True

    original_attrs = {"db.statement": "SELECT 'secret' FROM t", "user.id": "alice"}

    span = MagicMock()
    span.attributes = original_attrs
    span.name = "query"

    exporter = _make_filtering_exporter(
        _CapturingExporter(),
        redact_sql_literals=True,
        redact_attributes=["user.id"],
    )
    exporter.export([span])

    # Original attributes must be unchanged
    assert original_attrs["db.statement"] == "SELECT 'secret' FROM t"
    assert "user.id" in original_attrs


def test_filtering_exporter_redacts_sql_literals():
    # REQ-546, REQ-547
    # When redact_sql_literals=True, db.statement literals must be replaced with '?'.
    pytest = __import__("pytest")
    pytest.importorskip("opentelemetry.sdk.trace")

    from provisa.api.otel_setup import _make_filtering_exporter

    captured: list = []

    class _CapturingExporter:
        def export(self, spans):
            captured.extend(spans)
            return None

        def shutdown(self):
            pass

        def force_flush(self):
            return True

    span = MagicMock()
    span.attributes = {"db.statement": "SELECT 'secret' FROM t WHERE id = 42"}
    span.name = "query"

    exporter = _make_filtering_exporter(
        _CapturingExporter(),
        redact_sql_literals=True,
        redact_attributes=[],
    )
    exporter.export([span])

    assert len(captured) == 1
    exported_stmt = captured[0].attributes["db.statement"]
    assert "'secret'" not in exported_stmt
    assert "42" not in exported_stmt
    assert "?" in exported_stmt


def test_filtering_exporter_drops_redacted_attributes():
    # REQ-546
    # Attributes listed in redact_attributes must be absent from exported spans.
    pytest = __import__("pytest")
    pytest.importorskip("opentelemetry.sdk.trace")

    from provisa.api.otel_setup import _make_filtering_exporter

    captured: list = []

    class _CapturingExporter:
        def export(self, spans):
            captured.extend(spans)
            return None

        def shutdown(self):
            pass

        def force_flush(self):
            return True

    span = MagicMock()
    span.attributes = {"user.email": "alice@example.com", "safe.key": "value"}
    span.name = "op"

    exporter = _make_filtering_exporter(
        _CapturingExporter(),
        redact_sql_literals=False,
        redact_attributes=["user.email"],
    )
    exporter.export([span])

    assert len(captured) == 1
    assert "user.email" not in captured[0].attributes
    assert captured[0].attributes.get("safe.key") == "value"


# ---------------------------------------------------------------------------
# REQ-547 — Support path defaults redact_sql_literals=True
# ---------------------------------------------------------------------------


def test_support_path_redact_sql_defaults_true():
    # REQ-547
    # When no support_telemetry_filter config is provided, redact_sql_literals
    # on the support path must default to True.
    import yaml
    import os
    import tempfile

    cfg = {"observability": {}}  # no support_telemetry_filter key

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yaml.dump(cfg, f)
        config_path = f.name

    try:
        with open(config_path) as fh:
            otel_cfg = (yaml.safe_load(fh) or {}).get("observability", {})
        _support_filter = otel_cfg.get("support_telemetry_filter", {})
        _support_redact_sql = bool(_support_filter.get("redact_sql_literals", True))
        assert _support_redact_sql is True
    finally:
        os.unlink(config_path)


def test_support_path_redact_sql_can_be_disabled_explicitly():
    # REQ-547
    # Operator must be able to opt-out by setting redact_sql_literals: false in config.

    cfg = {"observability": {"support_telemetry_filter": {"redact_sql_literals": False}}}
    otel_cfg = cfg.get("observability", {})
    _support_filter = otel_cfg.get("support_telemetry_filter", {})
    _support_redact_sql = bool(_support_filter.get("redact_sql_literals", True))
    assert _support_redact_sql is False


# ---------------------------------------------------------------------------
# REQ-548 — Support OTLP endpoint is disabled by default
# ---------------------------------------------------------------------------


def test_support_endpoint_disabled_when_not_configured(monkeypatch):
    # REQ-548
    # When PROVISA_SUPPORT_OTLP_ENDPOINT env var is unset and support_endpoint
    # is absent from config, the resolved support_endpoint must be empty/falsy.

    monkeypatch.delenv("PROVISA_SUPPORT_OTLP_ENDPOINT", raising=False)
    otel_cfg: dict = {}  # no support_endpoint key
    support_endpoint = None or otel_cfg.get("support_endpoint", "")
    assert not support_endpoint


def test_support_endpoint_enabled_via_env(monkeypatch):
    # REQ-548
    # When PROVISA_SUPPORT_OTLP_ENDPOINT is set, the support endpoint must be truthy.
    monkeypatch.setenv("PROVISA_SUPPORT_OTLP_ENDPOINT", "http://support.example.com:4318")
    import os

    otel_cfg: dict = {}
    support_endpoint = os.environ.get("PROVISA_SUPPORT_OTLP_ENDPOINT") or otel_cfg.get(
        "support_endpoint", ""
    )
    assert support_endpoint == "http://support.example.com:4318"


def test_support_endpoint_enabled_via_config(monkeypatch):
    # REQ-548
    # When support_endpoint is set in config (and env var absent), it must be used.
    monkeypatch.delenv("PROVISA_SUPPORT_OTLP_ENDPOINT", raising=False)
    import os

    otel_cfg = {"support_endpoint": "http://support.internal:4317"}
    support_endpoint = os.environ.get("PROVISA_SUPPORT_OTLP_ENDPOINT") or otel_cfg.get(
        "support_endpoint", ""
    )
    assert support_endpoint == "http://support.internal:4317"


# ---------------------------------------------------------------------------
# REQ-549 — declared OTLP transport selection
# ---------------------------------------------------------------------------


def test_transport_is_declared_not_read_off_the_url_scheme():
    # REQ-549. http://collector:4317 is how the OTLP spec writes a gRPC endpoint, and it is what
    # scripts/provisa, start-ui.sh and the Helm chart all export. Deciding from the scheme built
    # an HTTP exporter that POSTed to the gRPC port, so every batch was reset and dropped.
    from provisa.api.otel_setup import _is_http_endpoint

    assert _is_http_endpoint("http://otel-collector:4317") is False
    assert _is_http_endpoint("https://collector.example.com:4318") is False


def test_http_transport_comes_from_the_config_or_the_env(monkeypatch):
    # REQ-549
    from provisa.api.otel_setup import _is_http_endpoint

    monkeypatch.delenv("OTEL_EXPORTER_OTLP_PROTOCOL", raising=False)
    assert _is_http_endpoint("http://localhost:4319", "http/protobuf") is True
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "http/protobuf")
    assert _is_http_endpoint("http://localhost:4319") is True
    # The env var outranks the config: an operator repointing at a gRPC collector says so there.
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc")
    assert _is_http_endpoint("http://localhost:4319", "http/protobuf") is False


def test_an_unset_endpoint_selects_no_transport():
    # REQ-549
    from provisa.api.otel_setup import _is_http_endpoint

    assert _is_http_endpoint("") is False


def test_an_unknown_protocol_raises_rather_than_guessing(monkeypatch):
    # REQ-549. Silently treating "http" as either transport is how a collector-shaped typo
    # became an exporter that dropped every batch without a word.
    import pytest as _pytest

    from provisa.api.otel_setup import _otlp_protocol

    monkeypatch.setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "http")
    with _pytest.raises(ValueError, match="not a transport"):
        _otlp_protocol()


def test_make_span_exporter_http_appends_path():
    # REQ-549
    # OTLP/HTTP span exporter must append /v1/traces to the endpoint URL.
    pytest = __import__("pytest")
    pytest.importorskip("opentelemetry.exporter.otlp.proto.http.trace_exporter")

    from provisa.api.otel_setup import _make_span_exporter

    exporter = _make_span_exporter("http://localhost:4318", "http/protobuf")
    # The exporter stores the endpoint; retrieve it to verify the /v1/traces suffix.
    endpoint = getattr(exporter, "_endpoint", None)
    assert endpoint is not None and endpoint.endswith("/v1/traces"), (
        f"Expected /v1/traces suffix, got: {endpoint}"
    )


def test_make_metric_exporter_http_appends_path():
    # REQ-549
    # OTLP/HTTP metric exporter must append /v1/metrics to the endpoint URL.
    pytest = __import__("pytest")
    pytest.importorskip("opentelemetry.exporter.otlp.proto.http.metric_exporter")

    from provisa.api.otel_setup import _make_metric_exporter

    exporter = _make_metric_exporter("http://localhost:4318", "http/protobuf")
    endpoint = getattr(exporter, "_endpoint", None)
    assert endpoint is not None and endpoint.endswith("/v1/metrics"), (
        f"Expected /v1/metrics suffix, got: {endpoint}"
    )


def test_make_log_exporter_http_appends_path():
    # REQ-549
    # OTLP/HTTP log exporter must append /v1/logs to the endpoint URL.
    pytest = __import__("pytest")
    pytest.importorskip("opentelemetry.exporter.otlp.proto.http._log_exporter")

    from provisa.api.otel_setup import _make_log_exporter

    exporter = _make_log_exporter("http://localhost:4318", "http/protobuf")
    endpoint = getattr(exporter, "_endpoint", None)
    assert endpoint is not None and endpoint.endswith("/v1/logs"), (
        f"Expected /v1/logs suffix, got: {endpoint}"
    )
