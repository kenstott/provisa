# Copyright (c) 2026 Kenneth Stott
# Canary: 6816413e-2d78-4997-9479-6673ec38e60c
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1

"""BDD step definitions for REQ-549 — the declared OpenTelemetry OTLP transport."""

from __future__ import annotations

import pytest
from pytest_bdd import given, when, then, parsers, scenario

import provisa.api.otel_setup as otel_setup


@pytest.fixture
def shared_data() -> dict:
    return {}


@scenario(
    "../features/REQ-549.feature",
    "REQ-549 default behaviour",
)
def test_req_549_default_behaviour():
    """OTLP/HTTP is selected by the declared protocol, never by the URL scheme."""


@given(parsers.parse("an OTLP endpoint whose declared protocol is http/protobuf"))
@given("an OTLP endpoint whose declared protocol is http/protobuf")
def given_http_endpoint(shared_data):
    endpoint = "http://otel-collector:4318"
    shared_data["endpoint"] = endpoint
    shared_data["protocol"] = "http/protobuf"
    assert otel_setup._is_http_endpoint(endpoint, "http/protobuf") is True


@when("Provisa configures the exporter")
def when_configure_exporter(shared_data):
    endpoint = shared_data["endpoint"]
    protocol = shared_data["protocol"]
    shared_data["span_exporter"] = otel_setup._make_span_exporter(endpoint, protocol)
    shared_data["metric_exporter"] = otel_setup._make_metric_exporter(endpoint, protocol)
    shared_data["log_exporter"] = otel_setup._make_log_exporter(endpoint, protocol)


def _exporter_endpoint(exporter) -> str:
    # OTLP/HTTP exporters expose the resolved endpoint via private/public attrs.
    for attr in ("_endpoint", "endpoint", "_otlp_endpoint"):
        val = getattr(exporter, attr, None)
        if isinstance(val, str) and val:
            return val
    raise AssertionError(f"Could not determine endpoint for exporter {exporter!r}")


@then(
    "OTLP/HTTP is used with path suffixes /v1/traces, /v1/metrics, /v1/logs appended automatically"
)
def then_http_paths_appended(shared_data):
    base = shared_data["endpoint"]

    span_ep = _exporter_endpoint(shared_data["span_exporter"])
    metric_ep = _exporter_endpoint(shared_data["metric_exporter"])
    log_ep = _exporter_endpoint(shared_data["log_exporter"])

    assert span_ep == base + "/v1/traces", f"span endpoint was {span_ep}"
    assert metric_ep == base + "/v1/metrics", f"metric endpoint was {metric_ep}"
    assert log_ep == base + "/v1/logs", f"log endpoint was {log_ep}"

    # Confirm these are the HTTP exporter classes, not gRPC.
    assert "http" in type(shared_data["span_exporter"]).__module__
    assert "http" in type(shared_data["metric_exporter"]).__module__
    assert "http" in type(shared_data["log_exporter"]).__module__


def test_declared_http_protobuf_selects_http():
    """The declared protocol, not the scheme, is what makes the transport HTTP."""
    assert otel_setup._is_http_endpoint("http://localhost:4318", "http/protobuf") is True
    assert otel_setup._is_http_endpoint("https://otel.example.com:4318", "http/protobuf") is True


def test_an_http_scheme_alone_is_still_grpc():
    """http://collector:4317 is how the spec writes a gRPC endpoint — the scheme decides nothing."""
    assert otel_setup._is_http_endpoint("http://otel-collector:4317") is False
    assert otel_setup._is_http_endpoint("grpc://localhost:4317") is False


def test_is_http_endpoint_empty_string():
    """_is_http_endpoint returns False for an empty string."""
    assert otel_setup._is_http_endpoint("") is False


def test_make_span_exporter_http_uses_http_class():
    """_make_span_exporter under http/protobuf returns an OTLP/HTTP span exporter."""
    exporter = otel_setup._make_span_exporter("http://otel-collector:4318", "http/protobuf")
    assert "http" in type(exporter).__module__


def test_make_span_exporter_http_path_suffix():
    """_make_span_exporter appends /v1/traces under http/protobuf."""
    exporter = otel_setup._make_span_exporter("http://otel-collector:4318", "http/protobuf")
    ep = _exporter_endpoint(exporter)
    assert ep == "http://otel-collector:4318/v1/traces", f"got {ep}"


def test_make_span_exporter_https_path_suffix():
    """_make_span_exporter appends /v1/traces to an https:// endpoint under http/protobuf."""
    exporter = otel_setup._make_span_exporter("https://otel.example.com:4318", "http/protobuf")
    ep = _exporter_endpoint(exporter)
    assert ep == "https://otel.example.com:4318/v1/traces", f"got {ep}"


def test_make_metric_exporter_http_uses_http_class():
    """_make_metric_exporter under http/protobuf returns an OTLP/HTTP metric exporter."""
    exporter = otel_setup._make_metric_exporter("http://otel-collector:4318", "http/protobuf")
    assert "http" in type(exporter).__module__


def test_make_metric_exporter_http_path_suffix():
    """_make_metric_exporter appends /v1/metrics under http/protobuf."""
    exporter = otel_setup._make_metric_exporter("http://otel-collector:4318", "http/protobuf")
    ep = _exporter_endpoint(exporter)
    assert ep == "http://otel-collector:4318/v1/metrics", f"got {ep}"


def test_make_log_exporter_http_uses_http_class():
    """_make_log_exporter under http/protobuf returns an OTLP/HTTP log exporter."""
    exporter = otel_setup._make_log_exporter("http://otel-collector:4318", "http/protobuf")
    assert "http" in type(exporter).__module__


def test_make_log_exporter_http_path_suffix():
    """_make_log_exporter appends /v1/logs under http/protobuf."""
    exporter = otel_setup._make_log_exporter("http://otel-collector:4318", "http/protobuf")
    ep = _exporter_endpoint(exporter)
    assert ep == "http://otel-collector:4318/v1/logs", f"got {ep}"


def test_make_span_exporter_grpc_uses_grpc_class():
    """The default transport is gRPC, so an undeclared endpoint gets the gRPC span exporter."""
    exporter = otel_setup._make_span_exporter("http://otel-collector:4317")
    assert "grpc" in type(exporter).__module__


def test_make_metric_exporter_grpc_uses_grpc_class():
    """The default transport is gRPC, so an undeclared endpoint gets the gRPC metric exporter."""
    exporter = otel_setup._make_metric_exporter("http://otel-collector:4317")
    assert "grpc" in type(exporter).__module__


def test_make_log_exporter_grpc_uses_grpc_class():
    """The default transport is gRPC, so an undeclared endpoint gets the gRPC log exporter."""
    exporter = otel_setup._make_log_exporter("http://otel-collector:4317")
    assert "grpc" in type(exporter).__module__


def test_http_and_grpc_exporters_are_different_types():
    """HTTP and gRPC span exporters must be distinct classes."""
    http_exporter = otel_setup._make_span_exporter("http://otel-collector:4318", "http/protobuf")
    grpc_exporter = otel_setup._make_span_exporter("http://otel-collector:4317", "grpc")
    assert type(http_exporter) is not type(grpc_exporter)


def test_an_unknown_protocol_raises_rather_than_guessing():
    """A collector-shaped typo must stop the exporter, not silently pick a transport."""
    with pytest.raises(ValueError, match="not a transport"):
        otel_setup._otlp_protocol("http")
