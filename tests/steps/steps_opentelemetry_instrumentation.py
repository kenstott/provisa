# Copyright (c) 2026 Kenneth Stott
# Canary: {canary}
#
# This source code is licensed under the Business Source License 1.1

"""BDD step definitions for REQ-549 — OpenTelemetry OTLP transport auto-detection."""

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
    """OTLP/HTTP transport auto-detection from URL scheme."""


@given(parsers.parse("an OTLP endpoint URL starting with http:// or https://"))
@given("an OTLP endpoint URL starting with http:// or https://")
def given_http_endpoint(shared_data):
    endpoint = "http://otel-collector:4318"
    shared_data["endpoint"] = endpoint
    # Confirm scheme detection classifies this as an HTTP endpoint
    assert otel_setup._is_http_endpoint(endpoint) is True


@when("Provisa configures the exporter")
def when_configure_exporter(shared_data):
    endpoint = shared_data["endpoint"]
    shared_data["span_exporter"] = otel_setup._make_span_exporter(endpoint)
    shared_data["metric_exporter"] = otel_setup._make_metric_exporter(endpoint)
    shared_data["log_exporter"] = otel_setup._make_log_exporter(endpoint)


def _exporter_endpoint(exporter) -> str:
    # OTLP/HTTP exporters expose the resolved endpoint via private/public attrs.
    for attr in ("_endpoint", "endpoint", "_otlp_endpoint"):
        val = getattr(exporter, attr, None)
        if isinstance(val, str) and val:
            return val
    raise AssertionError(f"Could not determine endpoint for exporter {exporter!r}")


@then(
    "OTLP/HTTP is used with path suffixes /v1/traces, /v1/metrics, /v1/logs appended automatically")
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


def test_is_http_endpoint_http_scheme():
    """_is_http_endpoint returns True for http:// URLs."""
    assert otel_setup._is_http_endpoint("http://localhost:4318") is True


def test_is_http_endpoint_https_scheme():
    """_is_http_endpoint returns True for https:// URLs."""
    assert otel_setup._is_http_endpoint("https://otel.example.com:4318") is True


def test_is_http_endpoint_grpc_scheme():
    """_is_http_endpoint returns False for grpc:// URLs."""
    assert otel_setup._is_http_endpoint("grpc://localhost:4317") is False


def test_is_http_endpoint_empty_string():
    """_is_http_endpoint returns False for an empty string."""
    assert otel_setup._is_http_endpoint("") is False


def test_make_span_exporter_http_uses_http_class():
    """_make_span_exporter with http:// endpoint returns an OTLP/HTTP span exporter."""
    exporter = otel_setup._make_span_exporter("http://otel-collector:4318")
    assert "http" in type(exporter).__module__


def test_make_span_exporter_http_path_suffix():
    """_make_span_exporter appends /v1/traces to an http:// endpoint."""
    exporter = otel_setup._make_span_exporter("http://otel-collector:4318")
    ep = _exporter_endpoint(exporter)
    assert ep == "http://otel-collector:4318/v1/traces", f"got {ep}"


def test_make_span_exporter_https_path_suffix():
    """_make_span_exporter appends /v1/traces to an https:// endpoint."""
    exporter = otel_setup._make_span_exporter("https://otel.example.com:4318")
    ep = _exporter_endpoint(exporter)
    assert ep == "https://otel.example.com:4318/v1/traces", f"got {ep}"


def test_make_metric_exporter_http_uses_http_class():
    """_make_metric_exporter with http:// endpoint returns an OTLP/HTTP metric exporter."""
    exporter = otel_setup._make_metric_exporter("http://otel-collector:4318")
    assert "http" in type(exporter).__module__


def test_make_metric_exporter_http_path_suffix():
    """_make_metric_exporter appends /v1/metrics to an http:// endpoint."""
    exporter = otel_setup._make_metric_exporter("http://otel-collector:4318")
    ep = _exporter_endpoint(exporter)
    assert ep == "http://otel-collector:4318/v1/metrics", f"got {ep}"


def test_make_log_exporter_http_uses_http_class():
    """_make_log_exporter with http:// endpoint returns an OTLP/HTTP log exporter."""
    exporter = otel_setup._make_log_exporter("http://otel-collector:4318")
    assert "http" in type(exporter).__module__


def test_make_log_exporter_http_path_suffix():
    """_make_log_exporter appends /v1/logs to an http:// endpoint."""
    exporter = otel_setup._make_log_exporter("http://otel-collector:4318")
    ep = _exporter_endpoint(exporter)
    assert ep == "http://otel-collector:4318/v1/logs", f"got {ep}"


def test_make_span_exporter_grpc_uses_grpc_class():
    """_make_span_exporter with a non-http scheme returns an OTLP/gRPC span exporter."""
    exporter = otel_setup._make_span_exporter("grpc://otel-collector:4317")
    assert "grpc" in type(exporter).__module__


def test_make_metric_exporter_grpc_uses_grpc_class():
    """_make_metric_exporter with a non-http scheme returns an OTLP/gRPC metric exporter."""
    exporter = otel_setup._make_metric_exporter("grpc://otel-collector:4317")
    assert "grpc" in type(exporter).__module__


def test_make_log_exporter_grpc_uses_grpc_class():
    """_make_log_exporter with a non-http scheme returns an OTLP/gRPC log exporter."""
    exporter = otel_setup._make_log_exporter("grpc://otel-collector:4317")
    assert "grpc" in type(exporter).__module__


def test_http_and_grpc_exporters_are_different_types():
    """HTTP and gRPC span exporters must be distinct classes."""
    http_exporter = otel_setup._make_span_exporter("http://otel-collector:4318")
    grpc_exporter = otel_setup._make_span_exporter("grpc://otel-collector:4317")
    assert type(http_exporter) is not type(grpc_exporter)


def test_https_endpoint_also_uses_http_exporter():
    """https:// scheme must produce the same OTLP/HTTP exporter class as http://."""
    http_exporter = otel_setup._make_span_exporter("http://otel-collector:4318")
    https_exporter = otel_setup._make_span_exporter("https://otel.example.com:4318")
    assert type(http_exporter) is type(https_exporter)
