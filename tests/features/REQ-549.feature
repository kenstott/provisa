# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-549 — OpenTelemetry Instrumentation
  # Provisa takes the OTLP transport from an explicit declaration — OTEL_EXPORTER_OTLP_PROTOCOL, else observability.protoc…

  Scenario: REQ-549 default behaviour
    Given an OTLP endpoint whose declared protocol is http/protobuf
    When Provisa configures the exporter
    Then OTLP/HTTP is used with path suffixes /v1/traces, /v1/metrics, /v1/logs appended automatically
