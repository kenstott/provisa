# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-549 — OpenTelemetry Instrumentation
  # Provisa auto-detects OTLP transport from the endpoint URL scheme: `http://` or `https://` schemes use OTLP/HTTP with `/v…

  Scenario: REQ-549 default behaviour
    Given an OTLP endpoint URL starting with http:// or https://
    When Provisa configures the exporter
    Then OTLP/HTTP is used with path suffixes /v1/traces, /v1/metrics, /v1/logs appended automatically
