# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-886 — Observability
  # Engine-emitted I/O tracing for UDF/transformer invocations must be mandatory and non-bypassable, recording UDF name, tra…

  Scenario: REQ-886 default behaviour
    Given a user invokes a UDF (transformer or custom function) via pgwire or SQL
    When the UDF executes and accesses tables via pgwire data derefs
    Then the engine records an invocation trace with UDF name, transport, identity, input refs, output size, duration, and status, stamping a trace ID into the UDF session so that pgwire audit rows for the UDF's data access join to the invocation trace via correlation ID, reconstructing the full data lineage precisely
    And if the UDF implements W3C traceparent propagation, its interior spans nest under the invocation span automatically via OTEL context
