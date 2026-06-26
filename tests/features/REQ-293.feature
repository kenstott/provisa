# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-293 — JDBC/ODBC Integration
  # JDBC driver transport via Arrow Flight — connect to Provisa's existing Flight server (`grpc://host:8815`) for streaming…

  Scenario: REQ-293 default behaviour
    Given a JDBC client connected to Provisa
    When the Flight server is reachable
    Then results stream as Arrow record batches with backpressure; falls back to HTTP silently if not
