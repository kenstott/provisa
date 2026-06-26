# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-051 — Output & Delivery
  # Arrow buffer via gRPC Arrow Flight endpoint; Trino produces Arrow natively.

  Scenario: REQ-051 default behaviour
    Given a query submitted via the Arrow Flight endpoint
    When Trino executes the query
    Then results are delivered as Arrow record batches via gRPC with no intermediate serialization
