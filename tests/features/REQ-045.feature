# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-045 — API & Integration
  # gRPC Arrow Flight endpoint for high-throughput consumers; Trino produces Arrow natively for zero-copy delivery.

  Scenario: REQ-045 default behaviour
    Given a high-throughput consumer connecting via gRPC Arrow Flight
    When Trino produces Arrow natively
    Then data streams with zero-copy delivery to the consumer
