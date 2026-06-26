# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-143 — Arrow Flight
  # Arrow Flight server (port 8815) streams record batches via gRPC. Full security pipeline applied.

  Scenario: REQ-143 default behaviour
    Given a client connects to the Arrow Flight server on port 8815
    When a query is submitted
    Then record batches are streamed via gRPC with the full Provisa security pipeline applied
