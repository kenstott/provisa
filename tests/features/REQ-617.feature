# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-617 — gRPC
  # Role selection on every Provisa gRPC RPC is via the `x-provisa-role` metadata key. Missing or unrecognised role metadata…

  Scenario: REQ-617 default behaviour
    Given a gRPC caller omitting or providing an unrecognised x-provisa-role metadata key
    When the RPC is received
    Then the call is rejected with UNAUTHENTICATED
