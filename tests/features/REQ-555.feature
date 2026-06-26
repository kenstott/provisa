# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-555 — ABAC Approval Hook
  # The gRPC approval hook maintains a single persistent channel per Provisa instance (one grpc.aio channel reused across al…

  Scenario: REQ-555 default behaviour
    Given a Provisa instance configured with gRPC approval hook
    When multiple approval hook calls are made
    Then a single persistent grpc.aio channel is reused across all calls
