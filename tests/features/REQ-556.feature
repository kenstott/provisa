# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-556 — ABAC Approval Hook
  # The approval hook client implements a circuit breaker that opens after 5 consecutive failures (configurable via circuit_…

  Scenario: REQ-556 default behaviour
    Given an approval hook endpoint that fails consecutively 5 times
    When the circuit breaker threshold is reached
    Then the circuit opens and enters half-open state after the configured cooldown period
