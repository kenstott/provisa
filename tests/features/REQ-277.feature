# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-277 — Federation Performance
  # Per-query and per-table/view session property overrides: steward or developer can attach named session hints (`join_dist…

  Scenario: REQ-277 default behaviour
    Given a query or table/view with session hints configured
    When the query is executed via the federation engine
    Then Provisa injects the corresponding SET SESSION statements before execution
