# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-275 — Federation Performance
  # On source registration, Provisa runs `ANALYZE` against the registered source's tables (where the connector supports it)…

  Scenario: REQ-275 default behaviour
    Given a source being registered with a connector that supports ANALYZE
    When registration completes
    Then Provisa runs ANALYZE on the source's tables to prime the cost-based optimizer
