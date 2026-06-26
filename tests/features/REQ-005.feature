# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-005 — Query Governance
  # Result-size ceilings are defined per role/table in config (`max_rows`); Stage 2 injects LIMIT when a query would exceed…

  Scenario: REQ-005 default behaviour
    Given a role with a configured max_rows ceiling for a table
    When a query references that table and would exceed the ceiling
    Then Stage 2 injects a LIMIT capping results at the role's ceiling
