# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-027 — Execution & Routing
  # Single-source queries route to direct RDBMS connection; SQLGlot transpiles to target dialect. Target: sub-100ms to low h…

  Scenario: REQ-027 default behaviour
    Given a query targeting a single registered RDBMS source
    When the query is executed
    Then it routes directly to the RDBMS connection and SQLGlot transpiles to the target dialect
