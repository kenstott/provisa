# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-028 — Execution & Routing
  # Cross-source queries route to Trino; SQLGlot transpiles to Trino SQL. Target: 300-500ms.

  Scenario: REQ-028 default behaviour
    Given a query joining tables from multiple registered sources
    When the query is executed
    Then it routes to Trino and SQLGlot transpiles to Trino SQL within 300–500ms
