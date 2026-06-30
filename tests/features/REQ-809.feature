# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-809 — SQLGlot Transpilation
  # Correlated scalar subqueries in PG-style canonical SQL are lifted into CTEs before the query is forwarded to Trino. rewr…

  Scenario: REQ-809 default behaviour
    Given a PG-style query containing a correlated scalar subquery
    When the query is transpiled to Trino SQL
    Then the correlated subquery is lifted into a CTE joined on the correlation key
    And the forwarded SQL contains no correlated scalar subquery
