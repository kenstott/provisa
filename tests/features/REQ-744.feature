# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-744 — Security
  # Masking preserves query structure — ORDER BY, LIMIT, GROUP BY, and other clauses remain unchanged; only SELECT projectio…

  Scenario: REQ-744 default behaviour
    Given a query with ORDER BY, LIMIT, GROUP BY, or other clauses
    When masking is injected
    Then the clauses remain unchanged; result is a new object, input is unchanged
