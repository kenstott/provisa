# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-531 — Security
  # Masked columns are rejected from WHERE and HAVING clauses at query parse time, before execution, preventing inference of…

  Scenario: REQ-531 default behaviour
    Given a query with a masked column in a WHERE or HAVING clause
    When the query is parsed
    Then it is rejected at parse time before execution via V005 validation
