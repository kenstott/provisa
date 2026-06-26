# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-427 — Vector Search
  # A virtual embedding column may be declared on a table with a `generated_from` subquery that must return exactly one text…

  Scenario: REQ-427 default behaviour
    Given a virtual embedding column declared with a generated_from subquery
    When the column is declared
    Then Provisa validates the subquery returns exactly one text value per row against a sample row
