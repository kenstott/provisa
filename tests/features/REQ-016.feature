# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-016 — Registration & Governance
  # Table publication triggers schema generation pass; table immediately available in query builder.

  Scenario: REQ-016 default behaviour
    Given a steward who publishes a table
    When the publication completes
    Then a schema generation pass is triggered and the table is immediately available in the query
    builder
