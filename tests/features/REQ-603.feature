# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-603 — Query Governance
  # V002 relationship governance: every JOIN ON condition in SQL and Cypher queries must match an approved, registered relat…

  Scenario: REQ-603 default behaviour
    Given a SQL or Cypher query with a JOIN ON condition
    When the compiler validates the query
    Then it is rejected at compile time if the join is not backed by a registered relationship
