# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-397 — Execution & Routing
  # When a PK is available, the exclusion WHERE clause must use `n.<pk_col> IN [<pk_value>]` instead of `id(n) IN [<nodeId>]…

  Scenario: REQ-397 default behaviour
    Given a query with a node exclusion clause and an available primary key
    When the exclusion WHERE clause is generated
    Then it uses n.<pk_col> IN [<pk_value>] rather than id(n) IN [<nodeId>]
