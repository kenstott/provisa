# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-352 — Cypher Query Frontend (Phase AU)
  # Cypher named parameters (`$param`) are translated to Trino positional parameters at compile time. Parameter types are in…

  Scenario: REQ-352 default behaviour
    Given a Cypher query with $param and no default
    When the parameter is missing from the request
    Then it is rejected at compile time
