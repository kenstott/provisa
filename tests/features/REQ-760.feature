# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-760 — Cypher Query Frontend (Phase AU)
  # String, list, and numeric functions (LEFT, RIGHT, TRIM, SIZE, COUNT DISTINCT, REDUCE) translate directly to Trino equiva…

  Scenario: REQ-760 default behaviour
    Given a Cypher query MATCH (n) RETURN left(n.name, 3), size(collect(n.age))
    When the translator processes it
    Then it emits Trino functions: left(...), cardinality(array_agg(...))
