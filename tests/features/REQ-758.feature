# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-758 — Cypher Query Frontend (Phase AU)
  # Bidirectional edge traversal (e.g., `(a)-[]-(b)` without direction marker) expands to UNION ALL when multiple relationsh…

  Scenario: REQ-758 default behaviour
    Given a Cypher query MATCH (a:Person)-[]-(b:Company) where forward and backward relationships exist
    When the translator processes it
    Then it emits UNION ALL with one branch per direction
