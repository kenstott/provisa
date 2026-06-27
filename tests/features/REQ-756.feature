# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-756 — Cypher Query Frontend (Phase AU)
  # Subquery predicates — EXISTS { ... }, COUNT { ... }, COLLECT { ... } — translate to correlated SQL subqueries. EXISTS be…

  Scenario: REQ-756 default behaviour
    Given a Cypher query MATCH (n) WHERE EXISTS { ... } RETURN n
    When the translator processes it
    Then it emits a correlated EXISTS subquery in the WHERE clause
