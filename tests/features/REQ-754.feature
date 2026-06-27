# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-754 — Cypher Query Frontend (Phase AU)
  # Correlated CALL subqueries (e.g., `CALL { WITH x MATCH (x)-[:REL]->(y) RETURN y }`) translate to CROSS JOIN LATERAL subq…

  Scenario: REQ-754 default behaviour
    Given a Cypher query with CALL { WITH x ... } correlated subquery
    When the translator processes it
    Then it emits CROSS JOIN LATERAL with the outer variable bound in the join condition
