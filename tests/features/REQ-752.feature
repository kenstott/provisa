# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-752 — Cypher Query Frontend (Phase AU)
  # Intermediate node property access in multi-hop MATCH patterns (e.g., `(p:Person)-[:WORKS_AT]->(c:Company)-[:HAS_DEPT]->(…

  Scenario: REQ-752 default behaviour
    Given a Cypher query with 3+ node variables in a path
    When the translator processes it
    Then all node aliases are available in WHERE and RETURN, and intermediate property access resolves correctly
