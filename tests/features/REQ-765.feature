# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-765 — Cypher Query Frontend (Phase AU)
  # Pattern comprehensions (e.g., `[(p)-[:WORKS_AT]->(c:Company) | c.name]`) translate to ARRAY(...SELECT...) subqueries wit…

  Scenario: REQ-765 default behaviour
    Given a Cypher query MATCH (p) RETURN [(p)-[:WORKS_AT]->(c:Company) | c.name]
    When the translator processes it
    Then it emits ARRAY(SELECT c."name" FROM ... WHERE ...)
