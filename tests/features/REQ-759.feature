# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-759 — Cypher Query Frontend (Phase AU)
  # Backward relationship traversal (e.g., `(c:Company)<-[:WORKS_AT]-(p:Person)`) inverts the join condition from the forwar…

  Scenario: REQ-759 default behaviour
    Given a Cypher query MATCH (c:Company)<-[:WORKS_AT]-(p:Person)
    When the translator processes it
    Then it emits a JOIN with swapped ON condition: p.company_id = c.id
