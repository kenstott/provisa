# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-755 — Cypher Query Frontend (Phase AU)
  # Node label alternation (e.g., `(n:Label1|Label2)`) translates to UNION ALL branches in SQL, one per label candidate. Eac…

  Scenario: REQ-755 default behaviour
    Given a Cypher query MATCH (n:TypeA|TypeB) RETURN n
    When the translator processes it
    Then it emits UNION ALL with one branch per type, each selecting from the appropriate table
