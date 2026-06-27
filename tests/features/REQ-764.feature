# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-764 — Cypher Query Frontend (Phase AU)
  # IN list predicates (e.g., `n.age IN [25, 30, 35]`) translate directly to SQL IN clauses with literal or parameter values…

  Scenario: REQ-764 default behaviour
    Given a Cypher query MATCH (n) WHERE n.age IN [25, 30, 35] RETURN n.name
    When the translator processes it
    Then it emits SQL IN (25, 30, 35) in the WHERE clause
