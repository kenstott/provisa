# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-777 — Cypher Query Frontend (Phase AU)
  # UNION ALL queries (e.g., `MATCH (n) RETURN ... UNION ALL MATCH (m) RETURN ...`) preserve distinct column aliases in both…

  Scenario: REQ-777 default behaviour
    Given a Cypher UNION ALL query with property filters on both node and edge properties
    When the translator processes it
    Then it emits a valid SQL UNION ALL with matching column aliases and unified WHERE conditions
