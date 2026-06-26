# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-347 — Cypher Query Frontend (Phase AU)
  # Cypher clauses map to SQL as follows: `MATCH` → `JOIN`; `OPTIONAL MATCH` → `LEFT JOIN`; `WHERE` → `WHERE`; `RETURN` → `S…

  Scenario: REQ-347 default behaviour
    Given a Cypher query with MATCH, WHERE, RETURN, ORDER BY, and LIMIT clauses
    When the translator processes it
    Then it emits SQL with JOIN, WHERE, SELECT, ORDER BY, and LIMIT clauses respectively
