# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-776 — Cypher Query Frontend (Phase AU)
  # OPTIONAL MATCH chains (e.g., `OPTIONAL MATCH (a)-[r1]->(b) OPTIONAL MATCH (b)-[r2]->(c)`) translate to left-joined multi…

  Scenario: REQ-776 default behaviour
    Given a Cypher query with a chain of OPTIONAL MATCH clauses
    When the translator processes it
    Then it emits sequential LEFT JOINs with null-aware WHERE conditions
