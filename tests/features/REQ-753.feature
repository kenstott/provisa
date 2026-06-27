# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-753 — Cypher Query Frontend (Phase AU)
  # Path object RETURN (e.g., `RETURN p`) emits a JSON_OBJECT with `nodes` (array of node objects), `edges` (array of edge o…

  Scenario: REQ-753 default behaviour
    Given a Cypher query MATCH p = (...) RETURN p
    When the translator processes it
    Then it emits JSON_OBJECT with nodes, edges, and length fields
