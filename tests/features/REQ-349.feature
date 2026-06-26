# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-349 — Cypher Query Frontend (Phase AU)
  # When a `RETURN` clause references a whole node variable, relationship variable, or path variable (not a scalar property)…

  Scenario: REQ-349 default behaviour
    Given a Cypher RETURN clause referencing a whole node variable
    When Stage 3 rewrite runs
    Then the node columns are wrapped into a single JSON object via CAST(ROW(...) AS JSON)
