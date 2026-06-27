# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-763 — Cypher Query Frontend (Phase AU)
  # UNWIND clauses (e.g., `UNWIND [1,2,3] AS x`) translate to Trino UNNEST with CROSS JOIN. Parameters in UNWIND (e.g., `$li…

  Scenario: REQ-763 default behaviour
    Given a Cypher query UNWIND [1, 2, 3] AS x RETURN x
    When the translator processes it
    Then it emits CROSS JOIN (SELECT ... FROM UNNEST(ARRAY[...]))
