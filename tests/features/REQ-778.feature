# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-778 — Cypher Query Frontend (Phase AU)
  # Cypher /query/cypher endpoint returns a typed response with `columns` (array of column names), `rows` (array of result o…

  Scenario: REQ-778 default behaviour
    Given a Cypher query submitted to POST /data/cypher
    When the endpoint processes and executes it
    Then the response includes columns, rows, and error fields with the correct shape
