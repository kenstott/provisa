# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-778 — Cypher Query Frontend (Phase AU)
  # [SUPERSEDED by REQ-778, 2026-10-24 -- an error is answered in the app's ApiError body, not an `error` field. Kept here f…

  Scenario: REQ-778 default behaviour
    Given a Cypher query submitted to POST /data/cypher
    When the endpoint processes and executes it
    Then the response includes columns, rows, and error fields with the correct shape
