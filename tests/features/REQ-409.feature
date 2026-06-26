# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-409 — Compiler & Schema
  # Cypher translator detects ISO 8601 datetime string literals in WHERE clauses and wraps them as `TIMESTAMP '...'` before…

  Scenario: REQ-409 default behaviour
    Given a Cypher WHERE clause with an ISO 8601 datetime string literal
    When the translator processes it
    Then it wraps the literal as TIMESTAMP '...' before SQLGlot parsing
