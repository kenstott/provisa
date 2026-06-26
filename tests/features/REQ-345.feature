# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-345 — Cypher Query Frontend (Phase AU)
  # Provisa exposes a `POST /query/cypher` endpoint that accepts a Cypher SELECT query and optional named parameters (`$para…

  Scenario: REQ-345 default behaviour
    Given a graph user submitting a Cypher SELECT query to POST /query/cypher
    When the compiler processes it
    Then it compiles to SQL, executes via Trino, and applies Stage 2 governance identically to
    GraphQL queries
