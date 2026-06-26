# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-353 — Cypher Query Frontend (Phase AU)
  # WITHDRAWN (2026-06-19). Cross-source Cypher queries are allowed — Trino joins across catalogs natively, so a query whose…

  Scenario: REQ-353 default behaviour
    Given a Cypher query whose node labels resolve to tables on different Trino catalogs
    When the translator processes it
    Then it generates a cross-catalog JOIN and executes normally without error
