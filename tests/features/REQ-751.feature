# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-751 — Cypher Query Frontend (Phase AU)
  # Variable-length relationship patterns `[*1..n]` are compiled to recursive CTEs in Trino SQL. The CTE enforces max-hop bo…

  Scenario: REQ-751 default behaviour
    Given a Cypher query with [*1..5] pattern between two node types
    When the translator processes it
    Then it emits a WITH RECURSIVE CTE with hop-count guards and JSON_ARRAY edges
