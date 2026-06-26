# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-577 — Cypher Query Frontend (Phase AU)
  # When multiple schema paths of equal hop count connect the same start and end node types, all matching paths are emitted…

  Scenario: REQ-577 default behaviour
    Given multiple schema paths of equal hop count between the same node types
    When the translator processes a shortestPath query
    Then all matching paths are emitted as UNION ALL branches without deduplication
