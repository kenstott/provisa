# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-766 — Cypher Query Frontend (Phase AU)
  # length(p) function on a path variable returns the hop count (number of edges). For recursive CTE paths, it returns the `…

  Scenario: REQ-766 default behaviour
    Given a Cypher query MATCH p = (...) RETURN length(p)
    When the translator processes it
    Then it extracts the `hops` field from the path object or returns 1 for single-hop paths
