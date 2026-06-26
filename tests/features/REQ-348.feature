# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-348 — Cypher Query Frontend (Phase AU)
  # Path queries — `shortestPath(...)`, `allShortestPaths(...)`, and variable-length relationship patterns `[*1..n]` — trans…

  Scenario: REQ-348 default behaviour
    Given a Cypher query with shortestPath or [*1..n] variable-length pattern
    When the translator processes it
    Then it emits a WITH RECURSIVE CTE and rejects unbounded [*] patterns at compile time
