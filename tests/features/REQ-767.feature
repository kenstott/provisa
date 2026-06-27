# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-767 — Cypher Query Frontend (Phase AU)
  # Recursive CTE shortestPath and allShortestPaths functions enforce max-hop bounds at compile time. shortestPath injects L…

  Scenario: REQ-767 default behaviour
    Given a Cypher query MATCH p = shortestPath((a:Person)-[:KNOWS*..5]->(b:Person))
    When the translator processes it
    Then it emits a WITH RECURSIVE CTE with ORDER BY hops LIMIT 1
