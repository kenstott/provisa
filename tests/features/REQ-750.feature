# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-750 — Cypher Query Frontend (Phase AU)
  # Cypher graph variables (nodes, edges, paths) returned in the RETURN clause are serialized as JSON objects with canonical…

  Scenario: REQ-750 default behaviour
    Given a Cypher query RETURN n, r, p where n is a node, r is an edge, p is a path
    When the Cypher router executes the query
    Then the response includes JSON objects for each graph variable with the canonical keys
