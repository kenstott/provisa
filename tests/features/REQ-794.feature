# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-794 — Neo4j Export
  # Query result values are introspected for node and edge structures. Nodes are identified by presence of {id, tableLabel,…

  Scenario: REQ-794 default behaviour
    Given a Cypher query returning nested nodes and edges
    When result rows are introspected
    Then nodes with {id, tableLabel, properties} are extracted
    And edges with {identity, startNode, endNode, type} are extracted
    And extraction works on deeply nested result values
