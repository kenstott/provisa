# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-715 — Neo4j Export
  # Node properties are written to Neo4j using SET n += {...} with Cypher literal encoding to safely handle nested objects,…

  Scenario: REQ-715 default behaviour
    Given a node with properties {active: true, count: 42, name: "Test", nested: {key: "value"}}
    When the node is exported
    Then all properties are SET in Neo4j with correct Cypher literal types
