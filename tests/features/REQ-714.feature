# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-714 — Neo4j Export
  # Nodes are MERGE'd into Neo4j using _provisa_id as the deduplication key, with labels derived from tableLabel or compound…

  Scenario: REQ-714 default behaviour
    Given a node with tableLabel "User" and properties {name: "Alice", age: 30}
    When the node is exported via POST /data/neo4j-export
    Then Neo4j contains a node with label "User", property _provisa_id set, and properties SET via += operator
