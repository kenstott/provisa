# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-795 — Neo4j Export
  # POST /data/neo4j-export endpoint accepts edge-only requests (empty nodes array) and exports relationships without requir…

  Scenario: REQ-795 default behaviour
    Given nodes already exported to Neo4j
    When POST /data/neo4j-export is called with empty nodes array and populated edges
    Then edges are successfully exported using start/end node IDs
    And Neo4j relationship count matches expected count
