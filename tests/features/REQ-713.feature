# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-713 — Neo4j Export
  # Neo4j export endpoint accepts graph nodes and edges via POST /data/neo4j-export, including target URL, credentials, data…

  Scenario: REQ-713 default behaviour
    Given a list of nodes with id, tableLabel, and properties, and edges with start, end, and type
    When POST /data/neo4j-export is called with url, username, password, database, nodes, and edges
    Then the nodes and edges are transmitted to the Neo4j HTTP transactional API
