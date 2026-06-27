# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-719 — Neo4j Export
  # Neo4j export endpoint returns {imported: N, errors: [...]} structure, allowing partial success where some statements fai…

  Scenario: REQ-719 default behaviour
    Given an export with 10 nodes, where 1 node fails due to a Neo4j constraint violation
    When POST /data/neo4j-export completes
    Then the response contains imported: 9, errors: ["constraint violation message"]
