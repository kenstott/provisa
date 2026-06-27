# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-717 — Neo4j Export
  # Neo4j export endpoint uses HTTP Basic authentication via Authorization header (base64-encoded username:password) to auth…

  Scenario: REQ-717 default behaviour
    Given credentials username: "neo4j", password: "secret"
    When POST /data/neo4j-export is called
    Then Authorization header contains "Basic " + base64("neo4j:secret")
