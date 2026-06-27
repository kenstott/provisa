# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-792 — Neo4j Export
  # GET /data/graph-schema endpoint returns the current graph schema including all node labels (as array of {label: "Domain:…

  Scenario: REQ-792 default behaviour
    Given a graph with multiple node labels and relationship types
    When GET /data/graph-schema is called
    Then the response includes node_labels array with all node labels
    And the response includes relationship_types array with source/target pairs
