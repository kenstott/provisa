# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-786 — Cypher Graph Analytics
  # Impute-relationships endpoint accepts visible node set with stable integer ids and resolves them to database primary key…

  Scenario: REQ-786 default behaviour
    Given a request with nodes: [{label: "Meta", id: 10}, {label: "Meta", id: 11}, ...]
    When the endpoint fetches rows from node_ids WHERE id = ANY([10, 11, ...])
    Then it extracts the raw PK from composite_id ("label|pk_value")
    And uses the raw PK values in the WHERE clause for relationship queries
    And returns stable integer ids in the result edges (via register_node_ids)
