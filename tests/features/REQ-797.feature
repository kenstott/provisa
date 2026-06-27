# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-797 — Neo4j Export
  # E2E neo4j export validates exported graph integrity: node and relationship counts in target Neo4j instance match expecte…

  Scenario: REQ-797 default behaviour
    Given a completed export with N nodes and E edges to Neo4j
    When node and relationship counts are queried in target Neo4j
    Then node count is ≤ N (due to MERGE deduplication on _provisa_id)
    And relationship count exactly equals E
    And all node counts are > 0
