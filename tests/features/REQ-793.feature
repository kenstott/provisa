# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-793 — Neo4j Export
  # POST /data/cypher endpoint accepts parameterless Cypher queries and returns row data. Queries requiring parameters are r…

  Scenario: REQ-793 default behaviour
    Given a parameterless Cypher query like "MATCH (n:Domain:Table) RETURN n"
    When POST /data/cypher is called with the query
    Then the response returns {rows: [...]}} with query results
    And a query requiring parameters returns error in response
