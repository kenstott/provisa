# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-296 — Query-API Sources (Neo4j & SPARQL)
  # Neo4j table registration includes a query preview step. On submission, Provisa executes the steward's Cypher query again…

  Scenario: REQ-296 default behaviour
    Given a steward submitting a Cypher query for Neo4j table registration
    When the query returns node or edge objects instead of flat scalar projections
    Then registration is blocked with an error directing the steward to use explicit scalar RETURN aliases
