# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-802 — Protocol Support
  # Bolt protocol (Neo4j binary protocol) TCP server at port 5251 accepts Cypher queries and mutations, transpiles through P…

  Scenario: REQ-802 default behaviour
    Given a Cypher client connecting to Bolt port 5251
    When the client sends Cypher query "MATCH (n:Person) RETURN n"
    Then the server accepts the handshake (magic + version negotiation)
    And the query is transpiled to SQL via WriteTranslator
    And governance (RLS, masking, visibility) is applied at compile time
    And results are executed and returned as Bolt structures (nodes, relationships)
    And response is serialized via PackStream and framed for TCP
