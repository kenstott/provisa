# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-298 — Query-API Sources (Neo4j & SPARQL)
  # API source caller extended to support HTTP POST with a request body. Required for Neo4j (POST to `/db/{database}/query/v…

  Scenario: REQ-298 default behaviour
    Given a Neo4j or SPARQL source requiring POST requests with a request body
    When the API source caller executes a query
    Then the POST body is transmitted correctly and existing GET endpoints are unaffected
