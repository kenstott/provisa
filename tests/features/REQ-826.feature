# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-826 — Caching
  # Federation strategy and freshness management — the datasources → federation engine arrow of REQ-825. The binding between…

  Scenario: REQ-826 default behaviour
    Given an engine that can only materialize an OpenAPI datasource
    When a query references that source
    Then federate() returns the MATERIALIZED strategy, loads/refreshes the data, and the query reads it in place
    And the router does not attempt a live/VIRTUAL route

    Given a MATERIALIZED strategy with cache_ttl=300
    When the table has not been reloaded within 300 seconds
    Then federate() reloads it before the query executes (cache_ttl means reload interval)

    Given an engine that can ATTACH an RDBMS datasource live
    When a query references that source
    Then federate() returns the VIRTUAL strategy and the query reads it live with no copy

    Given a NoSQL datasource federated by SCAN or MATERIALIZED
    When the view is created
    Then the schema is pinned from the semantic layer mapping, not inferred
