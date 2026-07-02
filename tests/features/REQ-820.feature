# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-820 — Live Query Routing
  # Live poll execution routes through Trino (federated query), not the PostgreSQL connection pool. Any federated SQL source…

  Scenario: REQ-820 default behaviour
    Given a live poll query targeting a federated BigQuery table
    When the poll interval triggers
    Then the query executes through Trino (not the PostgreSQL pool)
    And the watermark is persisted to live_query_state in PostgreSQL
