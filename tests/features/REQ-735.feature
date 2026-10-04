# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-735 — Cassandra Connector
  # Cassandra source adapter maps CQL data types to engine types (text to VARCHAR, bigint to BIGINT, timestamp to TIMESTAMP,…

  Scenario: REQ-735 default behaviour
    Given a Cassandra table with partition and clustering keys
    When the adapter discovers the schema
    Then CQL column types are mapped to engine types and key columns are annotated
