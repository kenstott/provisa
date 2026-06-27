# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-735 — Cassandra Connector
  # Cassandra source adapter maps CQL data types to Trino types (text→VARCHAR, bigint→BIGINT, timestamp→TIMESTAMP, uuid→UUID…

  Scenario: REQ-735 default behaviour
    Given a Cassandra table with partition and clustering keys
    When the adapter discovers the schema
    Then CQL column types are mapped to Trino types and key columns are annotated
