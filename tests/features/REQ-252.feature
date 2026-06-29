# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-252 — Compiler & Schema
  # Schema inference must be supported where the Trino connector provides auto-discovery (MongoDB, Cassandra, Elasticsearch)…

  Scenario: REQ-252 default behaviour
    Given a MongoDB source with discover: true
    When the schema compiler runs
    Then it introspects the connector and generates a starting column list, with explicit definitions taking precedence
