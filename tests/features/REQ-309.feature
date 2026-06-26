# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-309 — GraphQL Remote Schema Connector (REQ-307–313)
  # At query execution time, Provisa translates the incoming GraphQL request into a remote GraphQL query and forwards it to…

  Scenario: REQ-309 default behaviour
    Given a remote GraphQL source query executed within TTL
    When the same query is issued again
    Then results are served from the Iceberg cache in Trino with zero remote hops
