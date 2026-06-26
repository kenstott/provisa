# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-189 — Hasura Migration Converters
  # DDN converter resolves GraphQL field names to physical column names through `ObjectType.dataConnectorTypeMapping[].field…

  Scenario: REQ-189 default behaviour
    Given a DDN supergraph with ObjectType.dataConnectorTypeMapping[].fieldMapping entries
    When the DDN converter runs
    Then all GraphQL field names in relationships, permissions, and column definitions are resolved to physical column names
