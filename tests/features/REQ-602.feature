# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-602 — GraphQL Remote Schema Connector (REQ-307–313)
  # Schema generation synthesizes `ColumnMetadata` for remote schema tables (GraphQL remote, gRPC remote, OpenAPI) at schema…

  Scenario: REQ-602 default behaviour
    Given remote schema tables (GraphQL remote, gRPC remote, OpenAPI) registered in Provisa
    When schema generation runs
    Then ColumnMetadata is synthesized with correct type mappings equivalent to catalog introspection for local tables
