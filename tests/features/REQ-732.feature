# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-732 — SharePoint Connector
  # SharePoint tables can be registered in Provisa with known column definitions when the Calcite connector does not expose…

  Scenario: REQ-732 default behaviour
    Given the Calcite sharepoint connector does not expose information_schema.columns
    When a user registers a table via GraphQL registerTable mutation with columns=[{name, visibleTo, writableBy}]
    Then the table is created with the supplied column definitions
