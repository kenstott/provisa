# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-128 — JDBC/ODBC Integration
  # `getColumns(tableName)` introspects the registered table/view output schema — column names and types from compiled metad…

  Scenario: REQ-128 default behaviour
    Given a JDBC client calling getColumns(tableName)
    When compiled metadata is available and filtered by role visibility
    Then column names and types are returned from the registered schema
