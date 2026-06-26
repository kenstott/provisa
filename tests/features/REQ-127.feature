# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-127 — JDBC/ODBC Integration
  # `getTables()` returns registered tables and views visible to the authenticated role, by their registered names.

  Scenario: REQ-127 default behaviour
    Given a JDBC client calling getTables()
    When the authenticated role has visibility to certain tables and views
    Then only those registered tables and views are returned by their registered names
