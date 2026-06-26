# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-126 — JDBC/ODBC Integration
  # JDBC driver that exposes registered tables and views as virtual tables. Connection authenticates against Provisa, maps u…

  Scenario: REQ-126 default behaviour
    Given a JDBC client connecting to Provisa
    When the connection authenticates and maps the user to a role
    Then registered tables and views are accessible as virtual tables
