# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-129 — JDBC/ODBC Integration
  # `executeQuery(sql)` runs arbitrary SQL against registered tables/views, passes it through Stage 2 governance, executes v…

  Scenario: REQ-129 default behaviour
    Given a JDBC client calling executeQuery(sql)
    When the SQL is passed through Stage 2 governance and executed via the HTTP API
    Then the result is deserialized into a JDBC ResultSet via Arrow IPC or JSON transport
