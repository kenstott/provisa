# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-819 — Live Delivery Configuration
  # Per-table live delivery configuration is now configurable via the admin GraphQL API and admin UI (TablesPage). Configura…

  Scenario: REQ-819 default behaviour
    Given the admin GraphQL API for table mutations
    When updateTable is called with live configuration (query_id, watermark_column, poll_interval, delivery, outputs)
    Then the configuration is persisted to registered_tables.live and the live engine is notified

    Given the admin UI TablesPage
    When an operator edits live config for a table
    Then changes are reflected in the database and take effect without server restart
