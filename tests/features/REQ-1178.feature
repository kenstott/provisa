# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-1178 — Connector Configuration
  # Expand ClickHouse federation engine reach beyond the 7 OOTB source types by (1) adding OOTB native engines — SQLite (DAT…

  Scenario: REQ-1178 default behaviour
    Given a ClickHouse federation engine
    When an operator registers a SQLite source (OOTB, no config)
    Then the engine reaches sqlite live and the runtime returns its federated rows
    When an operator declares a clickhouse_database/table/scan connector for a new source_type in config/custom_connectors.yaml
    Then the engine reaches that new source_type with no code change
    And an absent ClickHouse integration engine fails loud at attach time
