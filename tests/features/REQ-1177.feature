# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-1177 — Connector Configuration
  # Operators may declare custom source connectors (proprietary FDWs or ATTACH extensions) in config without code changes. A…

  Scenario: REQ-1177 default behaviour
    Given an operator declares a custom source connector in config/custom_connectors.yaml with no code change
    When the descriptor is a duckdb_attach for a new source_type over a real ducklake catalog
    Then the DuckDB engine reaches that source_type and the runtime attaches it and returns its rows
    When the descriptor is a duckdb_scan for a new source_type over a real .xlsx via read_xlsx
    Then the DuckDB engine reaches that source_type and the runtime scans it in place and returns its rows
    When the descriptor is a generic pg_fdw for a new source_type
    Then the Postgres engine reaches that source_type and its connector emits standard SQL/MED IMPORT FOREIGN SCHEMA DDL
    And a descriptor naming an unknown kind fails loud at load rather than leaving the source_type silently unreachable
