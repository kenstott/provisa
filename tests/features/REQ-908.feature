# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-908 — Source Connectors
  # PgDuckdbIcebergConnector reads Apache Iceberg tables in-place via pg_duckdb's iceberg_scan (DuckDB iceberg extension) in…

  Scenario: REQ-908 default behaviour
    Given a source configured with pg_duckdb_iceberg (source_type "iceberg", key "pg_duckdb_iceberg")
    When the federation engine initializes
    Then the connector probes that iceberg_scan is registered (pg_duckdb without iceberg lacks the function, so probes correctly disable it)
    And if available, queries emit iceberg_scan('<root>', allow_moved_paths := true)
    And governance predicates are pushed down into the scan
    And results are correctly federated with other sources.
