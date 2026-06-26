# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-238 — Warm Tables (Local SSD via Trino File Cache)
  # Warm tables -- frequently queried RDBMS tables materialized into the Iceberg results catalog so Trino's built-in file sy…

  Scenario: REQ-238 default behaviour
    Given a table materialized into the Iceberg results catalog with Trino file cache enabled
    When a query targets that table
    Then Trino serves the result from local SSD Parquet cache at ~10-50ms latency
