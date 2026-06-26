# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-239 — Warm Tables (Local SSD via Trino File Cache)
  # Warm table auto-promotion -- track query frequency per table (increment counter on each compiled query). Tables exceedin…

  Scenario: REQ-239 default behaviour
    Given a table whose query count exceeds warm_tables.query_threshold within a refresh interval
    When the promotion check runs
    Then the table is auto-materialized into Iceberg; tables falling below threshold are demoted
