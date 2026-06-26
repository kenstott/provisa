# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-050 — Output & Delivery
  # Denormalized tabular: fully flattened single table, Parquet or CSV, single file or partitioned.

  Scenario: REQ-050 default behaviour
    Given a query with nested relationships
    When the output format is denormalized tabular (Parquet or CSV)
    Then results are fully flattened into a single table, optionally partitioned
