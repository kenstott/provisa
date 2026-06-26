# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-138 — Large Result Redirect & CTAS
  # Trino-native formats (Parquet, ORC) use CTAS — Trino writes directly to S3 via Iceberg, data never passes through Provis…

  Scenario: REQ-138 default behaviour
    Given a redirect format of Parquet or ORC
    When the query executes
    Then Trino writes results directly to S3 via Iceberg CTAS and no data passes through Provisa
