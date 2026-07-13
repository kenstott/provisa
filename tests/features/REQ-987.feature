# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-987 — Arrow Flight Transport
  # Databricks SQL warehouse must be supported as a first-class Provisa federation engine with Arrow-native transport capabi…

  Scenario: REQ-987 default behaviour
    Given the Databricks federation engine advertising ROWS/ARROW/ARROW_STREAM
    When a batch at or above COPY_INTO_ROW_THRESHOLD is landed into the store
    Then the batch is staged as Parquet and ingested via COPY INTO, never a per-row INSERT loop

    Given a batch below the threshold
    When it is landed
    Then a single multi-row INSERT is used, never row-by-row
