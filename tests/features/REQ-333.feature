# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-333 — Ingest Sources — Governed HTTP Push Receiver (REQ-331–337)
  # Stewards define the schema for each ingest table as a list of columns with `column_name`, `data_type` (SQL type, allowli…

  Scenario: REQ-333 default behaviour
    Given an ingest table defined with column_name, data_type, and path for each column
    When Provisa starts up
    Then CREATE TABLE IF NOT EXISTS DDL is executed with system columns _received_at and _updated_at injected
