# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-997 — Execution Routing
  # Execution routing for CTAS: resolve the write path via existing resolve_write_path(target_source_type, engine). If sourc…

  Scenario: REQ-997 default behaviour
    Given source_a on Snowflake and target schema on Snowflake
    When CREATE TABLE target_schema.new_table AS SELECT ... FROM source_a.table
    Then the entire CTAS is pushed to Snowflake engine (zero-copy)

    Given source_a on Snowflake and target schema on BigQuery
    When CREATE TABLE target_schema.new_table AS SELECT ... FROM source_a.table
    Then Snowflake executes the SELECT, lands rows in a temporary table
    Then rows are bulk-loaded to BigQuery via store_writer landing write face
    And the BigQuery engine is never on the write path
