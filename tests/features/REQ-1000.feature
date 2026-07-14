# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-1000 — Name Resolution
  # Placement disambiguation at CREATE only: a new unique schema.table normally lands in the single writable source its sche…

  Scenario: REQ-1000 default behaviour
    Given schema_shared maps to sources [snowflake_prod, snowflake_staging]
    When CREATE TABLE schema_shared.new_table AS SELECT ...
    Then the operation is rejected with "schema maps to multiple writable sources; use catalog to disambiguate"

    When CREATE TABLE snowflake_prod.schema_shared.new_table AS SELECT ...
    Then the operation succeeds, table created in snowflake_prod

    Given schema_unique maps to single writable source snowflake_only
    When CREATE TABLE schema_unique.new_table AS SELECT ...
    Then the operation succeeds without requiring catalog (unambiguous placement)
