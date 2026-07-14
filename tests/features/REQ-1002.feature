# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-1002 — Type Coercion
  # Cross-engine result-schema → target DDL type coercion policy: the SELECT result schema must map to valid target-dialect…

  Scenario: REQ-1002 default behaviour
    Given SELECT returns ARRAY<STRUCT<name STRING, value INT>> from Snowflake
    When creating target table in BigQuery
    Then complex type is coerced to BigQuery-native ARRAY<STRUCT<name STRING, value INT64>>

    Given SELECT returns DECIMAL(38,10) from source engine
    When creating target table with different DECIMAL scale limits
    Then type is coerced to target's supported DECIMAL range
