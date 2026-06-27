# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-736 — File & Lake Sources
  # File source adapter supports SQLite, CSV, and Parquet formats. SQLite uses native type mapping (INTEGER→BIGINT, REAL→DOU…

  Scenario: REQ-736 default behaviour
    Given a SQLite database with multiple tables
    When the adapter discovers schema and executes queries
    Then column types are mapped correctly and results are returned as row dicts
