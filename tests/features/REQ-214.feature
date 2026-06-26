# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-214 — Hasura v2 Parity: Low-Complexity Features
  # Column presets -- auto-set column values on insert/update from session variables or built-in functions. Config per table…

  Scenario: REQ-214 default behaviour
    Given a table config with column_presets for created_by and updated_at
    When an insert or update mutation is executed
    Then preset columns are removed from user input and injected with session variable or built-in function values before SQL generation
