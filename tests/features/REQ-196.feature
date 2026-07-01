# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-196 — Aggregates
  # Auto-generated aggregate queries following Hasura v2 pattern. Every table gets a `<table>_aggregate` root field. Numeric…

  Scenario: REQ-196 default behaviour
    Given a registered table with numeric and comparable columns
    When the schema compiler runs
    Then a <table>_aggregate root field is generated with sum/avg/stddev/variance on numeric columns, min/max on comparable columns, and count on all columns
