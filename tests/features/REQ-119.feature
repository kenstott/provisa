# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-119 — JSONB & API Sources
  # Stewards can promote specific nested fields from JSONB columns into native PostgreSQL generated columns (GENERATED ALWAY…

  Scenario: REQ-119 default behaviour
    Given a JSONB column with nested fields
    When a steward promotes a nested field via dot-path
    Then a PostgreSQL generated column is created that is filterable, indexable, and
    relationship-eligible
