# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-192 — Hasura Migration Converters
  # Converters emit warnings for unmappable features (event_triggers, remote_schemas, cron_triggers, BooleanExpressionType)…

  Scenario: REQ-192 default behaviour
    Given a Hasura project with event_triggers, remote_schemas, cron_triggers, or webhook-backed actions
    When the converter runs
    Then warnings are emitted for unmappable features and conversion completes rather than aborting
