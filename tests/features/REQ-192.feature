# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-192 — Hasura Migration Converters
  # [SUPERSEDED by REQ-417, 2026-10-17 -- the v2 converter converts event triggers, cron triggers, actions and remote schema…

  Scenario: REQ-192 default behaviour
    Given a Hasura v2 project with event triggers, a cron trigger, an action and a section with no Provisa counterpart
    When the converter runs
    Then each feature with a counterpart is converted and reported, what has none is warned about by name, and conversion completes
