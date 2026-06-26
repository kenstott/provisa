# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-237 — Hot Table Auto-Detection
  # Auto-hot tables can be opted out via `hot: false` on the table config. Explicit `hot: true` overrides the auto-detection…

  Scenario: REQ-237 default behaviour
    Given a table with hot: false in its config
    When schema is rebuilt
    Then the table is not cached in Redis even if it meets auto-detection criteria
