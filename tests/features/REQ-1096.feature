# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-1096 — Admin & Configuration
  # Live config export/diff/patch — an OPT-IN admin capability, distinct from the plain download/upload of REQ-164, that let…

  Scenario: REQ-1096 default behaviour
    Given demo mode is active
    Then live_config_export is forced on and the admin UI shows View Diff / Download Patch / Apply Revised
    Given live_config_export is enabled
    When Provisa finishes booting (after FK tracking and graphql-remote registration)
    Then the generated config is captured once as the boot-snapshot baseline
    When an admin changes a table's primary key (or creates an MV) in the UI
    And the admin opens View Diff
    Then the diff shows only that change against the boot snapshot, both sides normalized identically
    And Download Patch yields a git-apply-compatible unified diff from baseline to the curated current
    And Apply Revised uploads the normalized config and reloads
    Given live_config_export is disabled and demo mode is inactive
    Then the View Diff / patch controls are absent and an uploaded config is written verbatim
