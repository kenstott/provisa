# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-1137 — Licensing
  # After the trial expires and no valid license is present, a self-contained nag message is surfaced on every access surfac…

  Scenario: REQ-1137 default behaviour
    Given an expired trial with no valid license
    When a client connects over pgwire
    Then a single NoticeResponse carrying the license nag is sent and the query results are unaffected
    Given the same state
    When the UI loads any page
    Then a persistent license banner is shown in the shell chrome containing the machine ID, both contact routes, and the required fields
