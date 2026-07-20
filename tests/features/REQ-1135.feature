# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-1135 — Licensing
  # The installation records a tamper-evident first-use timestamp that survives reinstall. On first startup with no existing…

  Scenario: REQ-1135 default behaviour
    Given a fresh installation with no existing anchor
    When the application first starts
    Then first_seen is set to today and written as a signed anchor to all anchor locations
    Given an existing installation whose app directory is removed and reinstalled
    When the application starts again
    Then the earliest surviving anchor's first_seen is restored and the trial clock is not reset
