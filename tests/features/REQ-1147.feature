# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-1147 — Windows Native Launcher
  # When the startup monitor drives the launch (env PROVISA_STARTUP_UI=1), provisa-native.ps1 must NOT open the browser itse…

  Scenario: REQ-1147 default behaviour
    Given env PROVISA_STARTUP_UI=1 (monitor is driving launch)
    When provisa-native.ps1 reaches the point where it normally opens the browser
    Then it skips the browser open (checks the env var first)
    And only waits for the monitor's signal or /health readiness
    And the monitor, after confirming /health and warming UI endpoints, opens the browser once

    Given env PROVISA_STARTUP_UI not set (standalone native script launch)
    When provisa-native.ps1 reaches readiness
    Then it opens the browser directly (legacy behavior)
    And the user sees the application immediately
