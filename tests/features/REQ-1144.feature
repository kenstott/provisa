# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-1144 — Windows Native Launcher
  # The Windows native launcher displays a visible WinForms splash screen (startup monitor) during first launch instead of r…

  Scenario: REQ-1144 default behaviour
    Given a user running the Windows native launcher for the first time
    When the launcher starts
    Then a WinForms splash screen appears immediately
    And the monitor process launches first-launch-native.ps1 hidden in the background
    And the splash reads .startup-status breadcrumbs in real-time
    And when /health becomes ready, the monitor warms the UI endpoints
    And once ready, the monitor opens the browser and closes

    Given a startup failure (e.g. staging error) written as ERROR to .startup-status
    When the monitor reads the ERROR breadcrumb
    Then the splash displays the error message and remains open
    And the user sees the exact failure instead of "nothing happened"
