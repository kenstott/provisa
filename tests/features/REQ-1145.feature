# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-1145 — Windows Native Launcher
  # The launcher surfaces startup progress via breadcrumbs. first-launch-native.ps1 and provisa-native.ps1 write STATE|messa…

  Scenario: REQ-1145 default behaviour
    Given a startup sequence beginning
    When first-launch-native.ps1 enters the staging phase
    Then it writes "STAGING|Extracting runtime..." to .startup-status
    And as each phase completes (CONFIG, DEMO, START, WAIT), a new breadcrumb is written

    Given a failed breadcrumb write (e.g. permission denied on .startup-status)
    When the launcher attempts to write
    Then the write failure is silently caught (best-effort)
    And startup continues without interruption

    Given an ERROR state
    When a fatal condition is detected (e.g. runtime staging failed)
    Then "ERROR|<message>" is written to .startup-status
    And the launcher halts gracefully
    And the monitor reads the ERROR and displays it to the user
