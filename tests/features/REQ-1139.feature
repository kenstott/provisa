# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-1139 — Licensing
  # A license file is applied via `provisa license apply <file>`, a UI upload in Settings → License, or by placing it at ~/.…

  Scenario: REQ-1139 default behaviour
    Given an expired trial and a license file whose signature is valid and machine_id matches this installation
    When the user runs `provisa license apply <file>`
    Then the license is accepted and the nag no longer appears on any surface
    Given a license file whose machine_id does not match this installation
    When the user applies it
    Then it is rejected and the nag continues
