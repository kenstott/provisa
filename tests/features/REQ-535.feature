# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-535 — Authentication
  # When no auth provider is configured (dev mode), any request is treated as a role identity — the username IS the role, ta…

  Scenario: REQ-535 default behaviour
    Given no auth provider is configured
    When any request arrives
    Then it is treated as a role identity (admin by default) with all roles and wildcard domain access
