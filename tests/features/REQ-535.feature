# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-535 — Authentication
  # When no auth provider is configured (dev mode), any request is treated as the anonymous dev principal (user_id `anonymou…

  Scenario: REQ-535 default behaviour
    Given no auth provider is configured
    When any request arrives
    Then it is treated as the anonymous dev principal with role org_admin by default, all roles, and wildcard domain access
