# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-125 — Authentication
  # Superuser bootstrap access — superuser credentials in config (username + password from env secret). Always admin role +…

  Scenario: REQ-125 default behaviour
    Given superuser credentials are set in config via env secret
    When the superuser authenticates
    Then they receive admin role and all capabilities regardless of the configured auth provider
