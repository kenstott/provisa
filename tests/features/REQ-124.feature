# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-124 — Authentication
  # Simple username/password auth for testing — users defined in config YAML with bcrypt hashed passwords. Issues short-live…

  Scenario: REQ-124 default behaviour
    Given allow_simple_auth is true and users are defined in config YAML with bcrypt passwords
    When a developer submits valid credentials
    Then a short-lived JWT is issued for local testing
