# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-621 — Hasura Migration Converters
  # Both Hasura v2 and DDN converters emit placeholder connection credentials in the output config (host: localhost, passwor…

  Scenario: REQ-621 default behaviour
    Given a completed Hasura v2 or DDN conversion
    When the output config is inspected
    Then placeholder credentials (host: localhost, password: ${env:DB_PASSWORD}) are present and Provisa refuses to start without real values
