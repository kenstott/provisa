# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-723 — Splunk Connector
  # Splunk authentication: when use_token=true (default), the password field is passed as token; when use_token=false, usern…

  Scenario: REQ-723 default behaviour
    Given a source with use_token=true and a password field
    When the connector properties are built
    Then props contains token=<password> and no user/password keys
