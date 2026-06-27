# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-722 — Splunk Connector
  # Splunk connector accepts a URL (from base_url or constructed via https://host:port, default port 8089) to connect to the…

  Scenario: REQ-722 default behaviour
    Given Splunk host=splunk and port=8089
    When the source is registered
    Then the connector receives url=https://splunk:8089
