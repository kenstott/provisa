# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-721 — Splunk Connector
  # Splunk is registered as a source type and exposed via Apache Calcite connector (kenstott/calcite), enabling Splunk searc…

  Scenario: REQ-721 default behaviour
    Given a Splunk instance with admin credentials
    When a user registers the Splunk source with host, port, and auth token
    Then the source is added to the catalog and Splunk's search tables (internal_server)
    are enumerable
