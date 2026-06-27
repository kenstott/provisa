# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-727 — SharePoint Connector
  # SharePoint connector supports two authentication methods: CLIENT_CREDENTIALS (default) and certificate-based authenticat…

  Scenario: REQ-727 default behaviour
    Given a SharePoint source with auth-type CLIENT_CREDENTIALS
    When queries are executed
    Then Provisa sends client-id and client-secret to the Calcite connector
    And when auth-type is CERTIFICATE
    Then Provisa sends certificate-path and certificate-password instead
