# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-728 — SharePoint Connector
  # SharePoint connector builds connection properties from source base_url/host (site-url), username (client-id), password (…

  Scenario: REQ-728 default behaviour
    Given a SharePoint source with base_url="https://kenstott.sharepoint.com",
    username="client-id-value", password="secret", database="tenant-uuid"
    When catalog properties are built
    Then props contains site-url, auth-type, client-id, client-secret, tenant-id
    And certificate_path/certificate_password are included when present in mapping
