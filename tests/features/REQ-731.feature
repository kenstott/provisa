# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-731 — SharePoint Connector
  # SharePoint lists are enumerated as schemas and exposed as queryable tables via Trino, allowing users to discover availab…

  Scenario: REQ-731 default behaviour
    Given a SharePoint source is added in the Provisa UI
    When a user navigates to add a table and selects this source
    Then available SharePoint lists (e.g., "calendar", "events") appear in the table dropdown
