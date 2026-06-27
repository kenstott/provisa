# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-726 — SharePoint Connector
  # SharePoint is registered as a source type in the Provisa data connector registry and can be configured as a queryable da…

  Scenario: REQ-726 default behaviour
    Given a user creates a new source
    When they select type "sharepoint"
    Then the source is created and can be queried via Trino using the sharepoint connector
