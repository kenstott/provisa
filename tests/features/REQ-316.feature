# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-316 — OpenAPI Auto-Registration Connector
  # OpenAPI GET operations as tables. [SUPERSEDED by the 2026-10-02 REGISTRATION IS CURATION amendment below -- a GET operat…

  Scenario: REQ-316 default behaviour
    Given an OpenAPI source has been added
    When the steward opens the Register Table picker for it
    Then every GET operation of the spec is listed as a table and none is registered
    And a table the steward registers has path/query params as GraphQL arguments
