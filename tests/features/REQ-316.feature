# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-316 — OpenAPI Auto-Registration Connector
  # On registration, Provisa parses the spec and auto-registers all GET operations as virtual query tables. Path parameters…

  Scenario: REQ-316 default behaviour
    Given an OpenAPI spec is registered
    When Provisa parses the spec
    Then all GET operations are auto-registered as virtual query tables with path/query params as GraphQL arguments
