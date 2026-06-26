# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-043 — API & Integration
  # GraphQL endpoint is primary entry point for queries and mutations.

  Scenario: REQ-043 default behaviour
    Given a consumer with valid credentials
    When they submit a query or mutation to the GraphQL endpoint
    Then the request is processed and a typed response is returned
