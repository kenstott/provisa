# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-601 — OpenAPI Auto-Registration Connector
  # OpenAPI virtual table names are derived from the operation's `operationId`. If no `operationId` is defined, Provisa slug…

  Scenario: REQ-601 default behaviour
    Given an OpenAPI spec with operationId "findPetsByStatus"
    When the spec is registered
    Then the virtual table alias is "pet_by_status" used as the consumer-facing GraphQL name
