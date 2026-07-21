# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-1172 — Compiler & Schema
  # Registered callable field names (tracked functions and webhooks) in GraphQL schemas now apply the active naming conventi…

  Scenario: REQ-1172 default behaviour
    Given a Provisa deployment with apollo_graphql (camelCase) naming convention active
    And a tracked function registered with name "add_pet"
    When the GraphQL schema is generated
    Then the callable field appears as "addPet" not "add_pet"
    And the reverse-lookup alias key routes "addPet" back to the "add_pet" command
