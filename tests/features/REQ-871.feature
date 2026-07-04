# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-871 — Authorization
  # Mutation↔table association suggestions at registration time via protocol-specific association-suggesters. Universal rule…

  Scenario: REQ-871 default behaviour
    Given a remote GraphQL source with a query "users" returning [User] (so type_to_table maps User → the users table) and a mutation "createUser(input: UserInput): User"
    When the schema is registered and mapped
    Then the createUser tracked-function entry carries suggested_associations whose top candidate is the users table (score 1.0, reason "return type User"), and its writable_by stays empty — the suggestion is a hint that no code auto-binds
    And a mutation "sendTelemetry: Boolean" with no table-typed return yields an empty suggested_associations list rather than an error
