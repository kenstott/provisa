# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-308 — GraphQL Remote Schema Connector (REQ-307–313)
  # Each Query field on the remote schema is exposed as a virtual read-only table in Provisa. Column names and types are inf…

  Scenario: REQ-308 default behaviour
    Given a remote GraphQL schema is registered in Provisa
    When introspection completes
    Then Query fields are auto-registered as virtual read-only tables and Mutation fields as tracked functions
