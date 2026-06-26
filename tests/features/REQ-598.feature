# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-598 — GraphQL Remote Schema Connector (REQ-307–313)
  # Remote schema source registrations (GraphQL, gRPC, OpenAPI) accept a `relationships` list that stores FK/PK join paths a…

  Scenario: REQ-598 default behaviour
    Given a remote schema source with both manually declared and auto-detected relationships
    When a schema refresh is triggered
    Then auto-detected relationships are re-run and may change; manually declared relationships are preserved unchanged
