# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-326 — gRPC Remote Schema Connector (REQ-322–329)
  # Each mutation-classified gRPC method is exposed as a tracked function (mutation). Input message fields become GraphQL mu…

  Scenario: REQ-326 default behaviour
    Given a mutation-classified gRPC method
    When it is exposed as a tracked function
    Then input message fields become GraphQL mutation input arguments and the output schema becomes return_schema
