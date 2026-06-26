# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-325 — gRPC Remote Schema Connector (REQ-322–329)
  # Each query-classified gRPC method is exposed as a virtual read-only table. Output message fields become columns using th…

  Scenario: REQ-325 default behaviour
    Given a query-classified gRPC method with input and output message fields
    When it is exposed as a virtual table
    Then output fields become columns, input fields become GraphQL arguments, and streaming methods collect all messages
