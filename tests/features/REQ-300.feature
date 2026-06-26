# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-300 — GraphQL Variable Defaults
  # GraphQL operations may declare variable default values (e.g. `query Q($limit: Int = 10)`). The compiler MUST apply those…

  Scenario: REQ-300 default behaviour
    Given a GraphQL operation declaring $limit: Int = 10 and a request omitting the limit variable
    When the compiler processes the request
    Then it applies the default value 10 as if the caller had supplied it
