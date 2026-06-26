# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-408 — API & Integration
  # OpenAPI operations can carry `x-provisa-kind: query` or `x-provisa-kind: mutation` extension to override the GET-heurist…

  Scenario: REQ-408 default behaviour
    Given an OpenAPI operation with x-provisa-kind: query on a POST endpoint
    When the mapper processes the spec
    Then the POST operation is exposed as a GraphQL query instead of a mutation
