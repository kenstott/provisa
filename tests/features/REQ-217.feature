# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-217 — Hasura v2 Parity: Low-Complexity Features
  # Batch mutations already supported by GraphQL spec -- multiple mutations in one request execute sequentially. Document ex…

  Scenario: REQ-217 default behaviour
    Given a GraphQL request containing multiple mutations
    When the request is executed
    Then mutations execute sequentially per the GraphQL spec
