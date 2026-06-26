# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-205 — Tracked Functions & Custom Mutations
  # Database functions (stored procedures, UDFs) registered in Provisa config and exposed as GraphQL mutations or queries. V…

  Scenario: REQ-205 default behaviour
    Given a VOLATILE database function registered in Provisa config
    When the schema is generated
    Then it is exposed as a GraphQL mutation field
