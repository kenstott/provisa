# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-412 — Compiler & Schema
  # Naming convention option `graphql-default` provides camelCase field names, PascalCase types, and camelCase mutations (in…

  Scenario: REQ-412 default behaviour
    Given the default Provisa naming convention
    When the schema is generated
    Then field names are camelCase, types are PascalCase, and mutations are camelCase
