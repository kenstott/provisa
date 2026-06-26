# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-411 — Compiler & Schema
  # Naming convention option `hasura-default` provides snake_case mutation names (insert_orders, update_orders, delete_order…

  Scenario: REQ-411 default behaviour
    Given naming convention is set to hasura-default
    When the schema is generated
    Then mutation names and field names use snake_case matching Hasura V2 defaults
