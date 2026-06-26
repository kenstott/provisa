# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-542 — Naming & Schema
  # Regex naming rules in config are applied to table names in order when generating GraphQL field names, before uniqueness…

  Scenario: REQ-542 default behaviour
    Given a config with ordered regex naming rules
    When GraphQL field names are generated for table names
    Then each rule is applied in order before uniqueness resolution
