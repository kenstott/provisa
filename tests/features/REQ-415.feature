# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-415 — Registration & Governance
  # The `hasura_v2_relationship_style` option controls whether FK-derived relationships use Hasura V2's naming conventions —…

  Scenario: REQ-415 default behaviour
    Given hasura_v2_relationship_style enabled
    When FK-derived relationships are named
    Then many-to-one names are singular and one-to-many names are plural via inflection
