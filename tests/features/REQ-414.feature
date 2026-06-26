# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-414 — Registration & Governance
  # Demo/install example schema must include at least one FK relationship to exercise auto-generated relationship discovery.

  Scenario: REQ-414 default behaviour
    Given the demo installation schema
    When relationship discovery runs
    Then at least one FK relationship is auto-discovered and exercised
