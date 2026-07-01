# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-636 — Registration & Governance
  # When a Trino connector is configured for a source type (i.e., the type is in SOURCE_TO_CONNECTOR), Trino is the preferre…

  Scenario: REQ-636 default behaviour
    Given a source type with a Trino connector configured
    When schema or table introspection is triggered
    Then Trino is used as the introspection path; native driver is used only when no connector exists
