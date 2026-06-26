# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-157 — Naming & Schema
  # Order-by enum values preserve original column case (not uppercased).

  Scenario: REQ-157 default behaviour
    Given a column with mixed-case name
    When order-by enum values are generated
    Then the original case is preserved without uppercasing
