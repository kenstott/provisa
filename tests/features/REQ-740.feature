# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-740 — Security
  # Masking SELECT expressions only; WHERE, JOIN ON, and other predicates use physical unmasked columns unchanged. Masking i…

  Scenario: REQ-740 default behaviour
    Given a masked column also referenced in WHERE or JOIN ON
    When masking is injected
    Then SELECT projects the masked expression; WHERE and JOIN ON reference the physical unmasked column
