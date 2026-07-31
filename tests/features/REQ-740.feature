# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-740 — Security
  # Masking SELECT expressions only; predicates that remain in the query (JOIN ON, RLS-injected filters) use physical unmask…

  Scenario: REQ-740 default behaviour
    Given a masked column also referenced in WHERE or JOIN ON
    When masking is injected
    Then SELECT projects the masked expression; WHERE and JOIN ON reference the physical unmasked column
