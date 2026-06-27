# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-745 — Security
  # Role-based masking — different roles can have different masks for the same column; admin roles with no masking rules see…

  Scenario: REQ-745 default behaviour
    Given the same column with different masking rules per role
    When inject_masking is called for two different roles
    Then admin sees raw values; analyst sees regex mask; masked_viewer sees constant mask
