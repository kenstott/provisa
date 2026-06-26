# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-006 — Query Governance
  # Large-result redirect and Arrow output are available to any query the user's rights permit, subject to configured thresh…

  Scenario: REQ-006 default behaviour
    Given a user with rights to query a table
    When the query result exceeds the configured large-result threshold
    Then large-result redirect and Arrow output are available
