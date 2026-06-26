# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-003 — Query Governance
  # All queries and mutations are governed by user rights alone — table/view rights plus relationship rights. No registry me…

  Scenario: REQ-003 default behaviour
    Given a user with table/view rights
    When the user submits a query or mutation
    Then it is executed based solely on their rights without requiring registry membership or approval
