# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-015 — Registration & Governance
  # There is no per-table governance mode. Every table and view is queryable directly under the user's rights (table/view ri…

  Scenario: REQ-015 default behaviour
    Given any registered table or view
    When a user with the appropriate rights queries it
    Then Stage 2 governance is applied uniformly without any per-table mode distinctions
