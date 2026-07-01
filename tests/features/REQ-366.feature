# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-366 — Registration & Governance
  # Views require an approval workflow, OR the originator must already hold the rights to the underlying tables and to any j…

  Scenario: REQ-366 default behaviour
    Given a user attempting to create a view over tables they do not own
    When they submit the view creation
    Then an approval workflow is triggered unless the originator already holds rights to all underlying tables and joins
