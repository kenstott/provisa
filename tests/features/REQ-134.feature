# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-134 — Views (Governed Computed Datasets)
  # Views go through the same governance pipeline as tables — RLS, masking, sampling, role-based schema visibility, approval…

  Scenario: REQ-134 default behaviour
    Given a registered view with RLS rules and masking applied
    When a consumer queries the view
    Then RLS, masking, sampling, and role-based visibility are enforced identically to a table
