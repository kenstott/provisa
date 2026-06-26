# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-321 — OpenAPI Auto-Registration Connector
  # Spec refresh is triggered on demand via an admin mutation. On refresh, existing virtual table and tracked function regis…

  Scenario: REQ-321 default behaviour
    Given an OpenAPI spec that has been updated upstream
    When a steward triggers the spec refresh admin mutation
    Then registrations are updated and governance rules applied on top are preserved
