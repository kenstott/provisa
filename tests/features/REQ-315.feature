# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-315 — OpenAPI Auto-Registration Connector
  # If auto-discovery of a spec is not possible (behind auth, no spec endpoint, hand-written API), the steward may manually…

  Scenario: REQ-315 default behaviour
    Given an API with no public spec endpoint
    When a steward manually uploads a YAML/JSON OpenAPI 3.x spec
    Then it is stored locally and treated identically to a fetched spec
