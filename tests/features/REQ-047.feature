# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-047 — Output & Delivery
  # JSON output preserves native GraphQL nested structure.

  Scenario: REQ-047 default behaviour
    Given a GraphQL query returning nested relationships
    When the result is delivered as JSON
    Then the nested structure mirrors the GraphQL response shape with relationships intact
