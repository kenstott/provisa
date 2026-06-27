# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-746 — Security
  # Capability enforcement via check_capability and has_capability functions — capabilities are independently assigned per r…

  Scenario: REQ-746 default behaviour
    Given a role with a specific capability (e.g., query_development)
    When check_capability is called for that capability
    Then no exception is raised; for missing capability, InsufficientRightsError is raised
