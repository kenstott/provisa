# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-215 — Hasura v2 Parity: Low-Complexity Features
  # Inherited roles -- role hierarchy where child role inherits capabilities and domain_access from parent. Config: `parent_…

  Scenario: REQ-215 default behaviour
    Given roles configured with parent_role_id forming a hierarchy
    When the system starts up
    Then capabilities and domain_access are flattened up the chain so lookups remain O(1)
