# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-215 — Hasura v2 Parity: Low-Complexity Features
  # Inherited roles: a role hierarchy where [SUPERSEDED by REQ-2155, 2026-10-25 -- a role inherits capabilities; its domain…

  Scenario: REQ-215 default behaviour
    Given roles configured with parent_role_id forming a hierarchy
    When the system starts up
    Then capabilities and domain_access are flattened up the chain so lookups remain O(1)
