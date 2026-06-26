# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-158 — Auto-Materialized Relationships
  # Cross-source relationships with `materialize: true` auto-generate MV definitions at startup.

  Scenario: REQ-158 default behaviour
    Given a cross-source relationship configured with materialize: true
    When the platform starts up
    Then MV definitions are auto-generated for that relationship
