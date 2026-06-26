# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-160 — Auto-Materialized Relationships
  # Auto-MVs start STALE and are populated by the background refresh loop.

  Scenario: REQ-160 default behaviour
    Given an auto-generated MV created at startup
    When it is first created
    Then its state is STALE and the background refresh loop populates it before it serves queries
