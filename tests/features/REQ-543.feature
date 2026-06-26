# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-543 — Auto-Materialized Relationships
  # Mutations to source tables of a materialized cross-source relationship mark the corresponding MV as stale for re-refresh…

  Scenario: REQ-543 default behaviour
    Given a materialized cross-source relationship MV
    When a mutation is applied to one of its source tables
    Then the MV is marked stale and scheduled for re-refresh within the refresh_interval
