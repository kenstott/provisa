# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-208 — Tracked Functions & Custom Mutations
  # Functions execute via direct DB connection (same as mutations). Never routed through Trino.

  Scenario: REQ-208 default behaviour
    Given a tracked database function
    When it is executed via GraphQL
    Then it runs via a direct DB connection and is never routed through Trino
