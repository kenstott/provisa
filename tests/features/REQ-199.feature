# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-199 — Aggregates
  # View auto-materialization for aggregate optimization — expensive views auto-materialized and registered in aggregate cat…

  Scenario: REQ-199 default behaviour
    Given an expensive view eligible for auto-materialization
    When the background loop refreshes it and a query targets that view
    Then the query uses the MV; if the MV is stale, it falls back to live execution
