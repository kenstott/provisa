# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-198 — Aggregates
  # Aggregate MV routing — when a query requests aggregates over a pattern already materialized in an MV, the compiler rewri…

  Scenario: REQ-198 default behaviour
    Given an aggregate query whose pattern matches a materialized view
    When the compiler processes the query
    Then it rewrites the query to use the MV instead of the base table
