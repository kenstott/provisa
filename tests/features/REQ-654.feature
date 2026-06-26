# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-654 — Compiler & Schema
  # `{table}_group_by` root query field. Requires `by: [SelectColumn!]!`; accepts same args as the base table query (`where`…

  Scenario: REQ-654 default behaviour
    Given a registered table with numeric columns
    When a _group_by query is submitted with by: [category] and aggregate count
    Then the response contains one GroupByRow per distinct category value
    And each row includes groupKey and aggregates fields
