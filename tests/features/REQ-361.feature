# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-361 — Tracked Functions & Custom Mutations
  # Action query fields returning a known table type must resolve nested relationship fields by batching lookups against the…

  Scenario: REQ-361 default behaviour
    Given an action query field returning a registered table type with nested relationship fields
    When the results are resolved
    Then related rows are fetched via batched lookups with RLS and masking applied
