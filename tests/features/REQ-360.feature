# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-360 — Tracked Functions & Custom Mutations
  # Action query fields (tracked functions and webhooks with `exposed_as: query`) must support standard filter/sort/paginati…

  Scenario: REQ-360 default behaviour
    Given an action query field with where, order_by, limit, and offset arguments
    When the function executes and results are materialized
    Then filter, sort, and pagination are applied as Python post-processing
