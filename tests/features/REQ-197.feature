# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-197 — Aggregates
  # Per-role aggregate gating via `allow_aggregations` (matching v2) or per-table `aggregates` config section for explicit o…

  Scenario: REQ-197 default behaviour
    Given a role without allow_aggregations permission
    When the schema is generated
    Then aggregate root fields are not exposed to that role
