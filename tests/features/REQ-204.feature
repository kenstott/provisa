# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-204 — ABAC Approval Hook
  # Approval hook scoping — per-table (`approval_hook: true`), per-source (`approval_hook: true` on source config), or globa…

  Scenario: REQ-204 default behaviour
    Given a query referencing only tables without approval_hook enabled
    When the compiler evaluates the query
    Then the approval hook call is skipped entirely with zero overhead
