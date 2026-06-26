# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-290 — Automatic Persisted Queries (APQ)
  # APQ applies to any query the authenticated caller's rights permit. APQ is fully automatic — any successfully executed qu…

  Scenario: REQ-290 default behaviour
    Given an authenticated caller executing any permitted query
    When the query succeeds
    Then it is automatically registered in the APQ cache and reusable by hash with no steward action
