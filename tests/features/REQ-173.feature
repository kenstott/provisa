# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-173 — Dataset Change Events
  # Change events fire on the same mutation hook that invalidates cache and marks MVs stale.

  Scenario: REQ-173 default behaviour
    Given a mutation hook fires after a data change
    When the hook executes
    Then a change event is emitted and cache is invalidated and MVs are marked stale in the same hook
