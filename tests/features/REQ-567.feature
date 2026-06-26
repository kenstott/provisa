# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-567 — Subscriptions
  # When a subscription field selects columns from joined tables via registered relationships, the subscription engine colle…

  Scenario: REQ-567 default behaviour
    Given a subscription that joins two tables via a registered relationship
    When a row in the joined table changes
    Then the subscription query re-fires and the updated result is streamed to the subscriber
