# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-1072 — Data Catalog Integration
  # Sync mechanism: metadata changes push to the external catalog event-driven via the REQ-942 event substrate. A scheduled…

  Scenario: REQ-1072 default behaviour
    Given an org whose metadata export target is configured and entitled
    When the governed model changes
    Then a publish is queued for that org and no other
    And one drain publishes the whole pending change set as a single snapshot
    And a publish the catalog rejected stays claimable instead of being completed
    And the org's scheduled reconcile republishes the full snapshot on its own cron
