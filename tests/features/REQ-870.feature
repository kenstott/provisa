# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-870 — Authorization
  # Admin-only reclassification: legitimate reclassification of a mutation to read-safe is gated by ACCESS_CONFIG capability…

  Scenario: REQ-870 default behaviour
    Given a discovered remote mutation "createOrder" registered with an empty writable_by
    And an admin grants it to the "ops" role (writable_by = ['ops'])
    When introspection re-runs and upserts createOrder by name with an empty writable_by
    Then the ops grant is preserved (writable_by stays ['ops']) — discovery never wipes grants
    And when a role WITHOUT the ACCESS_CONFIG capability attempts to reclassify createOrder to read-safe, the attempt is rejected
    And an ACCESS_CONFIG (or admin) role may demote it to read, but no one may promote a read back to a write
