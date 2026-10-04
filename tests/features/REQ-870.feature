# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-870 — Authorization
  # Admin-only reclassification: legitimate reclassification of a mutation to read-safe is gated by ACCESS_CONFIG capability…

  Scenario: REQ-870 default behaviour
    Given a discovered mutation "createOrder" in domain "sales", assigned on discovery to org_admin alone
    And an admin assigns it to the "ops" role
    When introspection re-runs and registers createOrder again
    Then ops still reaches createOrder through its assignment and the sales domain — discovery never drops an assignment
    And when a role WITHOUT the ACCESS_CONFIG capability attempts to reclassify createOrder to read-safe, the attempt is rejected
    And an ACCESS_CONFIG (or admin) role may demote it to read, but no one may promote a read back to a write
