# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-870 — Authorization
  # Admin-only reclassification: legitimate reclassification of a mutation to read-safe is gated by ACCESS_CONFIG capability…

  Scenario: REQ-870 default behaviour
    Given a mutation "createOrder" in domain "sales" that discovery offers and has not registered
    And a steward registers it, assigned to the "ops" role
    Then ops reaches createOrder through its assignment and the sales domain
    And a role assigned it outside the sales domain, or reaching sales without the assignment, does not
    And when a role WITHOUT the ACCESS_CONFIG capability attempts to reclassify createOrder to read-safe, the attempt is rejected
    And an ACCESS_CONFIG (or admin) role may demote it to read, but no one may promote a read back to a write
