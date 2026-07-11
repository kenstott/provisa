Feature: NRT materialized-view lifecycle (MVP)
  The zero-config happy path of an always-on, real-time MV (REQ-966): event-driven,
  recompute-to-current, replace-persist, demand-emit — crash-safe (REQ-960),
  failover-safe (REQ-959), and debounced under churn (REQ-963). An MV is declared as
  SQL and must be deterministic (REQ-964).

  Scenario: A non-deterministic MV is rejected at registration
    Given a materialized view whose SQL calls now()
    When the view is registered
    Then registration is rejected as non-deterministic
    And the view is not added to the MV registry

  Scenario: A deterministic MV registers and derives its lineage
    Given a materialized view "SELECT region, sum(amt) AS t FROM orders GROUP BY region"
    When the view is registered
    Then the view is added to the MV registry

  Scenario: A burst of upstream changes collapses into one recompute
    Given a live MV with a debounce quiet window of 100 seconds
    And three upstream changes have fanned in
    When the loop ticks before the quiet window elapses
    Then the MV does not recompute and the changes stay pending
    When the debounce deadline has passed
    Then the MV recomputes exactly once over all three changes

  Scenario: A crash between land and commit loses no downstream ripple
    Given a source node whose fan-out crashes after landing
    When the node processes its pending work and crashes
    Then the landed data is preserved but no downstream ripple is committed
    And the claim is still owned by the processor
    When the processor re-runs
    Then the land is idempotent and the downstream ripple commits exactly once

  Scenario: A peer takeover blocks the stale owner's completion
    Given a claim owned by one processor that a peer reclaims and takes over
    When the stale owner tries to complete the claim
    Then its ownership CAS fails
    And the new owner can complete the claim
