# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-1141 — Caching
  # A steward can mark an operational database source as load-sensitive and configure it for scheduled-refresh-only material…

  Scenario: REQ-1141 default behaviour
    Given a load_protected_policy: scheduled_refresh source with window-only config (off-peak: 22:00-02:00 UTC, no cadence, no probe)
    When the off-peak window opens at 22:00
    Then the scheduler fires a refresh attempt (window-open edge trigger)
    And then idles until 02:00 when the window closes and opens again the next night

    Given the same source reconfigured with cadence-only (refresh_interval: 1h, no window, no probe)
    When the scheduler ticks every 1h at any time
    Then each tick drives a refresh attempt, regardless of local time

    Given the source with probe-only (no window, no cadence)
    When the scheduler continuously evaluates (e.g. on demand or via a background loop)
    And the probe reports unchanged (token == stored_token)
    Then the scheduler resets its clock without pulling, zero cost
    And when the probe reports changed (token != stored_token), a pull is triggered

    Given the source with window+cadence (window: 22:00-02:00, refresh_interval: 30m, no probe)
    When a cadence tick fires
    Then pull only if the tick landed inside the window

    Given the source with window+probe (window: 22:00-02:00, no cadence)
    When the off-peak window opens
    Then call the probe; pull only if it reports changed

    Given the source with all three (window, cadence, probe)
    When a cadence tick fires inside the window
    Then call the probe; pull only if changed

    When a query arrives at any time
    Then the query path serves the last materialized snapshot with FreshnessMode.SCHEDULED (always fresh=true)
    And the source never sees query-path traffic

    Given a source configured prefer_materialized: true, cache_ttl: 3600, with NO gates (no window, no cadence, no probe)
    Then this source is NOT a REQ-1141 scheduled refresh; it is REQ-826 lazy read-through
    And the query path triggers re-pulls on staleness (query-driven, not scheduler-driven)

    Given a source configured prefer_materialized: true, with NO gates AND NO cache_ttl
    Then MATERIALIZATION REQUIRES A REFRESH POLICY, so behavior splits on reachability:

    If the engine can ATTACH/VIRTUAL the source live (reachable):
    Then prefer_materialized is inert and the source degrades to live/VIRTUAL serving
    And no refresh policy = no scheduler discipline = live query-path access

    If the source is unreachable (_MATERIALIZE_ONLY type: APIs, NoSQL, streams, no live option):
    Then it is a load-once-then-frozen snapshot
    And it is only meaningful for static reference data or REQ-847 mutation-driven invalidation
