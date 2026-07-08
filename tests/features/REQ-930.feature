# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-930 — Materialization
  # Materialization is derived from federation-engine reachability, not a user knob. A source the engine cannot reach live i…

  Scenario: REQ-930 default behaviour
    Given a source that the federation engine cannot reach live
    When the source is first added
    Then it is materialized/landed unconditionally
    And subsequent refreshes follow the change signal (ttl/probe/push)
    Given a source that the engine can reach live
    When queries reference it
    Then it is queried live without materialization
    And optional perf-caching may be applied via TTL
