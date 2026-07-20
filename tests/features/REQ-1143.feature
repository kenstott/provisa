# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-1143 — Caching
  # The table detail view shows a derived, human-readable refresh_policy_summary that expresses the effective refresh/servin…

  Scenario: REQ-1143 default behaviour
    Given a steward viewing a table detail
    When the table detail renders at the top of the edit form
    Then a refresh_policy_summary is displayed, derived from federate(source, engine) resolution plus freshness_gate plus REQ-1141 gates
    And the summary shows one of: "served live" (VIRTUAL/SCAN reached directly), "refreshed [cadence] [window], only when changed; queries never touch the source" (load-protected snapshot), or "refreshed on access when older than [ttl]" (lazy cache)
    And misconfiguration warnings appear for: prefer_materialized with no gates/cache (reachable source → "inert on this engine — served live" | unreachable source → "frozen snapshot — loaded once, never refreshes")
    And the summary is recomputed per (source, engine) pair, showing different prose if reachability changes across engines
