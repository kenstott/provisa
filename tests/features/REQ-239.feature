# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-239 — Warm Tables (Replica Profile — Low-Latency Placement)
  # Warm table auto-promotion -- [SUPERSEDED by REQ-239 amendment, 2026-10-03 -- promotion is Hot replication (REQ-826): cou…

  Scenario: REQ-239 default behaviour
    Given a table left at Default whose statement count within replication.hot_interval reaches replication.hot_threshold
    When the promotion check runs
    Then the table is marked promoted and a replica build is requested from the data replicator
    And reads stay live until that build completes in the engine's store
    And the table is demoted when its count falls below half the threshold
