# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-845 — Materialization Store
  # The set of sources served by reactive replicas is engine-relative — reactive = { source : reach(source) == land } for th…

  Scenario: REQ-845 default behaviour
    Given a source with no attach connector for the active engine
    When it is referenced concurrently after TTL expiry
    Then a single coalesced pull lands its rows into materialization_store and the queries are rewritten to read the cached rows.
