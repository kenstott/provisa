# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-231 — Hot Tables (Redis-Cached Lookups)
  # Hot table refresh follows MV pattern: TTL-based via `refresh_interval` (default: `materialized_views.default_ttl`), back…

  Scenario: REQ-231 default behaviour
    Given a hot table with a configured refresh_interval
    When the TTL expires or a mutation occurs on the source table
    Then the cache is invalidated and asynchronously reloaded, falling back to live query if stale
