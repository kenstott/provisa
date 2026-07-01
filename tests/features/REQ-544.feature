# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-544 — Cache
  # Cache TTL resolves in order: table-level TTL > source-level TTL > global default TTL. Setting `cache_enabled: false` on…

  Scenario: REQ-544 default behaviour
    Given cache TTL configured at table, source, and global levels
    When a cache key is resolved
    Then table-level TTL takes precedence, and cache keys include role_id and RLS context for security partitioning
