# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-230 — Hot Tables (Redis-Cached Lookups)
  # Hot tables stored as a single JSON blob in Redis and injected as a VALUES CTE; column governance (RLS/masking/visibility…

  Scenario: REQ-230 default behaviour
    Given a table designated as hot
    When a query references that table
    Then the cached JSON blob is injected as a VALUES CTE and governance is applied by Stage-2 at
    query time
