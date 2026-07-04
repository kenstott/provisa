# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-855 — Materialization Store
  # materialization_store freshness policy — the ONE centralized freshness gate referenced by REQ-845 (reactive replica pull…

  Scenario: REQ-855 default behaviour
    Given a reactive replica pull-through source configured with the TTL+probe mode and a TTL floor
    When a query reads the cached rows before the floor elapses
    Then the materialized rows are served without probing the upstream
    When a later query arrives after the TTL floor has elapsed
    Then freshness_token(source, table) is evaluated and compared to the stored token; if equal the existing rows are kept, and if different the entry is invalidated, re-pulled, rematerialized, and the new token stored
    And a view materialization with a freshness gate skips its scheduled CTAS rebuild while the upstream token is unchanged.
