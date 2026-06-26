# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-234 — Materialized View Lifecycle
  # Auto-materialized view storage reclamation -- when a view is removed from config, disabled, or its source table is unreg…

  Scenario: REQ-234 default behaviour
    Given a materialized view that is removed from config or whose source is unregistered
    When config is reloaded or the daily cleanup runs
    Then the backing MV table is dropped and any orphaned MV tables are flagged for auto-drop
