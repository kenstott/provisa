# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-934 — Materialization Store
  # The materialization store is RECONCILED, not blindly created: an existing landing table whose columns match config is KE…

  Scenario: REQ-934 default behaviour
    Given a source with a landing table matching config columns
    When the engine boots
    Then the table is kept intact and reused
    Given a config/table drift that changes columns
    When reconcile_table is called
    Then the table is dropped and recreated
    Given a source without a landing table
    When reconcile is triggered
    Then DDL creates the table without landing data
