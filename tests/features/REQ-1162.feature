# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-1162 — Materialization Store
  # A materialized view's time-travel capability is a property of its DEFINITION, guaranteed on any materializing substrate…

  Scenario: REQ-1162 default behaviour
    Given a materialized view declared bitemporal (snapshot or delta) on a business key
    When it is refreshed repeatedly as rows are inserted, updated, and deleted
    Then each refresh only APPENDS (no UPDATE/DELETE), reconstruction returns the correct state at any past point, snapshot and delta modes agree, and on Iceberg each refresh is exactly one snapshot
