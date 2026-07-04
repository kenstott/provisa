# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-874 — Materialization Store
  # Delta fetch for materialization_store REPLICA incremental refresh: an INCREMENTAL RELOAD strategy for datasets whose fed…

  Scenario: REQ-874 default behaviour
    Given a MATERIALIZED-strategy dataset with a monotonic watermark, a delta query authored with $wm and {{fields}} placeholders, and a replica in a mutable relational store
    When the watermark signals a change (REQ-855 probe)
    And Provisa substitutes $wm with the cursor value and {{fields}} with the table's selection set
    Then the rendered delta query fetches only rows changed since the last watermark
    And those rows are upserted on the replica's registered primary key to replace prior state, or inserted if the source is append-only
    And the replica remains fresh without full re-materialization
