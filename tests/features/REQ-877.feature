# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-877 — Row-Level Delta Capture
  # Opt-in, per-table row-level delta capture for materialized views — the row-level companion to the column-level lineage t…

  Scenario: REQ-877 default behaviour
    Given an MV with row-level delta capture enabled When the MV refreshes with rows inserted/updated/deleted since the prior refresh Then the delta ledger records the insert/update/delete rows under that refresh version, and an opt-out MV writes no ledger entries
