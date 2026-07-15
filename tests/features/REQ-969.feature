# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-969 — Derived / MV Processor
  # Incremental maintenance computes a derived node's output from an upstream DELTA instead of a full recompute — the cost a…

  Scenario: REQ-969 default behaviour
    Given an incrementally-maintained MV over a fact table with a PK
    When the fact posts a delta of changed rows
    Then the MV applies only those rows to its prior state and emits a delta, with no full recompute
    Given an operator declares incremental on an MV whose SQL has no incremental form or no PK
    Then registration errors, and never silently falls back to full recompute
