# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-861 — Freshness Gating
  # File-based sources (CSV, Parquet, etc.) may carry an optional producer command (argv) that runs on-stale (gate-triggered…

  Scenario: REQ-861 default behaviour
    Given a file source S (type csv) with freshness_gate=true and a producer_command argv When the REQ-860 gate reports S stale and its residency read runs Then the producer command executes (shell=False) before the file is read, a non-zero exit fails loud and the stale file is not read, while a source within its TTL runs no producer and a source with no producer_command is read as-is
