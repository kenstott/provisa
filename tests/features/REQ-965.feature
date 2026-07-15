# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-965 — Derived / MV Processor
  # An MV has TWO INDEPENDENT operator-declared outcomes, not one: a PERSISTENCE outcome (how the compute result is applied…

  Scenario: REQ-965 default behaviour
    Given an MV whose operator declares persistence=replace and event=append
    When it fires
    Then it overwrites its own store table with the current-state result
    And it emits an append event carrying that result as a batch
    And a downstream history MV appends each emission to accumulate a time series
    Given an MV whose operator declares persistence=replace and event=delta
    When it fires
    Then it recomputes the whole table and diffs new-vs-old once to emit only the changed rows
    Given an MV whose operator declares event=delta on a result with no primary key
    Then registration errors — delta needs row identity — and never silently emits replace
    Given a very large MV whose operator declares persistence=replace and event=replace
    Then each fire replaces the table and signals recompute, with no diff attempted
