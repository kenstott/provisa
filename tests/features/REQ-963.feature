# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-963 — Derived / MV Processor
  # Live (always-current) MVs debounce input churn using the SAME claim+deadline primitive as periodic MVs (REQ-959) — only…

  Scenario: REQ-963 default behaviour
    Given a live MV over 10 frequently-changing tables with debounce quiet=2s max_delay=30s
    When a burst of changes arrives across several tables within 2s
    Then one claim opens, coalesces the burst, and fires a single recompute after the lull
    When the tables change continuously and never go quiet for 2s
    Then the max_delay cap fires a recompute at least every 30s
    When quiet is 0
    Then every change recomputes immediately (debounce disabled)
    And each recompute replaces the current-state result (no window peg, no append)
