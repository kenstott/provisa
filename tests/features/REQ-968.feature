# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-968 — Derived / MV Processor
  # Forced regen / replay recomputes a derived node — or an addressable calendar window (e.g. 2026-Q1, REQ-962) — ON DEMAND,…

  Scenario: REQ-968 default behaviour
    Given a sealed daily window 2026-07-08 with a corrected upstream
    When an operator forces regen by window-id 2026-07-08
    Then that period recomputes exactly from its as-of inputs, bypassing the no-op gate
    And the forced event is marked in the audit stream
    Given a changed MV SQL definition
    When an operator forces regen by MV node
    Then the node recomputes without re-landing its sources and cascades to dependents
