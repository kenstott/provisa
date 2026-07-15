# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-983 — Derived / MV Processor
  # A preserved snapshot is a point-in-time dataset that is MATERIALIZED-AND-SEALED because it is NOT reconstructible from c…

  Scenario: REQ-983 default behaviour
    Given an MV whose upstream event history is not retained
    When an operator needs its state as-of a past moment
    Then a preserved snapshot must be declared with a why-tag (resource: history-not-retained) and landed as an immutable, addressable partition
    And it is never rewritten, only superseded by a new partition
    When the deriving history IS retained instead
    Then PIT is served as a read-time view over the accumulating MV (REQ-958) and no preserved snapshot is declared
