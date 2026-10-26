# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-372 — File & Lake Sources
  # [SUPERSEDED by REQ-1163, 2026-10-26 -- an Iceberg or Delta table travels under the one time-travel argument every relati…

  Scenario: REQ-372 default behaviour
    Given an Iceberg table read with as_recorded at query time
    When the statement is routed to the engine
    Then the snapshot current at that time is read (FOR TIMESTAMP AS OF), as_of is refused by name, and a table that keeps no versions refuses both
