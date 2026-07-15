# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-970 — Derived / MV Processor
  # A derived node's store-table schema is DERIVED from its SQL SELECT (output column names + types), not taken from a sourc…

  Scenario: REQ-970 default behaviour
    Given an MV defined by a SELECT with computed columns
    When it is registered
    Then its output columns and types are derived from the SELECT and its store table is reconciled
    When the SELECT changes shape
    Then the store table is recreated and relanded on the next fire
    Given an MV declared persist=upsert with no derivable PK
    Then registration errors rather than silently landing a replace
