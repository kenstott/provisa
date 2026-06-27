# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-771 — Cypher Query Frontend (Phase AU)
  # Variable-length relationship edge columns (e.g., `[c*..5]` variable) are deserialized as lists of Edge objects from JSON…

  Scenario: REQ-771 default behaviour
    Given a SQL result with a JSON_ARRAY column containing edge objects from [*..n] pattern
    When the assembler processes the rows
    Then it deserializes the array into a list of Edge objects
