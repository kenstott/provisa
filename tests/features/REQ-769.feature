# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-769 — Cypher Query Frontend (Phase AU)
  # Edge graph variable deserialization from SQL result rows. JSON columns marked as EDGE are parsed into Edge objects with…

  Scenario: REQ-769 default behaviour
    Given a SQL result with a JSON column marked as EDGE
    When the assembler processes the rows
    Then it deserializes the JSON into typed Edge objects with start/end node objects
