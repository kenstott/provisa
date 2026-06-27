# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-768 — Cypher Query Frontend (Phase AU)
  # Node graph variable deserialization from SQL result rows. JSON columns marked as NODE in graph_vars are parsed into Node…

  Scenario: REQ-768 default behaviour
    Given a SQL result with a JSON column marked as NODE
    When the assembler processes the rows
    Then it deserializes the JSON into typed Node objects
