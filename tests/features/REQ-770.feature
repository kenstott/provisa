# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-770 — Cypher Query Frontend (Phase AU)
  # Path graph variable deserialization from SQL result rows. Rows with _path_id and _depth columns are collapsed into singl…

  Scenario: REQ-770 default behaviour
    Given SQL result rows with _path_id and _depth columns marking path hops
    When the assembler processes the rows
    Then rows with matching _path_id are collapsed into a single Path object
