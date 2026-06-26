# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-281 — Federation Performance
  # Source-level `federation_hints` use the Provisa-branded @provisa vocabulary (`join=broadcast|partitioned`, `reorder=none…

  Scenario: REQ-281 default behaviour
    Given a source config with federation_hints using the @provisa vocabulary
    When a query touches that source
    Then translate_federation_hints converts the hints to Trino session props before execution
