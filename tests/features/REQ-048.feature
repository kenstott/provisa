# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-048 — Output & Delivery
  # NDJSON streaming variant: one JSON object per line.

  Scenario: REQ-048 default behaviour
    Given a query returning multiple rows
    When the output format is NDJSON
    Then each result row is emitted as a single JSON object on its own line
