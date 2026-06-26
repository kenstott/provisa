# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-067 — SQLGlot Transpilation
  # Target dialect determined by source type captured at table registration time.

  Scenario: REQ-067 default behaviour
    Given a table registered with a specific source type
    When a query is transpiled
    Then the target SQL dialect matches the source type recorded at registration time
