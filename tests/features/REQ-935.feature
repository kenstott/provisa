# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-935 — Materialization Store
  # At land time, incoming rows are mapped to the target columns by NAME (matched land, unmatched target NULL, extra keys dr…

  Scenario: REQ-935 default behaviour
    Given incoming rows with columns matching the target schema by name
    When check_source_drift is called
    Then rows are mapped to target columns with unmatched targets set to NULL
    Given incoming rows below the match floor threshold
    When check_source_drift is called
    Then the land is refused and an error event is emitted
