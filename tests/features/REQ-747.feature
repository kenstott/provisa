# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-747 — Security
  # SQL validator bypass for remote same-source relationship pairs — when bypass_uncovered_relationships=True and both table…

  Scenario: REQ-747 default behaviour
    Given two remote tables from the same source_id with bypass_uncovered_relationships=True
    When SQL validator checks the join
    Then V002 violation is not raised; cross-source joins still require coverage
