# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-612 — Registration & Governance
  # Relationship candidates are ranked by a four-level confidence hierarchy: (Highest) Approved catalog relationship validat…

  Scenario: REQ-612 default behaviour
    Given multiple relationship candidates of varying evidence types
    When candidates are presented to a steward
    Then they are ranked by confidence from approved catalog down to cross-source semantic inference
