# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-841 — Federation Engine Abstraction
  # A reachable source (one with a connector for the selected engine) is exposed by that connector's mechanism — attach (ref…

  Scenario: REQ-841 default behaviour
    Given a source reference and the configured federation engine
    When the planner resolves it
    Then if a connector exists it is exposed by that connector's mechanism (attach in place, or land into materialization_store), otherwise it is rejected as unreachable.
