# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-958 — Derived / MV Processor
  # Temporal MVs are expressed as completeness-gated processing windows, not a separate snapshot pipeline. A node's required…

  Scenario: REQ-958 default behaviour
    Given an MV whose lineage requires inputs A and B
    And a processing window with a deadline
    When A posts a windowed event but B has not
    Then the window stays open and does not fire
    When B posts its windowed event before the deadline
    Then the window fires once, computing as-of ctx.window.end
    And the result lands as an append keyed on the window boundary
    When a window reaches its deadline with the set incomplete
    Then it fires partial with a warn, or defers, per the node's policy
