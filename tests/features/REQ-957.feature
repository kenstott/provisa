# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-957 — Table Processor (Shared)
  # A node's processing envelope collapses to ONE optional user hook: preprocess(rows, ctx) -> rows, run after produce (sour…

  Scenario: REQ-957 default behaviour
    Given an MV node with a preprocess(rows, ctx) hook
    When the node is claimed and produce yields rows
    Then preprocess runs before land
    And returning rows lands them and re-posts the node's change
    And returning [] lands nothing and re-posts nothing
    And ctx.warn(reasons) emits a warn event yet still lands the rows
    And raising emits an error event, lands nothing, and fans the error to dependents
    And a claimed upstream error short-circuits to error before produce runs
