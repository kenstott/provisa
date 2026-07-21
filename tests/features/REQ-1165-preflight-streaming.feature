Feature: REQ-1165 — Per-input streaming preflight check
  A preflight check receives a dict of lazy Arrow streams keyed by INPUT NODE, never a
  materialized row set. A SQL-expressible check is pushed down to an engine-side count probe over
  the named input; a non-SQL check streams that input's Arrow batches and short-circuits. A
  streaming check on an engine that does not advertise ARROW_STREAM fails loud (no materialize
  fallback).

  Scenario: A SQL-expressible check is pushed down over the named input node
    Given a real engine with an input node "orders" holding a negative quantity
    And a preflight check that aborts when any orders row has a negative quantity
    When the preflight gate evaluates before landing
    Then the verdict is abort
    And no Arrow stream was opened for the input

  Scenario: A non-SQL check streams the input's Arrow batches
    Given a real engine with an input node "orders" whose quantities sum below the threshold
    And a preflight check that quarantines when the running sum is too low
    When the preflight gate evaluates before landing
    Then the verdict is quarantine
    And the input node was streamed as Arrow batches

  Scenario: A streaming check on a non-streaming engine fails loud
    Given a real engine that does not advertise ARROW_STREAM
    And a non-SQL preflight check over the input
    When the preflight gate evaluates before landing
    Then the gate raises an unsupported-capability error
