# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-810 — Materialized Views
  # Materialized-view transparent rewriting supports partial join-pattern matches. rewrite_if_mv_match in provisa/mv/rewrite…

  Scenario: REQ-810 default behaviour
    Given a materialized view covering a subset of a query's joins
    When the query is compiled
    Then the joins covered by the MV are rewritten to read the MV
    And the joins not covered by the MV are preserved and executed live
