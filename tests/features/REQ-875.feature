# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-875 — Query Routing
  # Cheap-count route: a count(*)-shaped query over an UNMATERIALIZED source that exposes an EXACT native count (REQ-673 car…

  Scenario: REQ-875 default behaviour
    Given an unmaterialized API source with an exact cardinality capability and no RLS predicate
    When a user issues a bare SELECT count(*) query with no WHERE clause
    Then the query is routed to the native count call instead of materializing the full dataset
    And when the same query applies to a table where RLS rules restrict the user's visible rows
    Then the cheap-count route is disabled and the query falls back to materialize+count to preserve governance
