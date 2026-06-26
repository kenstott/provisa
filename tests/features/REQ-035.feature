# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-035 — Mutation Execution
  # RLS WHERE clauses injected into UPDATE and DELETE before execution.

  Scenario: REQ-035 default behaviour
    Given a table with RLS rules configured
    When an UPDATE or DELETE mutation is compiled
    Then RLS WHERE clauses are injected into the SQL before execution
