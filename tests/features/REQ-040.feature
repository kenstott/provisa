# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-040 — Security
  # SQL enforcement layer: executor injects RLS WHERE clauses and strips unauthorized columns before execution, every reques…

  Scenario: REQ-040 default behaviour
    Given a query submitted by a user with restricted rights
    When the executor processes the query
    Then RLS WHERE clauses are injected and unauthorized columns are stripped before execution
