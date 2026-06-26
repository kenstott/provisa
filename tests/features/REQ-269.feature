# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-269 — SQL & Multi-Protocol Client Access
  # DB-API 2.0 connection exposes all registered tables and views the user's rights permit for arbitrary SQL. There is no co…

  Scenario: REQ-269 default behaviour
    Given a DB-API 2.0 connection
    When arbitrary SQL is executed
    Then only tables and views permitted by the user's rights are accessible with uniform Stage 2
      governance
