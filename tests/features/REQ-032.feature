# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-032 — Mutation Execution
  # DB mutations are single-source by definition, bypass Trino, no routing decision, no registry approval — but always requi…

  Scenario: REQ-032 default behaviour
    Given a DB mutation targeting a registered table
    When the mutation is executed
    Then it bypasses Trino and registry approval but enforces write authority on the target table
