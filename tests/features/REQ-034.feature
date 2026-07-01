# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-034 — Mutation Execution
  # Mutation input types reflect only columns user's role is permitted to write; excluded column references rejected at pars…

  Scenario: REQ-034 default behaviour
    Given a user whose role excludes certain columns
    When a mutation input type is generated for that role
    Then excluded columns are absent from the input type and references to them are rejected at parse time
