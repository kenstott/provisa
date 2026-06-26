# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-413 — Registration & Governance
  # Auto-generate GQL relationships from FK constraints in database schema introspection — relationships discoverable from F…

  Scenario: REQ-413 default behaviour
    Given a database source with FK constraints
    When schema introspection runs
    Then GQL relationships are auto-generated from the FK metadata
