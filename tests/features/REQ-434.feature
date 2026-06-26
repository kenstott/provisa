# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-434 — Registration & Governance
  # Creation-request mechanism: any governed create operation (view, relationship, etc.) attempted by a user lacking the aut…

  Scenario: REQ-434 default behaviour
    Given a user without create authority attempting to create a view or relationship
    When they submit the creation
    Then a persisted request is created in the queue rather than an error; an authorized user may
    execute or reject it
