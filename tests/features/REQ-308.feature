# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-308 — GraphQL Remote Schema Connector (REQ-307–313)
  # Remote GraphQL fields as tables. [SUPERSEDED by the 2026-10-02 REGISTRATION IS CURATION amendment below -- a Query field…

  Scenario: REQ-308 default behaviour
    Given a remote GraphQL source has been added
    When the steward opens the Register Table picker for it
    Then every table its schema offers is listed and none is registered
    And a table the steward registers is readable, with the columns chosen
    And the tables not registered remain unregistered
