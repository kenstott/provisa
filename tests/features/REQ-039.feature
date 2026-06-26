# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-039 — Security
  # Schema visibility layer: unauthorized tables/columns do not appear in SDL or query builder; compiler rejects at parse ti…

  Scenario: REQ-039 default behaviour
    Given a user without rights to a table or column
    When the user accesses the SDL or query builder
    Then unauthorized tables and columns do not appear and are rejected at parse time
