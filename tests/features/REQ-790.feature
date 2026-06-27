# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-790 — File & Lake Sources
  # File connector table enumeration (via directory glob discovery) is accessible through the Provisa UI table registration…

  Scenario: REQ-790 default behaviour
    Given the Sources & Tables UI with a registered file connector source
    When a user clicks "Add Table" and selects the file source
    Then the schema dropdown is populated with discovered schemas
    And the table dropdown lists all CSV-derived tables in the selected schema
