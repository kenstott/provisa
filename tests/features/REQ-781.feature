# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-781 — Graph Explorer
  # Graph Explorer Favorites: clicking a favorite's label loads the Cypher query into the editor for execution or modificati…

  Scenario: REQ-781 default behaviour
    Given the Favorites panel displays a favorite with a Cypher query
    When the user clicks the favorite's label
    Then the query is loaded into the editor header
    And the editor is ready to execute or modify the query
