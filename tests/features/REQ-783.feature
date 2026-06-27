# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-783 — Graph Explorer
  # Graph Explorer Favorites: delete button removes a favorite from the panel and localStorage immediately.

  Scenario: REQ-783 default behaviour
    Given the Favorites panel with at least one visible favorite
    When the user hovers over a favorite and clicks the delete button
    Then the favorite is removed from the panel immediately
    And the removal persists in localStorage (empty panel shows placeholder)
