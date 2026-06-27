# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-782 — Graph Explorer
  # Graph Explorer Favorites: inline rename input allows editing a favorite's label with Enter to commit and Escape to cance…

  Scenario: REQ-782 default behaviour
    Given the Favorites panel with a visible favorite and action buttons on hover
    When the user clicks the rename button
    Then a text input appears focused with the current label
    When the user types a new label and presses Enter
    Then the input closes and the favorite displays the new label
    And the change persists in localStorage
