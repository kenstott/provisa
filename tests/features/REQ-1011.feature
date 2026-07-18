# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-1011 — Theming
  # Support both Dark and Light color schemes via Mantine's color-scheme system, with a user-facing theme toggle. Current st…

  Scenario: REQ-1011 default behaviour
    Given the app rendered with the Mantine color-scheme manager
    When the user activates the theme toggle
    Then the color scheme switches between dark and light and persists across reloads via localStorage
