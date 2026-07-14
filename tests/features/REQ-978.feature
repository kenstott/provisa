# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-978 — Desktop Installation
  # Installing the demo is optional and OFF by default. When installed, the launcher opens the UI at ?tour=1 to auto-start t…

  Scenario: REQ-978 default behaviour
    Given a fresh desktop installation without demo data
    When the user completes the wizard and launches the app
    Then the UI loads without demo sources
    And tour mode is not active
    Given a user selects "Install demo" during the wizard
    When the launcher starts the UI
    Then the UI loads with demo sources
    And tour mode is active at ?tour=1
