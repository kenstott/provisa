# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-1012 — Internationalization
  # Internationalize the entire frontend: no hardcoded user-facing strings (~536 exist today). Use react-i18next with an 'en…

  Scenario: REQ-1012 default behaviour
    Given the frontend wired with react-i18next and an en base catalog
    When a component renders user-facing text
    Then the text resolves from the i18n catalog (no hardcoded literals) with locale-aware date/number formatting
