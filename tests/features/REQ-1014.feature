# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-1014 — Accessibility
  # Accessibility is a tested, enforced requirement: add automated axe-core checks (@axe-core/playwright) into the Playwrigh…

  Scenario: REQ-1014 default behaviour
    Given the Playwright e2e coverage fixture with @axe-core/playwright wired in
    When any e2e spec runs against a rendered page
    Then axe-core scans the DOM and the run asserts zero accessibility violations
