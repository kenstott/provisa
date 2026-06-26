# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-276 — Federation Performance
  # Admin API exposes a "Refresh Statistics" mutation per source that re-runs `ANALYZE` on demand. Useful for volatile sourc…

  Scenario: REQ-276 default behaviour
    Given a registered source with stale statistics
    When a steward calls the Refresh Statistics mutation
    Then ANALYZE is re-run on demand for that source
