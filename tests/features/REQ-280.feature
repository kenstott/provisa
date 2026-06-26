# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-280 — Federation Performance
  # ANALYZE runs on the API cache table after each materialization CTAS; ANALYZE failure is logged, not raised (connector to…

  Scenario: REQ-280 default behaviour
    Given a materialization CTAS that has completed
    When ANALYZE runs on the resulting API cache table
    Then ANALYZE failures are logged but do not raise or fail the materialization
