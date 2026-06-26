# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-554 — Security
  # All roles receive sampled results capped at DEFAULT_SAMPLE_SIZE (default: 100 rows) unless the role holds the `full_resu…

  Scenario: REQ-554 default behaviour
    Given a role without the full_results capability
    When a query is executed
    Then results are capped at DEFAULT_SAMPLE_SIZE rows via the Stage 2 row cap mechanism
