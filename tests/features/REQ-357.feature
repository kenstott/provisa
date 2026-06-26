# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-357 — Natural Language Query Service (Phase AV)
  # Once all three generation loops produce valid queries, all three are executed in parallel via the standard Provisa pipel…

  Scenario: REQ-357 default behaviour
    Given all three generation loops complete
    When results are returned
    Then the response includes cypher, graphql, and sql branches each with query text and result
