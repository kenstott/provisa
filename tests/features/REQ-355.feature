# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-355 — Natural Language Query Service (Phase AV)
  # For each submitted NL query the service runs three independent parallel generation loops — one targeting Cypher, one Gra…

  Scenario: REQ-355 default behaviour
    Given an NL query submitted to the service
    When the three generation loops run
    Then each independently generates and validates a Cypher, GraphQL, and SQL candidate with
    compiler-driven refinement
