# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-720 — Neo4j Export
  # E2E export flow batches nodes and edges separately to avoid transaction timeouts, with default batch size of 200 nodes p…

  Scenario: REQ-720 default behaviour
    Given a graph with 500 nodes and 300 edges
    When the E2E export test runs
    Then nodes are sent in 3 batches (200, 200, 100) and edges in 1 batch
