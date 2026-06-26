# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-576 — Cypher Query Frontend (Phase AU)
  # When shortestPath endpoints have different node types and no self-referential relationship exists in the schema, the tra…

  Scenario: REQ-576 default behaviour
    Given a shortestPath query between two different node types with a unique schema path
    When the translator processes it
    Then it emits a flat JOIN chain instead of a recursive CTE
