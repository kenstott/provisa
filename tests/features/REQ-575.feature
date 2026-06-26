# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-575 — Cypher Query Frontend (Phase AU)
  # Bidirectional traversal syntax (a)-[]-(b) is rewritten at compile time to a UNION ALL of all matching directed forward a…

  Scenario: REQ-575 default behaviour
    Given a Cypher query with bidirectional traversal (a)-[]-(b)
    When the translator processes it
    Then it emits a UNION ALL of forward and backward directed relationship joins
