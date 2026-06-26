# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-573 — Cypher Query Frontend (Phase AU)
  # Correlated CALL subqueries of the form CALL { WITH x MATCH (x)-[:R]->(n) RETURN n.prop AS alias } are translated to CROS…

  Scenario: REQ-573 default behaviour
    Given a Cypher CALL subquery with WITH importing an outer variable
    When the translator processes it
    Then it emits a CROSS JOIN LATERAL expression
