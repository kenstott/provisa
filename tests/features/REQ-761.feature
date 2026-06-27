# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-761 — Cypher Query Frontend (Phase AU)
  # Implicit GROUP BY is inferred in the RETURN clause when non-aggregated columns are mixed with aggregate functions. All n…

  Scenario: REQ-761 default behaviour
    Given a Cypher query MATCH (n) RETURN n.type, count(*) AS cnt
    When the translator processes it
    Then it emits GROUP BY n.type inferred from non-aggregated columns
