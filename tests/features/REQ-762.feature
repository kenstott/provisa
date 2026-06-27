# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-762 — Cypher Query Frontend (Phase AU)
  # CASE expressions (both searched `CASE WHEN ... THEN ... ELSE ... END` and simple `CASE x WHEN ... THEN ... ELSE ... END`…

  Scenario: REQ-762 default behaviour
    Given a Cypher query MATCH (n) RETURN CASE WHEN n.age > 18 THEN 'adult' ELSE 'minor' END
    When the translator processes it
    Then it emits Trino CASE...WHEN...THEN...ELSE...END syntax
