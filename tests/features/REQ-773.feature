# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-773 — Cypher Query Frontend (Phase AU)
  # Domain-scoped node projections (e.g., `MATCH (n:PetStore) RETURN n`) include all properties from nodes in the domain in…

  Scenario: REQ-773 default behaviour
    Given a Cypher query MATCH (n:DomainLabel) RETURN n where DomainLabel groups multiple node types
    When the translator processes it
    Then it emits UNION ALL with one branch per node type, each projecting all domain properties
