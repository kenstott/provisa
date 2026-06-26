# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-572 — Cypher Query Frontend (Phase AU)
  # Provisa handles CALL db.labels(), CALL db.relationshipTypes(), and CALL db.propertyKeys() as introspection procedures th…

  Scenario: REQ-572 default behaviour
    Given a client issuing CALL db.labels()
    When the cypher router handles it
    Then it returns label data from CypherLabelMap without generating or executing SQL
