# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-775 — Cypher Query Frontend (Phase AU)
  # Anonymous all-rels path pattern `MATCH p=()-->()` (no node/edge labels or variables) generates a valid SQL subquery refe…

  Scenario: REQ-775 default behaviour
    Given a Cypher query MATCH p=()-->() RETURN p LIMIT 25
    When the translator processes it
    Then it emits a valid _all_rels subquery with JSON_OBJECT path serialization
