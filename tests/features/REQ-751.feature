# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-751 — Cypher Query Frontend (Phase AU)
  # [SUPERSEDED by REQ-751 amendment ANY ENGINE, ONLY WHAT THE START REACHES, 2026-10-28 -- the recursive CTE is engine-neut…

  Scenario: REQ-751 default behaviour
    Given a Cypher query with [*1..5] pattern between two node types
    When the translator processes it
    Then it emits a WITH RECURSIVE CTE with hop-count guards and JSON_ARRAY edges
