# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-772 — Cypher Query Frontend (Phase AU)
  # Graph rewriter applies JSON object wrapping to all graph variables in the SELECT clause. Scalar columns remain unwrapped…

  Scenario: REQ-772 default behaviour
    Given an SQL query with both scalar and graph variable columns
    When the graph rewriter processes it
    Then scalar columns are left unchanged, graph variables are wrapped in JSON_OBJECT
