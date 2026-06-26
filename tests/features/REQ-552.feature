# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-552 — Execution & Routing
  # Cross-source JOINs routed through the federation engine apply automatic type coercion when joining columns across source…

  Scenario: REQ-552 default behaviour
    Given a cross-source JOIN where join columns have differing native types across sources
    When the query is routed through the federation engine
    Then automatic type coercion is applied to prevent type mismatch errors
