# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-263 — Two-Stage Compiler (Governed SQL)
  # Stage 2 governance transformer applies four governance concerns: (1) RLS — inject WHERE predicate per table reference pe…

  Scenario: REQ-263 default behaviour
    Given a SQL query submitted by a role with governance rules configured
    When Stage 2 processes the query
    Then RLS predicates, column masking, column visibility, and row cap are all applied via AST rewrite
