# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-264 — Two-Stage Compiler (Governed SQL)
  # Stage 2 must handle all SQL structural patterns: subqueries, CTEs, JOINs, `SELECT *` (expand via schema introspection th…

  Scenario: REQ-264 default behaviour
    Given a SQL query with subqueries, CTEs, JOINs, SELECT *, UNION, or nested expressions
    When Stage 2 processes the query
    Then RLS and masking are injected at every table reference in the full AST
