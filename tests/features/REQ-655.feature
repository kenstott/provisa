# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-655 — Compiler & Schema
  # `{table}_group_by` supports two additional filter clauses beyond the base `where`: (1) `aggregates(where: {TypeName}Bool…

  Scenario: REQ-655 default behaviour
    Given a _group_by query with a having: clause on an aggregate field
    When the query is compiled
    Then the generated SQL includes a HAVING clause after GROUP BY
    And aggregates(where:) generates a SQL FILTER (WHERE ...) expression
