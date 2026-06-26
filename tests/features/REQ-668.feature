# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-668 — Cypher Mutations
  # `MATCH (n:Label) WHERE ... SET n.prop = val` is translated to `UPDATE catalog.schema.table SET column = value WHERE ...`…

  Scenario: REQ-668 default behaviour
    Given a Cypher MATCH-SET statement with multiple property assignments
    When the WriteTranslator processes the statement
    Then the output is an UPDATE SQL statement with comma-separated SET clauses
    And domain-prefix stripping maps Cypher property names to physical column names
