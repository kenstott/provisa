# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-666 — Cypher Mutations
  # `CREATE (n:Label {props})` is translated to `INSERT INTO catalog.schema.table (columns) VALUES (values)`. Property names…

  Scenario: REQ-666 default behaviour
    Given a Cypher CREATE statement with a registered label and scalar properties
    When the WriteTranslator processes the statement
    Then the output is an INSERT INTO SQL statement with correct column-value pairs
    And type coercion is applied to align Cypher scalar types with column types
