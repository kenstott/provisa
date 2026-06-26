# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-667 — Cypher Mutations
  # `MATCH (n:Label) WHERE ... DELETE n` is translated to `DELETE FROM catalog.schema.table WHERE ...`. The WHERE clause is…

  Scenario: REQ-667 default behaviour
    Given a Cypher MATCH-DELETE statement targeting a registered label
    When the WriteTranslator processes the statement
    Then the output is a DELETE FROM SQL statement with the WHERE clause from the MATCH pattern
