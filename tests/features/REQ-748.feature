# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-748 — Security
  # Inverse relationship collision fix: joins producing inverse relationships (e.g., pets←assignments, assignments→pets) mus…

  Scenario: REQ-748 default behaviour
    Given two tables with inverse relationships sharing the same column names
    When semantic_sql_to_cypher converts joins in both directions
    Then forward join emits correct rel_type; inverse join emits distinct rel_type
