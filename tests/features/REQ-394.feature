# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-394 — Domain Model
  # Multiple PK checkboxes on a table infer a composite key; the first designated PK column is used as the canonical `id_col…

  Scenario: REQ-394 default behaviour
    Given a table with multiple columns designated as primary keys
    When Cypher node identity resolution runs
    Then the first designated PK column is used as the canonical id_column
