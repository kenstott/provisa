# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-398 — API & Integration
  # The `/data/graph-schema` REST endpoint must expose `pk_columns` (list of column names per node label) so the UI can dete…

  Scenario: REQ-398 default behaviour
    Given the UI requesting /data/graph-schema
    When the endpoint responds
    Then pk_columns are included per node label so the UI can determine exclusion eligibility
