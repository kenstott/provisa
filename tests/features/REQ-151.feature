# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-151 — Column Path Extraction
  # Columns with `path` extract values from JSON source columns using PG `>>` syntax. SQLGlot transpiles to `json_extract_sc…

  Scenario: REQ-151 default behaviour
    Given a column configured with a path pointing into a JSON source column
    When a query is compiled for a PostgreSQL source
    Then PG >> syntax is used; when compiled for Trino, json_extract_scalar is used
