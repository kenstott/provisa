# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-152 — Column Path Extraction
  # Path columns on PostgreSQL sources route direct. Non-PG sources force Trino routing.

  Scenario: REQ-152 default behaviour
    Given a table with path columns
    When the query engine routes a query
    Then PostgreSQL sources use the direct route and all non-PG sources are forced through Trino
