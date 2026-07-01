# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-232 — Hot Tables (Redis-Cached Lookups)
  # Hot table JOIN optimization -- when a query joins a hot table (e.g., `orders JOIN countries ON country_code`), the compi…

  Scenario: REQ-232 default behaviour
    Given a query that JOINs a hot table
    When the compiler processes the query
    Then the hot table data is injected as a VALUES-based CTE and the DB engine sees no table reference
