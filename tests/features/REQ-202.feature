# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-202 — OrderBy Alignment
  # Relationship ordering — order by related object fields (e.g., `order_by: {author: {name: asc}}`). Matches Hasura v2 defa…

  Scenario: REQ-202 default behaviour
    Given a query with order_by referencing a related object field
    When the compiler processes it
    Then the SQL ORDER BY clause references the related table's column
