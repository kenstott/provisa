# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-213 — Hasura v2 Parity: Low-Complexity Features
  # `DISTINCT ON` query argument -- deduplicate results by specified columns. Added as `distinct_on` arg on root query field…

  Scenario: REQ-213 default behaviour
    Given a GraphQL query with a distinct_on argument specifying columns
    When the compiler processes it
    Then deduplicated results are returned using DISTINCT ON or a window function fallback for non-PostgreSQL dialects
