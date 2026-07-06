# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-903 — PostgreSQL Deployment
  # Postgres federation engines validate connector availability and fail explicitly when sources require unavailable connect…

  Scenario: REQ-903 default behaviour
    Given a source requiring an unavailable connector
    When query planning occurs
    Then the source resolves to UnreachableSource with explicit error.
