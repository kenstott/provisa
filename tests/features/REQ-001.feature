# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-001 — Query Governance
  # Any authenticated identity can query using any supported language (GraphQL, SQL, pgwire, Arrow Flight). Data returned is…

  Scenario: REQ-001 default behaviour
    Given an authenticated identity with role "analyst"
    When a GraphQL query is submitted against a registered table
    Then data is returned filtered by RLS and masking rules only
    And no capability gate rejects the query
