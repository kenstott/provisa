# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-274 — SQL & Multi-Protocol Client Access
  # Query language selection is per-call for DB-API and GraphQL clients: pass a GraphQL string to execute via Stage 1+2; pas…

  Scenario: REQ-274 default behaviour
    Given a DB-API or GraphQL client making a call
    When a GraphQL string is passed it executes via Stage 1+2; when SQL is passed it uses Stage 2 only
    Then ADBC, SQLAlchemy, and JDBC always use SQL via Stage 2 only
