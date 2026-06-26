# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-267 — Two-Stage Compiler (Governed SQL)
  # A `/data/sql` REST endpoint accepts raw PG-compatible SQL + role (via auth), passes it through Stage 2 governance, route…

  Scenario: REQ-267 default behaviour
    Given a raw SQL query submitted to /data/sql with valid auth
    When the endpoint processes the query
    Then it passes through Stage 2 governance and executes identically to the GraphQL path
