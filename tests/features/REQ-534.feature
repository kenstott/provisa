# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-534 — Compiler & Schema
  # GraphQL queries with multiple root fields are compiled into separate SQL queries and executed independently. Results are…

  Scenario: REQ-534 default behaviour
    Given a GraphQL query with multiple root fields
    When it is executed
    Then each root field is compiled and executed independently and results are merged into one
    response
