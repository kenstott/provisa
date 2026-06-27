# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-791 — File & Lake Sources
  # Registered file-based tables are queryable via the data GraphQL endpoint. Tables appear in the GraphQL schema with domai…

  Scenario: REQ-791 default behaviour
    Given a registered customers table created from CSV files via file connector
    When a GraphQL query is issued against the data endpoint for customers
    Then the query returns all rows from the CSV files with all columns
    And the response matches the CSV schema with snake_case column names
