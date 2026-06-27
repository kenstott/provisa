# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-789 — File & Lake Sources
  # CSV column headers are automatically mapped to GraphQL field names using LINQ4J convention: camelCase headers are conver…

  Scenario: REQ-789 default behaviour
    Given a CSV file with camelCase headers (e.g., "companyName", "customerId")
    When the file is introspected by the file connector
    Then headers are automatically converted to snake_case (e.g., "company_name", "customer_id")
    And GraphQL field names reflect the snake_case conversion
