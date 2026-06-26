# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-144 — Arrow Flight
  # Zaychik Arrow Flight SQL proxy translates between Flight SQL clients and Trino JDBC.

  Scenario: REQ-144 default behaviour
    Given a Flight SQL client submits a query
    When Zaychik receives the request
    Then it translates the Flight SQL protocol to Trino JDBC and returns results as Arrow batches
