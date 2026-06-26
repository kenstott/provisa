# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-583 — pgwire Server
  # After DDL execution on either the Trino or direct path, the new table is registered into the role's compilation context…

  Scenario: REQ-583 default behaviour
    Given a DDL statement that creates a table
    When execution completes
    Then the table is registered into the role's compilation context and immediately queryable
