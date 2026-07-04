# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-872 — Authorization
  # Registered remote-schema mutations/functions (tracked_functions, tracked_webhooks) MUST be projected into each query sur…

  Scenario: REQ-872 default behaviour
    Given a tracked_function "createOrder(customer_id integer, total number)" returning a table (return_schema set), visible to all roles, in the registry
    When a SQL-surface client queries information_schema.routines and information_schema.parameters over pgwire as a role that can see it
    Then routines lists createOrder as a set-returning FUNCTION and parameters lists customer_id and total in ordinal order with their SQL data types (pg_proc shows proname=createOrder, pronargs=2, proretset=true)
    And a function whose visible_to excludes the querying role does not appear in the catalog
