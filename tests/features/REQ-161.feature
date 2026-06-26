# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-161 — Query Development Tools
  # `POST /data/compile` returns compiled SQL with RLS/masking applied, route decision, and params without executing.

  Scenario: REQ-161 default behaviour
    Given a developer posting a query to /data/compile
    When the server applies RLS and masking
    Then compiled SQL, route decision, and params are returned without executing the query
