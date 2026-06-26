# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-588 — pgwire Server
  # The catalog intercept layer handles scalar expression queries: `current_user` / `session_user` → authenticated `role_id`…

  Scenario: REQ-588 default behaviour
    Given a JDBC driver or ORM issuing scalar probes like current_user or version()
    When the catalog intercept layer processes the query
    Then hardcoded values are returned without a Trino round-trip
