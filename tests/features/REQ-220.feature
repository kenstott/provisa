# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-220 — Hasura v2 Parity: Medium-Complexity Features
  # Database event triggers -- table changes (insert/update/delete) fire webhooks. PostgreSQL trigger + `pg_notify()` -> asy…

  Scenario: REQ-220 default behaviour
    Given a table with event trigger config specifying a webhook URL and operation filter
    When an insert, update, or delete occurs on that table
    Then an HTTP POST is fired to the configured URL via the asyncpg listener with retry policy applied
