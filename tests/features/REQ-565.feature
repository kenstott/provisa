# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-565 — Subscriptions
  # At startup, Provisa idempotently installs `AFTER INSERT OR UPDATE OR DELETE` triggers on all registered PostgreSQL subsc…

  Scenario: REQ-565 default behaviour
    Given Provisa has started and registered a PostgreSQL subscription table
    When an external process inserts a row directly into the table
    Then the trigger fires pg_notify and the SSE subscriber receives the change event
