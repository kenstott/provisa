# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-923 — Trino Introspection
  # Trino column introspection (provisa/compiler/introspect.py, introspect_column_types / _fetch_with_startup_retry) retries…

  Scenario: REQ-923 default behaviour
    Given Trino introspection is invoked during coordinator startup
    When the coordinator reports SERVER_STARTING_UP
    Then the introspection retries with backoff up to the ready timeout
    When any other Trino error occurs
    Then the error propagates without retry
