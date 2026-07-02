# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-814 — Subscriptions
  # Provider selection (_resolve_provider_type in provisa/api/data/subscribe.py + get_provider in provisa/subscriptions/regi…

  Scenario: REQ-814 default behaviour
    Given a PostgreSQL table with live.strategy=native
    When get_provider() is called
    Then PgNotificationProvider is instantiated
    And the source_type is not used to dispatch
