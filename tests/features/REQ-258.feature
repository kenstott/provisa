# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-258 — API & Integration
  # SSE subscriptions via `GET /data/subscribe/{table}` with pluggable notification providers per source type. PostgreSQL us…

  Scenario: REQ-258 default behaviour
    Given a client subscribing to GET /data/subscribe/{table}
    When the source type is PostgreSQL, MongoDB, or Kafka
    Then change events stream via SSE using the native provider with RLS filtering applied
