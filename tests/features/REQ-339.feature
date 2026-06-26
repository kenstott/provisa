# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-339 — Phase AT — WebSocket & RSS Sources (REQ-338–344)
  # WebSocket provider auto-reconnects on disconnect or error with configurable `reconnect_interval` (default: 5 s). The rec…

  Scenario: REQ-339 default behaviour
    Given a WebSocket source that experiences a transient disconnect
    When the connection drops
    Then the provider auto-reconnects after reconnect_interval and continues emitting events until close() is called
