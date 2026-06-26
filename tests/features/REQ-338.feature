# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-338 — Phase AT — WebSocket & RSS Sources (REQ-338–344)
  # `websocket` is a supported source type. Connects to a WebSocket server, optionally sends a JSON subscribe payload on con…

  Scenario: REQ-338 default behaviour
    Given a WebSocket source registered with an optional subscribe payload
    When Provisa connects to the WebSocket server
    Then received JSON messages are emitted as ChangeEvents into the governed query fabric
