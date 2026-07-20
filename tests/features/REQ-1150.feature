# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-1150 — Protocol Support
  # Registered commands (tracked functions and webhooks -- REQ-205..208, REQ-242, REQ-885) MUST be invocable and discoverabl…

  Scenario: REQ-1150 default behaviour
    Given a command `active_users` registered as a set-returning query-kind tracked function When it is invoked over MCP, Arrow Flight, gRPC, and Cypher/Bolt Then each surface returns the same governed row set and the command is discoverable in that surface's catalog/schema, with writable_by / governance enforced identically to the GraphQL, pgwire, and REST surfaces
