# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-1105 — MCP Authentication
  # Wire resolve_token_role into the FastMCP Streamable HTTP transport to map remote bearer tokens to Provisa roles, enablin…

  Scenario: REQ-1105 default behaviour
    Given the MCP Streamable HTTP transport bound off the loopback
    When a request arrives carrying an OIDC bearer token
    Then the token is resolved to a Provisa role via the same provider/claim mapping pgwire uses
    And that role governs the tool call for the duration of the request
    And a request with no bearer, or a token that resolves to no role, is rejected 401 (fail closed, never admin)
