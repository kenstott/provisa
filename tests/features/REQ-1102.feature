# Generated from docs/arch/requirements.yaml. Do not hand-edit.
Feature: REQ-1102 — MCP Status Endpoint
  # The MCP status endpoint returns a resolved, proxy-aware, editable connect URL derived from X-Forwarded-Host/Host headers…

  Scenario: REQ-1102 default behaviour
    Given MCP is enabled on the server with PROVISA_MCP_PORT=9999
    When the UI calls GET /api/mcp/status with X-Forwarded-Host: example.com
    Then the endpoint returns {url: "http://example.com:9999/mcp", enabled: true}
