# Copyright (c) 2026 Kenneth Stott
# Canary: 2f68455f-6808-42ba-8d07-f1aa02713b2e
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for the MCP server status endpoint (REQ-1008).

The status reflects the SAME env config start_mcp_server reads. No fallback:
unset port => disabled/None, unset stdio role => None.
"""

from __future__ import annotations

import pytest

from provisa.api.mcp.status import get_mcp_server, mcp_status

_TOOL_NAMES = {
    "list_schemas",
    "list_tables",
    "describe_table",
    "run_sql",
    "explain_sql",
    "search_catalog",
}


class _FakeURL:
    def __init__(self, scheme: str) -> None:
        self.scheme = scheme


class _FakeRequest:
    """Minimal stand-in for the header/scheme access _resolve_mcp_url needs."""

    def __init__(self, headers: dict[str, str], scheme: str = "http") -> None:
        self.headers = headers
        self.url = _FakeURL(scheme)


def test_disabled_when_port_unset(monkeypatch):
    monkeypatch.delenv("PROVISA_MCP_PORT", raising=False)
    monkeypatch.delenv("PROVISA_MCP_ROLE", raising=False)
    monkeypatch.delenv("PROVISA_MCP_MAX_ROWS", raising=False)
    s = mcp_status()
    assert s["enabled"] is False
    assert s["port"] is None
    assert s["transport"] is None
    assert s["stdio_role"] is None
    assert s["max_rows"] == 1000
    assert s["enable_env_var"] == "PROVISA_MCP_PORT"
    assert {t["name"] for t in s["tools"]} == _TOOL_NAMES


def test_disabled_when_port_zero(monkeypatch):
    monkeypatch.setenv("PROVISA_MCP_PORT", "0")
    s = mcp_status()
    assert s["enabled"] is False
    assert s["port"] is None


def test_enabled_reports_port_transport_role(monkeypatch):
    monkeypatch.setenv("PROVISA_MCP_PORT", "9100")
    monkeypatch.setenv("PROVISA_MCP_ROLE", "analyst")
    monkeypatch.setenv("PROVISA_MCP_MAX_ROWS", "250")
    s = mcp_status()
    assert s["enabled"] is True
    assert s["port"] == 9100
    assert s["transport"] == "streamable-http"
    assert s["stdio_role"] == "analyst"
    assert s["max_rows"] == 250


def test_enabled_without_role_reports_none(monkeypatch):
    monkeypatch.setenv("PROVISA_MCP_PORT", "9100")
    monkeypatch.delenv("PROVISA_MCP_ROLE", raising=False)
    s = mcp_status()
    assert s["enabled"] is True
    assert s["stdio_role"] is None


@pytest.mark.asyncio
async def test_endpoint_returns_status(monkeypatch):
    monkeypatch.setenv("PROVISA_MCP_PORT", "9100")
    monkeypatch.setenv("PROVISA_MCP_ROLE", "analyst")
    result = await get_mcp_server(_FakeRequest({"host": "localhost:3000"}))
    assert result["enabled"] is True
    assert result["port"] == 9100
    assert result["stdio_role"] == "analyst"
    assert result["url"] == "http://localhost:9100/mcp"


# -- REQ-1102: connect-URL resolution ------------------------------------------
def test_url_none_when_disabled(monkeypatch):
    monkeypatch.delenv("PROVISA_MCP_PORT", raising=False)
    assert mcp_status(_FakeRequest({"host": "example.com"}))["url"] is None


def test_url_reuses_request_host_stripping_ui_port(monkeypatch):
    # The MCP server has its OWN port; only the hostname from the UI's host header is reused.
    monkeypatch.setenv("PROVISA_MCP_PORT", "8009")
    s = mcp_status(_FakeRequest({"host": "provisa.acme.com:3000"}))
    assert s["url"] == "http://provisa.acme.com:8009/mcp"


def test_url_honors_forwarded_headers(monkeypatch):
    monkeypatch.setenv("PROVISA_MCP_PORT", "8009")
    s = mcp_status(
        _FakeRequest({"x-forwarded-proto": "https", "x-forwarded-host": "data.acme.com"})
    )
    assert s["url"] == "https://data.acme.com:8009/mcp"


def test_url_explicit_override_wins(monkeypatch):
    monkeypatch.setenv("PROVISA_MCP_PORT", "8009")
    monkeypatch.setenv("PROVISA_MCP_EXTERNAL_URL", "https://mcp.acme.com/mcp")
    s = mcp_status(_FakeRequest({"host": "ignored:3000"}))
    assert s["url"] == "https://mcp.acme.com/mcp"


def test_url_localhost_when_no_request(monkeypatch):
    monkeypatch.setenv("PROVISA_MCP_PORT", "8009")
    monkeypatch.delenv("PROVISA_MCP_EXTERNAL_URL", raising=False)
    assert mcp_status()["url"] == "http://localhost:8009/mcp"


# -- REQ-1104: bundled-bridge command gate -------------------------------------
def test_bridge_command_none_unless_native_launcher_sets_it(monkeypatch):
    monkeypatch.setenv("PROVISA_MCP_PORT", "8009")
    monkeypatch.delenv("PROVISA_MCP_BRIDGE_COMMAND", raising=False)
    s = mcp_status(_FakeRequest({"host": "localhost:3000"}))
    assert s["bridge_command"] is None  # container/remote tier -> panel shows "supply your own"
    assert s["bridge_args"] == ["-m", "mcp_proxy", "--transport", "streamablehttp"]


def test_bridge_command_is_the_bundled_interpreter_on_native(monkeypatch):
    monkeypatch.setenv("PROVISA_MCP_PORT", "8009")
    monkeypatch.setenv("PROVISA_MCP_BRIDGE_COMMAND", "/home/u/.provisa/runtime/python")
    s = mcp_status(_FakeRequest({"host": "localhost:3000"}))
    assert s["bridge_command"] == "/home/u/.provisa/runtime/python"
