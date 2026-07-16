# Copyright (c) 2026 Kenneth Stott
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
    result = await get_mcp_server()
    assert result["enabled"] is True
    assert result["port"] == 9100
    assert result["stdio_role"] == "analyst"
