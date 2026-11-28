# Copyright (c) 2026 Kenneth Stott
# Canary: aa16f35e-cb3f-48f5-a13b-bd7cd37e4425
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""MCP protocol answers the conformance run checks (REQ-1008, REQ-2167): a tools/call naming
no tool the session may call is a JSON-RPC error, and the logging capability the server uses
for its log notifications is declared and its logging/setLevel answered."""

from __future__ import annotations

from typing import Any

import pytest

from provisa.api.mcp import tools
from tests.unit.mcp_local_principal import act_for
from tests.unit.test_mcp_sip_mode import _serving, _state


@pytest.fixture(autouse=True)
def _pinned(monkeypatch):
    monkeypatch.setenv("PROVISA_MCP_ROLE", "analyst")
    act_for(monkeypatch)


async def test_an_unknown_tool_is_a_json_rpc_invalid_params_error():
    from mcp import types
    from mcp.shared.exceptions import McpError

    async with _serving(_state()) as (_mcp, connect):
        async with connect("?tools=full") as session:
            with pytest.raises(McpError) as refused:
                await session.call_tool("no_such_tool_x", {})
            # The session still serves a call after the refusal.
            await session.send_ping()
    assert refused.value.error.code == types.INVALID_PARAMS
    assert refused.value.error.message == "Unknown tool: no_such_tool_x"


async def test_the_logging_capability_is_declared_and_set_level_answered():
    async with _serving(_state()) as (_mcp, connect):
        async with connect("?tools=full") as session:
            capabilities = session.get_server_capabilities()
            answered = await session.set_logging_level("warning")
    assert capabilities is not None and capabilities.logging is not None
    assert answered.model_dump(exclude_none=True) == {}


async def test_a_log_notification_below_the_sessions_level_is_not_sent(monkeypatch):
    from provisa.core.statement_warnings import ServerWarning, warn

    async def _run_sql(_state: Any, _role: str, sql: str, **_kw: Any) -> dict:
        warn(ServerWarning(code="test.warning", message=f"warned about {sql}"))
        return {"rows": []}

    monkeypatch.setattr(tools, "run_sql", _run_sql)
    notes: list = []

    def _logged() -> list[str]:
        return [
            n.root.params.data
            for n in notes
            if getattr(n.root, "method", None) == "notifications/message"
        ]

    async with _serving(_state()) as (_mcp, connect):
        async with connect("?tools=full", notes) as session:
            await session.call_tool("run_sql", {"sql": "SELECT 1"})
            before = _logged()
            await session.set_logging_level("error")
            await session.call_tool("run_sql", {"sql": "SELECT 2"})
            after = _logged()
    assert "warned about SELECT 1" in before
    assert "warned about SELECT 2" not in after
