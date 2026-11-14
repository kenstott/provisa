# Copyright (c) 2026 Kenneth Stott
# Canary: 3d7c0a52-5b0e-4f7e-9a51-2c8e64f1d0b7
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2246: the MCP server's sip mode, through the Streamable HTTP transport a client uses.

Each test connects real MCP client sessions to the server's ASGI app (the role middleware and
FastMCP's session manager), in process: the mode is the ``tools`` parameter of the address, or
the operator setting ``mcp.tools`` when the address names none.
"""

from __future__ import annotations

import json
from contextlib import asynccontextmanager
from collections.abc import AsyncGenerator, Callable
from typing import Any

import httpx
import pytest

from provisa.api.mcp import server as mcp_server
from provisa.api.mcp import tool_groups
from tests.role_state import RoleState
from tests.unit.mcp_local_principal import act_for

_BASE = "http://127.0.0.1:8000"


def _state() -> Any:
    return RoleState(contexts={"analyst": object(), "steward": object()}, org_id="default")


Connect = Callable[..., Any]


@asynccontextmanager
async def _serving(state: Any) -> AsyncGenerator[tuple[Any, Connect]]:
    """The built server and ``connect(query, notes=None)``: a context giving an initialized MCP
    client session on ``/mcp<query>``; ``notes`` collects the server's notifications to it."""
    from mcp import ClientSession
    from mcp.client.streamable_http import streamable_http_client

    mcp = mcp_server.build_mcp_server(state)
    app = mcp_server._wrap_role_auth(mcp.streamable_http_app(), state, require_token=False)

    @asynccontextmanager
    async def connect(query: str = "", notes: list | None = None) -> AsyncGenerator[Any]:
        async def _on_message(message: Any) -> None:
            if notes is not None and not isinstance(message, Exception):
                notes.append(message)

        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url=_BASE, timeout=30) as http:
            async with streamable_http_client(f"{_BASE}/mcp{query}", http_client=http) as (
                read,
                write,
                _,
            ):
                async with ClientSession(read, write, message_handler=_on_message) as session:
                    await session.initialize()
                    yield session

    async with mcp.session_manager.run():
        yield mcp, connect


async def _names(session: Any) -> set[str]:
    return {t.name for t in (await session.list_tools()).tools}


def _expected_sip(registered: set[str]) -> set[str]:
    return {t for t in registered if tool_groups.GROUP_OF[t] in tool_groups.SIP_GROUPS}


async def _registered(mcp: Any) -> set[str]:
    return {t.name for t in await mcp.list_tools()}


def _payload(result: Any) -> Any:
    return json.loads(result.content[0].text)


async def test_a_sip_connection_lists_the_sip_groups_and_the_catalog_tool():
    async with _serving(_state()) as (mcp, connect):
        async with connect("?tools=sip") as session:
            names = await _names(session)
        expected = _expected_sip(await _registered(mcp))
    assert names == expected
    assert tool_groups.CATALOG_TOOL in names
    assert {"run_sql", "describe_table", "list_tables", "query_metric"} <= names
    assert not names & {"describe", "correlate", "create_glossary_term", "upsert_metric"}


async def test_the_catalog_tool_names_each_other_group_and_its_purpose():
    async with _serving(_state()) as (_mcp, connect):
        async with connect("?tools=sip") as session:
            answer = _payload(await session.call_tool(tool_groups.CATALOG_TOOL, {}))
    groups = {g["group"]: g for g in answer["groups"]}
    assert set(groups) == set(tool_groups.PURPOSES) - tool_groups.SIP_GROUPS
    assert set(groups) == {
        "statistics",
        "graph",
        "glossary",
        "data_products",
        "metric_management",
        "proposals",
        "test_data",
    }
    for name, group in groups.items():
        assert group["purpose"] == tool_groups.PURPOSES[name]
        assert group["listed"] is False
        assert group["tools"]
    assert answer["mode"] == "sip"


async def test_adding_a_group_lists_its_tools_in_that_session_only():
    notes: list = []
    async with _serving(_state()) as (mcp, connect):
        registered = await _registered(mcp)
        stats = {t for t in registered if tool_groups.GROUP_OF[t] == "statistics"}
        glossary = {t for t in registered if tool_groups.GROUP_OF[t] == "glossary"}
        async with connect("?tools=sip", notes) as one, connect("?tools=sip") as other:
            added = _payload(await one.call_tool(tool_groups.CATALOG_TOOL, {"add": "statistics"}))
            after_one = await _names(one)
            after_other = await _names(other)
            await one.call_tool(tool_groups.CATALOG_TOOL, {"add": "glossary"})
            after_two = await _names(one)
    sip = _expected_sip(registered)
    assert added["added"] == "statistics"
    assert {g["group"]: g["listed"] for g in added["groups"]}["statistics"] is True
    assert after_one == sip | stats
    assert after_other == sip
    assert after_two == sip | stats | glossary
    changed = [
        n for n in notes if getattr(n.root, "method", None) == "notifications/tools/list_changed"
    ]
    assert len(changed) == 2


async def test_an_unknown_group_is_refused_by_name():
    async with _serving(_state()) as (_mcp, connect):
        async with connect("?tools=sip") as session:
            result = await session.call_tool(tool_groups.CATALOG_TOOL, {"add": "astrology"})
    assert result.isError
    assert "astrology" in result.content[0].text


async def test_a_full_connection_lists_every_tool():
    async with _serving(_state()) as (mcp, connect):
        async with connect("?tools=full") as session:
            names = await _names(session)
        assert names == await _registered(mcp)


async def test_the_server_tells_a_client_its_tool_list_can_change():
    async with _serving(_state()) as (_mcp, connect):
        async with connect("?tools=sip") as session:
            init = session.get_server_capabilities()
    assert init is not None and init.tools is not None and init.tools.listChanged is True


@pytest.mark.parametrize("setting", ["sip", "full"])
async def test_a_connection_naming_no_mode_gets_the_settings_mode(monkeypatch, setting):
    monkeypatch.setenv("PROVISA_MCP_TOOLS", setting)
    async with _serving(_state()) as (mcp, connect):
        async with connect() as session:
            names = await _names(session)
        registered = await _registered(mcp)
    assert names == (registered if setting == "full" else _expected_sip(registered))


async def test_the_address_overrides_the_setting(monkeypatch):
    monkeypatch.setenv("PROVISA_MCP_TOOLS", "sip")
    async with _serving(_state()) as (mcp, connect):
        async with connect("?tools=full") as session:
            names = await _names(session)
        assert names == await _registered(mcp)


async def test_the_setting_defaults_to_full():
    from provisa.core import settings_registry

    assert settings_registry.setting("mcp.tools").default == "full"
    assert settings_registry.setting("mcp.tools").choices == tool_groups.MODES


async def test_an_unknown_mode_is_refused_by_name():
    state = _state()
    mcp = mcp_server.build_mcp_server(state)
    app = mcp_server._wrap_role_auth(mcp.streamable_http_app(), state, require_token=False)
    async with mcp.session_manager.run():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url=_BASE) as h:
            response = await h.post(
                "/mcp?tools=gulp",
                json={"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}},
                headers={"accept": "application/json, text/event-stream"},
            )
    assert response.status_code == 400
    assert "gulp" in response.json()["error"]
    assert "full" in response.json()["error"] and "sip" in response.json()["error"]


async def _rights_outcomes(connect: Connect, query: str, add: str | None) -> list[Any]:
    """What two calls of the statistics tool ``describe`` answer: as a role its rights allow, and
    as a role they refuse."""
    outcomes = []
    async with connect(query) as session:
        if add is not None:
            await session.call_tool(tool_groups.CATALOG_TOOL, {"add": add})
        for role in ("analyst", "steward"):
            result = await session.call_tool("describe", {"sql": "SELECT 1", "role": role})
            outcomes.append((result.isError, result.content[0].text))
    return outcomes


async def test_rights_on_an_added_or_unlisted_tool_are_decided_as_in_full_mode(monkeypatch):
    from provisa.api.mcp import tools

    state = _state()
    monkeypatch.setenv("PROVISA_MCP_ROLE", "analyst,steward")
    act_for(monkeypatch, state)

    async def _stats_describe(_state: Any, role: str, sql: str) -> dict:
        # The tool's own rights decision, as the real one makes it: by the role it acts as.
        if role != "analyst":
            raise PermissionError(f"role {role} may not describe")
        return {"role": role, "sql": sql}

    monkeypatch.setattr(tools, "stats_describe", _stats_describe)
    async with _serving(state) as (_mcp, connect):
        full = await _rights_outcomes(connect, "?tools=full", None)
        sip_added = await _rights_outcomes(connect, "?tools=sip", "statistics")
        sip_unlisted = await _rights_outcomes(connect, "?tools=sip", None)
    assert full[0][0] is False and "analyst" in full[0][1]
    assert full[1][0] is True and "may not describe" in full[1][1]
    assert sip_added == full
    assert sip_unlisted == full


async def test_every_registered_tool_has_exactly_one_group(monkeypatch):
    monkeypatch.setenv("TYPESAFEAI_API_KEY", "k")
    mcp = mcp_server.build_mcp_server(_state())
    registered = await _registered(mcp)
    assert "jev_evaluate" in registered
    assert registered == set(tool_groups.GROUP_OF)
    tool_groups.check_registered(registered)


def test_a_tool_with_no_group_fails_the_check():
    registered = set(tool_groups.GROUP_OF) | {"brand_new_tool"}
    with pytest.raises(RuntimeError, match="brand_new_tool"):
        tool_groups.check_registered(registered)


def test_a_group_naming_an_unregistered_tool_fails_the_check():
    registered = set(tool_groups.GROUP_OF) - {"run_sql"}
    with pytest.raises(RuntimeError, match="run_sql"):
        tool_groups.check_registered(registered)


async def test_the_server_refuses_to_build_with_an_ungrouped_tool(monkeypatch):
    monkeypatch.delitem(tool_groups.GROUP_OF, "list_schemas")
    with pytest.raises(RuntimeError, match="list_schemas"):
        mcp_server.build_mcp_server(_state())


def test_group_purposes_name_no_requirement():
    for purpose in tool_groups.PURPOSES.values():
        assert "REQ-" not in purpose
