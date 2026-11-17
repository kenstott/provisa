# Copyright (c) 2026 Kenneth Stott
# Canary: 5b8e1f47-2c9a-4d03-a6e2-91f7c3b0d548
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2246 (amended 2026-11-16): an MCP session lists only what its roles may use, and each
command visible to the role is its own typed tool.

Real MCP client sessions over the server's ASGI app (the role middleware and FastMCP's session
manager), in process. Over remote HTTP the role is the bearer token's, resolved when the list is
asked for; over the local transport it is the pinned ``mcp.role``.
"""

# Requirements: REQ-2246

from __future__ import annotations

import json
from collections.abc import AsyncGenerator, Callable
from contextlib import asynccontextmanager
from types import SimpleNamespace
from typing import Any

import httpx
import pytest

from provisa.api.mcp import server as mcp_server
from provisa.api.mcp import tool_groups
from tests.role_state import RoleState
from tests.unit.mcp_local_principal import act_for

_BASE = "http://127.0.0.1:8000"

# The analyst reads and runs commands; the steward also curates the glossary and edits tables.
_READS = ["query_development", "command_execution", "glossary_read"]
_ROLES = {
    "analyst": {"id": "analyst", "capabilities": _READS, "domain_access": ["*"]},
    "steward": {
        "id": "steward",
        "capabilities": [*_READS, "glossary_rw", "table_registration"],
        "domain_access": ["*"],
    },
}


def _command(name: str, visible_to: list[str], **extra: Any) -> dict:
    return {
        "name": name,
        "domain_id": "sales",
        "kind": "query",
        "impl_kind": "python",
        "visible_to": visible_to,
        "arguments": [
            {"name": "region", "type": "String"},
            {"name": "top", "type": "Int", "default": 5},
        ],
        "description": f"The {name} command.",
        **extra,
    }


def _state(**commands: dict) -> Any:
    return RoleState(
        contexts={"analyst": object(), "steward": object()},
        roles={k: dict(v) for k, v in _ROLES.items()},
        org_id="default",
        idp_entitlements={},
        tracked_functions=dict(commands),
        tracked_webhooks={},
        undefined_commands={},
        model_stamp="stamp",
        auth_config={"provider": "test"},
        multitenancy=False,
    )


def _registered_offered(mcp: Any, rights: set[str]) -> set[str]:
    return {
        t.name
        for t in mcp._tool_manager.list_tools()
        if tool_groups.offered(t.name, rights) and t.name != "jev_evaluate"
    }


Connect = Callable[..., Any]


@asynccontextmanager
async def _serving(state: Any) -> AsyncGenerator[tuple[Any, Connect]]:
    """The built server and ``connect(query, token=None)``: an initialized MCP client session on
    ``/mcp<query>``, sending ``token`` as its bearer when given."""
    from mcp import ClientSession
    from mcp.client.streamable_http import streamable_http_client

    mcp = mcp_server.build_mcp_server(state)
    app = mcp_server._wrap_role_auth(mcp.streamable_http_app(), state, require_token=False)

    @asynccontextmanager
    async def connect(query: str = "", token: str | None = None) -> AsyncGenerator[Any]:
        headers = {"authorization": f"Bearer {token}"} if token else {}
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url=_BASE, timeout=30, headers=headers
        ) as http:
            async with streamable_http_client(f"{_BASE}/mcp{query}", http_client=http) as (
                read,
                write,
                _,
            ):
                async with ClientSession(read, write) as session:
                    await session.initialize()
                    yield session

    async with mcp.session_manager.run():
        yield mcp, connect


@pytest.fixture
def bearer(monkeypatch):
    """Remote HTTP: a bearer token ``as-<role>`` is a principal holding just that role."""

    async def _validate(token: str, _state: Any) -> Any:
        role = token.removeprefix("as-")
        return SimpleNamespace(
            user_id=f"user-{role}", roles=[role], raw_claims={}, entitlements_claim=None
        )

    async def _roles(identity: Any, _state: Any, _org_id: str) -> tuple[str, list[str]]:
        return identity.roles[0], list(identity.roles)

    async def _ready(_org_id: str) -> None:
        return None

    async def _not_suspended(*_args: Any) -> bool:
        return False

    from provisa.api import app as app_module
    from provisa.security import role_probe

    monkeypatch.setattr(mcp_server, "_validate_mcp_token", _validate)
    monkeypatch.setattr(mcp_server, "_roles_for_identity", _roles)
    monkeypatch.setattr(app_module, "ensure_serving_runtime", _ready)
    monkeypatch.setattr(role_probe, "suspended", _not_suspended)


async def _names(session: Any) -> set[str]:
    return {t.name for t in (await session.list_tools()).tools}


def _payload(result: Any) -> Any:
    return json.loads(result.content[0].text)


# -- every tool has its rights, checked as its group is --------------------------------------


def test_every_registered_tool_has_a_rights_entry():
    mcp = mcp_server.build_mcp_server(_state())
    registered = {t.name for t in mcp._tool_manager.list_tools()}
    assert registered == set(tool_groups.RIGHTS_OF)


def test_a_tool_with_no_rights_entry_fails_the_check(monkeypatch):
    monkeypatch.delitem(tool_groups.RIGHTS_OF, "list_schemas")
    with pytest.raises(RuntimeError, match="list_schemas"):
        mcp_server.build_mcp_server(_state())


def test_a_right_that_does_not_exist_fails_the_check(monkeypatch):
    monkeypatch.setitem(tool_groups.RIGHTS_OF, "list_schemas", frozenset({"astrology"}))
    with pytest.raises(RuntimeError, match="astrology"):
        mcp_server.build_mcp_server(_state())


# -- remote HTTP: the role is the bearer token's ---------------------------------------------


async def test_two_tokens_of_different_rights_are_listed_different_tools(bearer):
    async with _serving(_state()) as (mcp, connect):
        async with connect("?tools=full", token="as-analyst") as session:
            analyst = await _names(session)
        async with connect("?tools=full", token="as-steward") as session:
            steward = await _names(session)
        held_by = {r: set(_ROLES[r]["capabilities"]) for r in _ROLES}
        assert analyst == _registered_offered(mcp, held_by["analyst"])
        assert steward == _registered_offered(mcp, held_by["steward"])
    curating = {"create_glossary_term", "upsert_metric", "find_table_id"}
    assert curating <= steward
    assert not curating & analyst
    assert {"run_sql", "list_glossary_terms", "list_commands"} <= analyst
    # Neither holds environment_management: its tools are listed to neither.
    assert "get_environment_detail" not in analyst | steward


async def test_a_tool_the_list_does_not_offer_is_refused_by_name(bearer):
    async with _serving(_state()) as (_mcp, connect):
        async with connect("?tools=full", token="as-analyst") as session:
            result = await session.call_tool("upsert_metric", {"name": "m", "expression": "x"})
    assert result.isError
    assert result.content[0].text == "Tool upsert_metric is not offered to your roles"


async def test_a_token_is_listed_the_commands_its_role_sees_as_typed_tools(bearer):
    state = _state(
        top_regions=_command("top_regions", ["analyst", "steward"]),
        restock=_command("restock", ["steward"], kind="mutation"),
    )
    async with _serving(state) as (_mcp, connect):
        async with connect("?tools=full", token="as-analyst") as session:
            analyst = {t.name: t for t in (await session.list_tools()).tools}
        async with connect("?tools=full", token="as-steward") as session:
            steward = {t.name: t for t in (await session.list_tools()).tools}
    assert "top_regions" in analyst and "restock" not in analyst
    assert {"top_regions", "restock"} <= set(steward)
    tool = analyst["top_regions"]
    assert tool.description == "The top_regions command."
    assert tool.inputSchema["properties"]["region"]["type"] == "string"
    assert tool.inputSchema["properties"]["top"]["type"] == "integer"
    assert tool.inputSchema["properties"]["top"]["default"] == 5
    assert tool.inputSchema["required"] == ["region"]
    assert tool.annotations is not None and tool.annotations.readOnlyHint is True
    assert steward["restock"].annotations.readOnlyHint is False


async def test_a_command_tool_runs_through_the_one_command_invocation(bearer, monkeypatch):
    from provisa.api.data import action_exec

    ran: list[tuple[str, dict, str]] = []

    async def _dispatch(fn: dict, args: dict, _state: Any, role_id: str) -> list[dict]:
        ran.append((fn["name"], dict(args), role_id))
        return [{"region": args["region"], "n": args["top"]}]

    monkeypatch.setattr(action_exec, "dispatch_function", _dispatch)
    monkeypatch.setattr(action_exec, "_record_call", lambda *_a: None)
    state = _state(top_regions=_command("top_regions", ["analyst"]))
    async with _serving(state) as (_mcp, connect):
        async with connect("?tools=full", token="as-analyst") as session:
            result = await session.call_tool("top_regions", {"region": "eu"})
            missing = await session.call_tool("top_regions", {})
            wrong = await session.call_tool("top_regions", {"region": "eu", "top": "many"})
    assert not result.isError, result.content[0].text
    assert _payload(result) == {"rows": [{"region": "eu", "n": 5}]}
    assert ran == [("top_regions", {"region": "eu", "top": 5}, "analyst")]
    assert missing.isError and "region" in missing.content[0].text
    assert wrong.isError and "many" in wrong.content[0].text


async def test_a_command_the_role_does_not_see_is_refused_as_an_unknown_tool(bearer):
    state = _state(restock=_command("restock", ["steward"]))
    async with _serving(state) as (_mcp, connect):
        async with connect("?tools=full", token="as-analyst") as session:
            result = await session.call_tool("restock", {"region": "eu"})
    assert result.isError
    assert result.content[0].text == "Unknown tool: restock"


async def test_a_command_named_by_its_mcp_binding_is_listed_by_that_name(bearer):
    state = _state(
        top_regions=_command(
            "top_regions", ["analyst"], language_bindings={"mcp": {"name": "best_regions"}}
        ),
        hidden=_command("hidden", ["analyst"], language_bindings={"mcp": {"exclude": True}}),
    )
    async with _serving(state) as (_mcp, connect):
        async with connect("?tools=full", token="as-analyst") as session:
            names = await _names(session)
    assert "best_regions" in names
    assert "top_regions" not in names and "hidden" not in names


async def test_the_tool_list_follows_the_model(bearer):
    state = _state()
    async with _serving(state) as (_mcp, connect):
        async with connect("?tools=full", token="as-analyst") as session:
            before = await _names(session)
            state.tracked_functions = {"top_regions": _command("top_regions", ["analyst"])}
            registered = await _names(session)
            state.tracked_functions = {}
            retired = await _names(session)
    assert "top_regions" not in before
    assert registered == before | {"top_regions"}
    assert retired == before


async def test_a_sip_session_lists_its_mode_within_its_rights_and_its_commands(bearer):
    state = _state(top_regions=_command("top_regions", ["analyst", "steward"]))
    async with _serving(state) as (mcp, connect):
        async with connect("?tools=sip", token="as-analyst") as session:
            names = await _names(session)
            catalog = _payload(await session.call_tool(tool_groups.CATALOG_TOOL, {}))
        offered = _registered_offered(mcp, set(_ROLES["analyst"]["capabilities"]))
    sip = {t for t in offered if tool_groups.GROUP_OF[t] in tool_groups.SIP_GROUPS}
    assert names == sip | {"top_regions"}
    groups = {g["group"]: g for g in catalog["groups"]}
    # The analyst curates nothing: the groups whose every tool needs a right it lacks are not named.
    assert "metric_management" not in groups and "test_data" not in groups
    assert groups["glossary"]["tools"] == ["list_glossary_findings"]


# -- local: the role is the pinned mcp.role --------------------------------------------------


async def test_a_local_session_is_listed_the_pinned_roles_tools_and_commands(monkeypatch):
    state = _state(
        top_regions=_command("top_regions", ["analyst", "steward"]),
        restock=_command("restock", ["steward"]),
    )
    act_for(monkeypatch, state)
    listed: dict[str, set[str]] = {}
    async with _serving(state) as (mcp, connect):
        for role in ("analyst", "steward"):
            monkeypatch.setenv("PROVISA_MCP_ROLE", role)
            async with connect("?tools=full") as session:
                listed[role] = await _names(session)
        for role in ("analyst", "steward"):
            offered = _registered_offered(mcp, set(_ROLES[role]["capabilities"]))
            commands = {"top_regions"} | ({"restock"} if role == "steward" else set())
            assert listed[role] == offered | commands


async def test_a_local_session_runs_a_command_tool_as_the_pinned_role(monkeypatch):
    from provisa.api.data import action_exec

    ran: list[str] = []

    async def _dispatch(fn: dict, args: dict, _state: Any, role_id: str) -> list[dict]:
        ran.append(role_id)
        return [{"ok": True}]

    monkeypatch.setattr(action_exec, "dispatch_function", _dispatch)
    monkeypatch.setattr(action_exec, "_record_call", lambda *_a: None)
    state = _state(restock=_command("restock", ["steward"]))
    act_for(monkeypatch, state)
    async with _serving(state) as (_mcp, connect):
        monkeypatch.setenv("PROVISA_MCP_ROLE", "analyst")
        async with connect("?tools=full") as session:
            refused = await session.call_tool("restock", {"region": "eu"})
            unoffered = await session.call_tool("find_table_id", {"domain": "d", "table": "t"})
        monkeypatch.setenv("PROVISA_MCP_ROLE", "steward")
        async with connect("?tools=full") as session:
            result = await session.call_tool("restock", {"region": "eu"})
    assert refused.isError and refused.content[0].text == "Unknown tool: restock"
    assert unoffered.content[0].text == "Tool find_table_id is not offered to your roles"
    assert not result.isError, result.content[0].text
    assert ran == ["steward"]


# -- a command's MCP binding may not take a tool's name -------------------------------------


def test_a_command_bound_to_a_tools_name_in_mcp_is_refused():
    from provisa.core.command_bindings import CommandBindingRefused

    clash = _command("lookup", ["analyst"], language_bindings={"mcp": {"name": "run_sql"}})
    with pytest.raises(CommandBindingRefused, match="run_sql"):
        tool_groups.check_command_names([clash])
    tool_groups.check_command_names([_command("lookup", ["analyst"])])
