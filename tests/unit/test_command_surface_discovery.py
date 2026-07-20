# Copyright (c) 2026 Kenneth Stott
# Canary: 5f8c1a92-6d47-4e13-8b25-9c0e7a4f31d6
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1156: registered commands are DISCOVERABLE (not only invocable) on every client surface.

The invocation half (route through the one ``invoke_tracked_function`` executor) is covered per
surface elsewhere. This module covers the discovery half — the command-listing each surface
projects — plus the shared ``list_visible_commands`` projection they all build on.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from provisa.api.data.action_exec import list_visible_commands


def _state():
    query_set = {
        "name": "active_users",
        "description": "current active users",
        "domain_id": "pet-store",
        "kind": "query",
        "returns": "pet-store.users",  # set-returning / table-valued
        "visible_to": [],  # empty = every role
        "arguments": [{"name": "since", "type": "Int"}],
    }
    mutation_scalar = {
        "name": "reset_cache",
        "description": "flush the cache",
        "domain_id": "ops",
        "kind": "mutation",
        "visible_to": ["admin"],
        "arguments": [],
    }
    hidden = {
        "name": "secret_cmd",
        "domain_id": "ops",
        "kind": "mutation",
        "visible_to": ["root"],
        "arguments": [],
    }
    return SimpleNamespace(
        tracked_functions={
            "active_users": query_set,
            "ps__active_users": query_set,  # domain-prefixed alias (mirrors app_loaders)
            "reset_cache": mutation_scalar,
            "secret_cmd": hidden,
        },
        roles={"admin": {"domain_access": ["*"]}},
    )


# ---------------------------------------------------------------- shared projection


def test_list_visible_commands_dedupes_and_filters():
    out = list_visible_commands(_state(), "admin")
    names = [c["name"] for c in out]
    assert names.count("active_users") == 1  # prefixed alias collapsed
    assert "reset_cache" in names  # visible_to contains admin
    assert "secret_cmd" not in names  # visible_to excludes admin


def test_list_visible_commands_role_agnostic_view_lists_all():
    # role_id None = broadest catalog view (Flight): every command, incl. role-restricted ones
    names = {c["name"] for c in list_visible_commands(_state(), None)}
    assert names == {"active_users", "reset_cache", "secret_cmd"}


def test_list_visible_commands_projects_kind_and_set_returning():
    by_name = {c["name"]: c for c in list_visible_commands(_state(), "admin")}
    assert by_name["active_users"]["kind"] == "query"
    assert by_name["active_users"]["set_returning"] is True
    assert by_name["active_users"]["arguments"] == [{"name": "since", "type": "Int"}]
    assert by_name["reset_cache"]["kind"] == "mutation"
    assert by_name["reset_cache"]["set_returning"] is False


def test_public_role_sees_only_unrestricted_commands():
    names = {c["name"] for c in list_visible_commands(_state(), "guest")}
    assert names == {"active_users"}  # only the empty-visible_to command


# ---------------------------------------------------------------- MCP


def test_mcp_list_commands_tool():
    from provisa.api.mcp import tools

    state = _state()
    state.contexts = {"admin": object()}  # require_role checks membership
    out = tools.list_commands(state, "admin")
    names = {c["name"] for c in out}
    assert names == {"active_users", "reset_cache"}


# ---------------------------------------------------------------- Arrow Flight


def test_flight_command_flight_info_descriptor_and_schema():
    from provisa.api.flight.catalog import command_to_flight_info

    cmd = next(c for c in list_visible_commands(_state(), None) if c["name"] == "active_users")
    info = command_to_flight_info(cmd)
    path = [p.decode() if isinstance(p, bytes) else p for p in info.descriptor.path]
    assert path == ["commands", cmd["domain"], cmd["name"]]
    # one Arrow field per declared argument + command metadata on the schema
    assert [f.name for f in info.schema] == ["since"]
    assert info.schema.metadata[b"command"] == cmd["name"].encode()
    assert info.schema.metadata[b"kind"] == cmd["kind"].encode()


# ---------------------------------------------------------------- gRPC proto


def _schema_input():
    fns = list(_state().tracked_functions.values())
    return SimpleNamespace(
        role={"id": "admin", "domain_access": ["*"]},
        functions=fns,
    )


def test_command_rpc_name_roundtrip():
    from provisa.grpc.proto_gen import command_rpc_name

    assert command_rpc_name("active_users") == "ActiveUsers"
    assert command_rpc_name("reset_cache") == "ResetCache"


def test_proto_gen_emits_per_command_rpcs():
    from provisa.grpc.proto_gen import _visible_commands

    si = _schema_input()
    cmds = {c["name"] for c in _visible_commands(si)}
    assert cmds == {"active_users", "reset_cache"}  # secret_cmd hidden from admin


def test_proto_gen_visible_commands_typed_request_fields():
    from provisa.grpc.proto_gen import _arg_proto_type

    assert _arg_proto_type("Int") == "int64"
    assert _arg_proto_type("Float") == "double"
    assert _arg_proto_type("Boolean") == "bool"
    assert _arg_proto_type("String") == "string"
    assert _arg_proto_type("SomethingElse") == "string"


def test_grpc_servicer_resolves_call_rpc_to_command_name():
    from provisa.grpc.server import ProvisaServicer

    servicer = ProvisaServicer(_state(), pb2_module=object(), pb2_grpc_module=object())
    assert servicer._resolve_command_rpc("ActiveUsers") == "active_users"
    assert servicer._resolve_command_rpc("ResetCache") == "reset_cache"
    assert servicer._resolve_command_rpc("DoesNotExist") is None


# ---------------------------------------------------------------- Cypher / Bolt


def test_bolt_command_signature():
    from provisa.bolt.session import _command_signature

    cmd = next(c for c in list_visible_commands(_state(), None) if c["name"] == "active_users")
    sig = _command_signature(cmd)
    assert sig == "active_users(since :: INT) :: (LIST OF MAP)"


def test_bolt_show_procedures_lists_commands():
    from provisa.bolt.session import _system_query

    state = _state()
    cols, rows = _system_query("SHOW PROCEDURES", None, "admin", True, state, None)
    assert cols == ["name", "description", "signature"]
    names = {r[0] for r in rows}
    assert names == {"active_users", "reset_cache"}


def test_http_cypher_commands_procedure_regex():
    from provisa.api.rest.registered_call import _COMMANDS_PROC_RE

    assert _COMMANDS_PROC_RE.match("CALL dbms.procedures()")
    assert _COMMANDS_PROC_RE.match("CALL provisa.commands() YIELD name")
    assert not _COMMANDS_PROC_RE.match("CALL active_users(1)")


@pytest.mark.asyncio
async def test_http_cypher_intercept_lists_commands():
    import json

    from provisa.api.rest.registered_call import intercept_precompile

    body = SimpleNamespace(query="CALL dbms.procedures()", params={})
    resp = await intercept_precompile(body, _state(), "admin", label_map=None)
    payload = json.loads(bytes(resp.body))
    assert payload["columns"] == ["name", "description", "signature"]
    assert {r["name"] for r in payload["rows"]} == {"active_users", "reset_cache"}
