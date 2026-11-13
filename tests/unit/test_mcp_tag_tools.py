# Copyright (c) 2026 Kenneth Stott
# Canary: 6c2e9a41-7d3b-4f58-a0e6-93b1d4c7f852
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The tag edits as agent tools: assign, unassign, cite and uncite, on an assignment or on a
tag's definition.

Over MCP each tool calls the admin mutation the tag forms call, with the CALLER's request, and
returns the mutation's own result unchanged; a caller without the right gets the mutation's own
refusal. In Polly's chat the same tool is never run on the server: it is handed to the browser as
the admin_mutation it stands for, which the person confirms before it is sent.
"""

# Requirements: REQ-2208

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from provisa.api.admin import schema_mutation
from provisa.api.admin.schema_mutation import Mutation
from provisa.api.admin.types import MutationResult
from provisa.api.mcp import chat, model_tool_specs, model_tools, tag_edit

_ROLES = {
    "viewer": {"capabilities": ["query_development"]},
    "steward": {"capabilities": ["table_registration"]},
}


@pytest.fixture
def app_state(monkeypatch):
    from provisa.api.app import state

    monkeypatch.setattr(state, "roles", _ROLES, raising=False)
    monkeypatch.setattr(state, "contexts", {"viewer": object(), "steward": object()}, raising=False)
    return state


def _request(role: str):
    identity = SimpleNamespace(user_id="u1", roles=[role], acting_roles=[role])
    return SimpleNamespace(
        headers={}, state=SimpleNamespace(identity=identity, active_org_id="acme", role=role)
    )


_CITATION = {"document_id": 7, "section_heading": "Retention"}

# Each tool, the arguments it is called with, the mutation it calls, and what that mutation is
# handed: (assignment-or-tag input, citations-or-citation).
_CALLS: list[tuple[str, dict, str]] = [
    (
        "assign_tag",
        {
            "object_kind": "column",
            "object_key": "column:12:email",
            "tag_id": "pii",
            "reason": "holds addresses",
            "expires_on": "2027-01-31",
            "citations": [_CITATION],
        },
        "assign_tag",
    ),
    (
        "unassign_tag",
        {"object_kind": "column", "object_key": "column:12:email", "tag_id": "pii"},
        "unassign_tag",
    ),
    (
        "cite_tag_assignment",
        {
            "object_kind": "column",
            "object_key": "column:12:email",
            "tag_id": "pii",
            "citation": _CITATION,
        },
        "cite_tag_assignment",
    ),
    (
        "uncite_tag_assignment",
        {
            "object_kind": "column",
            "object_key": "column:12:email",
            "tag_id": "pii",
            "citation": _CITATION,
        },
        "uncite_tag_assignment",
    ),
    ("cite_tag_definition", {"tag_id": "pii", "citation": _CITATION}, "cite_tag_definition"),
    ("uncite_tag_definition", {"tag_id": "pii", "citation": _CITATION}, "uncite_tag_definition"),
]
_NAMES = [c[0] for c in _CALLS]


def test_every_tag_tool_is_declared_and_has_a_function():
    assert set(_NAMES) == model_tool_specs.TAG_NAMES
    for name in model_tool_specs.TAG_NAMES:
        assert callable(getattr(model_tools, name))


@pytest.mark.parametrize(("name", "args", "mutation"), _CALLS, ids=_NAMES)
async def test_the_tool_calls_the_mutation_as_the_caller_and_returns_its_result(
    monkeypatch, app_state, name, args, mutation
):
    seen: dict[str, Any] = {}
    answer = MutationResult(
        success=False, message="as the mutation said", code="schema.some_code", params={"a": 1}
    )

    async def fake(self, info, *positional, **named):
        seen["request"] = info.context["request"]
        seen["args"] = positional
        seen["named"] = named
        return answer

    monkeypatch.setattr(Mutation, mutation, fake)
    caller = _request("steward")
    got = await model_tools.__dict__[name](app_state, "steward", caller, **args)
    assert seen["request"] is caller
    assert got == {
        "success": False,
        "message": "as the mutation said",
        "code": "schema.some_code",
        "params": {"a": 1},
    }


async def test_assign_tag_hands_the_mutation_the_whole_assignment(monkeypatch, app_state):
    seen: dict[str, Any] = {}

    async def fake(self, info, input, citations=None):
        seen["input"] = input
        seen["citations"] = citations
        return MutationResult(success=True, message="Tag 'pii' assigned")

    monkeypatch.setattr(Mutation, "assign_tag", fake)
    await model_tools.assign_tag(app_state, "steward", _request("steward"), **_CALLS[0][1])
    given = seen["input"]
    assert (given.tag_id, given.object_type, given.table_id, given.column_name) == (
        "pii",
        "column",
        12,
        "email",
    )
    assert (given.reason, given.expires_on) == ("holds addresses", "2027-01-31")
    [cited] = seen["citations"]
    assert (cited.document_id, cited.section_heading, cited.section_page) == (7, "Retention", None)


@pytest.mark.parametrize(
    ("kind", "key", "fields"),
    [
        ("source", "source:pg:main", {"source_id": "pg:main"}),
        ("table", "table:3", {"table_id": 3}),
        ("column", "column:3:a:b", {"table_id": 3, "column_name": "a:b"}),
        ("relationship", "relationship:r1", {"relationship_id": "r1"}),
        ("command", "command:refund", {"command_name": "refund"}),
        ("product", "product:p1", {"product_id": "p1"}),
        ("term", "term:9", {"term_id": 9}),
        ("rls_rule", "rls_rule:4", {"rls_rule_id": 4}),
        ("role", "role:analyst", {"role_id": "analyst"}),
        ("metric", "metric:revenue", {"metric_name": "revenue"}),
        ("document", "document:7", {"document_id": 7}),
        (
            "section",
            "section:7:heading:Retention: data",
            {"document_id": 7, "section_heading": "Retention: data"},
        ),
        ("section", "section:7:page:3", {"document_id": 7, "section_page": 3}),
    ],
)
def test_an_object_key_names_its_object_as_the_store_keys_it(kind, key, fields):
    from provisa.core.models import TagAssignment

    assert tag_edit.target(kind, key) == fields
    assert TagAssignment(tag_id="t", object_type=kind, **fields).object_key() == key


@pytest.mark.parametrize(
    ("kind", "key"),
    [
        ("table", "column:3:a"),
        ("table", "table:x"),
        ("section", "section:7:line:2"),
        ("cat", "cat:1"),
    ],
)
def test_a_key_that_names_no_object_of_its_kind_is_refused(kind, key):
    with pytest.raises(ValueError, match="object_key"):
        tag_edit.target(kind, key)


# -- a caller without the right: the mutation's own refusal -------------------------------------


class _Pool:
    def acquire(self):
        class _Ctx:
            async def __aenter__(self):
                return object()

            async def __aexit__(self, *exc):
                return False

        return _Ctx()


@pytest.mark.parametrize(("name", "args", "mutation"), _CALLS, ids=_NAMES)
async def test_a_caller_without_the_right_gets_the_mutations_refusal(
    monkeypatch, app_state, name, args, mutation
):
    from provisa.core.repositories import tag as tag_repo

    async def pool():
        return _Pool()

    async def a_tag(conn, tag_id):
        return {"id": tag_id, "sensitive": False}

    # Only what the refusal is reached through: the tag a citation change reads first.
    monkeypatch.setattr(schema_mutation, "_get_pool", pool)
    monkeypatch.setattr(tag_repo, "get", a_tag)
    with pytest.raises(PermissionError, match="Missing capability: 'table_registration'"):
        await model_tools.__dict__[name](app_state, "viewer", _request("viewer"), **args)


# -- the MCP server and Polly's chat -------------------------------------------------------------


def test_the_mcp_server_registers_every_tag_tool(monkeypatch, app_state):
    import asyncio

    from provisa.api.mcp.server import build_mcp_server

    monkeypatch.setenv("PROVISA_MCP_ROLE", "steward")
    mcp = build_mcp_server(app_state)
    names = {t.name for t in asyncio.run(mcp.list_tools())}
    assert model_tool_specs.TAG_NAMES <= names


def test_polly_is_offered_every_tag_tool():
    offered = {t["name"] for t in [*chat._TOOLS, *chat._CLIENT_TOOLS]}
    assert model_tool_specs.TAG_NAMES <= offered


@pytest.mark.parametrize(("name", "args", "mutation"), _CALLS, ids=_NAMES)
def test_in_pollys_chat_the_edit_waits_for_the_persons_confirmation(name, args, mutation):
    """The chat pauses on the call and hands the browser the admin_mutation it stands for: the
    browser shows it with its summary and sends it, from the person's own session, only on their
    yes (clientTools.ts admin_mutation)."""
    call = {"id": "t1", "name": name, "input": args}
    client, server = chat._split_tool_calls([call])
    assert (client, server) == ([call], [])
    handed, _note = chat._prepare_client_call(call)
    assert handed["id"] == "t1"
    assert handed["name"] == "admin_mutation"
    camel = "".join(p.capitalize() if i else p for i, p in enumerate(mutation.split("_")))
    assert f"{camel}(" in handed["input"]["mutation"]
    assert "success" in handed["input"]["mutation"] and "code" in handed["input"]["mutation"]
    assert handed["input"]["summary"]
    assert "REQ-" not in handed["input"]["summary"]


def test_assign_tag_in_chat_is_the_forms_own_mutation():
    handed, _ = chat._prepare_client_call({"id": "t1", "name": "assign_tag", "input": _CALLS[0][1]})
    assert handed["input"]["variables"] == {
        "input": {
            "tagId": "pii",
            "objectType": "column",
            "tableId": 12,
            "columnName": "email",
            "reason": "holds addresses",
            "expiresOn": "2027-01-31",
        },
        "citations": [{"documentId": 7, "sectionHeading": "Retention"}],
    }


async def test_the_chat_never_writes_a_tag_edit_itself(monkeypatch, app_state):
    async def must_not(*a, **k):
        raise AssertionError("written without the person's confirmation")

    monkeypatch.setattr(Mutation, "assign_tag", must_not)
    with pytest.raises(ValueError, match="confirm"):
        await chat._execute_tool(
            app_state, "steward", "assign_tag", _CALLS[0][1], request=_request("steward")
        )


def test_no_tag_tool_text_names_a_requirement():
    for spec in model_tool_specs.TAG_SPECS:
        assert "REQ-" not in spec["description"]
    assert "REQ-" not in model_tool_specs.TAG_SYSTEM_SECTION
