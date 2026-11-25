# Copyright (c) 2026 Kenneth Stott
# Canary: 09815e30-77a5-4344-b476-4cd1c480b292
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2114 (amended 2026-11-24): Polly's source_types tool and the rule that she never takes a
secret in the chat."""

from __future__ import annotations

import json

import pytest

from provisa.api.admin import source_types as st
from provisa.api.mcp import chat as chat_mod


def _flat(text: str) -> str:
    return " ".join(text.split())


def _tool() -> dict:
    return next(t for t in chat_mod._TOOLS if t["name"] == "source_types")


class TestTheTool:
    def test_source_types_is_a_server_tool(self):
        tool = _tool()
        assert tool["input_schema"]["properties"]["type"]["type"] == "string"
        assert "required" not in tool["input_schema"]
        assert "source_types" not in chat_mod._CLIENT_TOOL_NAMES

    async def test_with_no_type_it_lists_every_offered_type(self):
        result = await chat_mod._dispatch_tool(None, "admin", "source_types", {}, scope=None)
        names = [t["name"] for t in result["types"]]
        assert names == [d.name for d in st.source_types()]
        github = next(t for t in result["types"] if t["name"] == "github")
        assert github["brand"] == "github"
        assert github["backend_type"] == "graphql_remote"
        assert result["secret_reference"] == "${secret:NAME}"
        json.dumps(result)

    async def test_with_a_type_it_returns_that_type_in_full(self):
        result = await chat_mod._dispatch_tool(
            None, "admin", "source_types", {"type": "postgresql"}, scope=None
        )
        assert result["create"] == {"via": "admin_mutation", "mutation": "createSource"}
        password = next(f for f in result["fields"] if f["name"] == "password")
        assert password == {"name": "password", "at": "input", "secret": True}
        json.dumps(result)

    async def test_a_backend_type_finds_its_picker_type(self):
        result = await chat_mod._dispatch_tool(
            None, "admin", "source_types", {"type": "grpc_remote"}, scope=None
        )
        assert result["name"] == "grpc"

    async def test_an_unknown_type_is_refused_by_name(self):
        with pytest.raises(KeyError, match="nosuchtype"):
            await chat_mod._dispatch_tool(
                None, "admin", "source_types", {"type": "nosuchtype"}, scope=None
            )


class TestThePromptRule:
    def test_polly_reads_the_registry_not_her_memory(self):
        s = _flat(chat_mod._SYSTEM)
        assert "read with source_types -- never from memory and never from this prompt" in s
        assert "call source_types with its type and build the change from what it returns" in s

    def test_sources_are_created_with_the_descriptors_shape(self):
        s = _flat(chat_mod._SYSTEM)
        assert "create through admin_mutation createSource with the descriptor's shape" in s
        assert "through admin_request to that route with the descriptor's body" in s

    def test_a_secret_is_never_asked_for_or_taken_in_the_chat(self):
        s = _flat(chat_mod._SYSTEM)
        assert "NEVER ask for a secret value in the chat" in s
        assert "is set to a ${secret:NAME} reference" in s
        assert (
            "the user enters the value itself in the Sources form or in the organization's "
            "secrets" in s
        )
        assert "If the user types a credential into the chat anyway, do not use it" in s

    def test_no_hand_kept_type_list_in_the_prompt(self):
        """The prompt names no type list of its own: a brand or stub only the registry knows is
        absent from it."""
        s = chat_mod._SYSTEM
        for name in ("openmetadata", "datahub", "hiveserver2", "cockroachdb", "greenplum"):
            assert name not in s, name
