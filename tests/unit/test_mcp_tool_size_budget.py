# Copyright (c) 2026 Kenneth Stott
# Canary: 3b8e6f20-5c1a-4d97-b2e4-91f0a7c6d835
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The MCP server's tool definitions stay within a size budget.

A client is sent every tool's name, description and schemas on every model call, so their size is
paid on every turn. Measured as ``scripts/mcp_tool_size.py`` measures it.
"""

from __future__ import annotations

from scripts.mcp_tool_size import estimate_tokens, mcp_tool_sizes

# Total serialized characters of tools/list. Set a little above the measured size (43,777 chars,
# 88 tools, 2026-11-13). Raise it only deliberately, for a tool or text worth its cost.
BUDGET_CHARS = 45_000


async def test_tool_definitions_fit_the_budget(monkeypatch):
    # jev_evaluate is registered only with a Jev key; the budget is for the default tool list.
    monkeypatch.delenv("TYPESAFEAI_API_KEY", raising=False)
    sizes = await mcp_tool_sizes()
    total = sum(sizes.values())
    largest = sorted(sizes.items(), key=lambda kv: -kv[1])[:5]
    assert total <= BUDGET_CHARS, (
        f"The MCP tool definitions are {total:,} chars (~{estimate_tokens(total):,} tokens), over "
        f"the {BUDGET_CHARS:,} budget; every client pays this on every model call. Condense "
        f"descriptions to what a model needs to pick and call the tool (see "
        f"scripts/mcp_tool_size.py for per-tool sizes; largest: {largest}), or raise "
        f"BUDGET_CHARS deliberately in {__file__}."
    )


async def test_no_schema_carries_a_title(monkeypatch):
    monkeypatch.delenv("TYPESAFEAI_API_KEY", raising=False)
    from types import SimpleNamespace

    from provisa.api.mcp.server import build_mcp_server

    for tool in await build_mcp_server(SimpleNamespace()).list_tools():
        assert '"title"' not in tool.model_dump_json(by_alias=True, exclude_none=True), tool.name


def test_drop_titles_keeps_a_property_named_title():
    from provisa.api.mcp.server import _drop_titles

    schema = {
        "title": "fArguments",
        "type": "object",
        "properties": {
            "title": {"title": "Title", "type": "string", "default": {"title": "kept"}},
            "items": {"anyOf": [{"title": "X", "type": "integer"}, {"type": "null"}]},
        },
        "$defs": {"title": {"title": "T", "type": "object"}},
    }
    _drop_titles(schema)
    assert schema == {
        "type": "object",
        "properties": {
            "title": {"type": "string", "default": {"title": "kept"}},
            "items": {"anyOf": [{"type": "integer"}, {"type": "null"}]},
        },
        "$defs": {"title": {"type": "object"}},
    }
