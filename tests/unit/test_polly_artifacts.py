# Copyright (c) 2026 Kenneth Stott
# Canary: 2d7a9c41-6e83-4f15-b0c2-8a4e1f9d3b76
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What Polly hands the user besides text: a graph drawn in the chat, and a result as a file.

render_graph runs a Cypher read under the role, checked as a Graph page navigate is, and draws
it; the chat panel gets the picture (with its download and its way to the Graph page) and the
model gets only what was drawn. run_sql delivers a result as a file when asked for one, and when
the result has more rows than the cap; the chat panel gets the download.
"""

from __future__ import annotations

import base64
import json
from types import SimpleNamespace

import pytest

from provisa.api.mcp import chat as chat_mod
from provisa.api.mcp import cypher_check
from provisa.api.mcp import tools as mcp_tools
from tests.unit.test_mcp_chat import (  # the chat's fakes, and the key its vendor is configured with
    _block,
    _install_fake_anthropic,
    _state,
    _a_configured_vendor_key,  # noqa: F401  (autouse fixture: the readiness check needs a key)
)

_CYPHER = "MATCH (l:Poems:Line)-[e:EVOKES]->(t:Poems:Theme) RETURN l, e, t LIMIT 50"
_ELEMENTS = {
    "nodes": [{"data": {"id": "Line:1"}}, {"data": {"id": "Theme:1"}}],
    "edges": [{"data": {"id": "e1", "source": "Line:1", "target": "Theme:1"}}],
}
_HANDLE = {
    "sink": "local",
    "redirect_url": "/data/redirect-file/0123456789abcdef0123456789abcdef.csv",
    "row_count": 1540,
    "expires_in": 3600,
    "content_type": "text/csv",
}


def _call(name: str, args: dict):
    return SimpleNamespace(
        content=[_block(type="tool_use", name=name, id="t1", input=args)], stop_reason="tool_use"
    )


def _answer():
    return SimpleNamespace(content=[_block(type="text", text="Done.")], stop_reason="end_turn")


async def _chat(monkeypatch, name: str, args: dict):
    monkeypatch.setenv("PROVISA_MCP_CHAT_MODEL", "claude-haiku-5-5")
    holder = _install_fake_anthropic(monkeypatch, [_call(name, args), _answer()])
    convo = [{"role": "user", "content": "go"}]
    # The user behind the chat: its drawings run in that user's graph session (REQ-2176).
    request = SimpleNamespace(
        state=SimpleNamespace(
            identity=SimpleNamespace(user_id="ada", roles=["analyst"], derived_roles={})
        )
    )
    events = [ev async for ev in chat_mod.run_chat(_state(), "analyst", convo, request=request)]
    # The tool's result as the model got it: the last message of the call after the tool ran.
    return events, holder["client"].messages.calls[1]["messages"][-1]["content"][0]


@pytest.fixture
def drawn(monkeypatch):
    seen: dict = {}

    async def _check(_state, _role, query):
        seen["checked"] = query
        return cypher_check.GraphRead(None, {"l": "node", "e": "edge", "t": "node"})

    async def _run_cypher(_state, role, cypher, params=None, limit=None, *, principal):
        seen["ran"] = (role, cypher)
        seen["principal"] = principal
        return {"row_count": 1, "truncated": False, "elements": _ELEMENTS}

    def _render(elements, layout="force", *_a):
        seen["drawn"] = (elements, layout)
        return b"\x89PNG-bytes"

    monkeypatch.setattr(cypher_check, "check_graph_read", _check)
    monkeypatch.setattr(mcp_tools, "run_cypher", _run_cypher)
    monkeypatch.setattr(mcp_tools, "render_graph", _render)
    return seen


@pytest.mark.asyncio
async def test_a_graph_is_drawn_in_the_chat_and_the_model_gets_only_what_was_drawn(
    monkeypatch, drawn
):
    events, model_result = await _chat(
        monkeypatch, "render_graph", {"cypher": _CYPHER, "layout": "hierarchy"}
    )
    types = [e["type"] for e in events]
    artifact = next(e for e in events if e["type"] == "artifact")
    assert types.index("artifact") < types.index("tool_result")
    assert artifact == {
        "type": "artifact",
        "kind": "graph",
        "png": base64.b64encode(b"\x89PNG-bytes").decode("ascii"),
        "cypher": _CYPHER,
        "nodes": 2,
        "relationships": 1,
    }
    assert drawn["ran"] == ("analyst", _CYPHER)
    assert drawn["principal"] == ("ada", ["analyst"])
    assert drawn["drawn"] == (_ELEMENTS, "hierarchy")
    told = json.loads(model_result["content"])
    assert told["nodes"] == 2 and told["relationships"] == 1
    assert "artifact" not in told and "png" not in model_result["content"]


@pytest.mark.asyncio
async def test_a_graph_that_would_draw_no_edges_is_not_drawn(monkeypatch, drawn):
    async def _check(_state, _role, _query):
        return cypher_check.GraphRead("it returns only nodes", {"l": "node"})

    monkeypatch.setattr(cypher_check, "check_graph_read", _check)
    events, model_result = await _chat(monkeypatch, "render_graph", {"cypher": _CYPHER})
    assert not any(e["type"] == "artifact" for e in events)
    assert model_result["is_error"] is True and "it returns only nodes" in model_result["content"]
    assert "ran" not in drawn


@pytest.fixture
def sql_runs(monkeypatch):
    runs: list[dict] = []

    async def _run_sql(_state, _role, sql, limit=None, offset=0, *, redirect=False, **kw):
        runs.append({"limit": limit, "redirect": redirect, **kw})
        if redirect:
            return {"rows": [], "row_count": 0, "total_rows": 1540, "redirect": _HANDLE}
        page = 1000 if limit is None else limit
        return {
            "columns": ["n"],
            "rows": [{"n": 1}],
            "row_count": page,
            "total_rows": 1540,
            "truncated": True,
        }

    monkeypatch.setattr(mcp_tools, "run_sql", _run_sql)
    return runs


@pytest.mark.asyncio
async def test_a_result_asked_for_as_a_file_is_a_download_in_the_chat(monkeypatch, sql_runs):
    events, model_result = await _chat(
        monkeypatch, "run_sql", {"sql": "SELECT n FROM t", "file_format": "parquet"}
    )
    # The rows for the model, then the file for the user.
    assert sql_runs == [
        {"limit": None, "redirect": False},
        {"limit": None, "redirect": True, "redirect_format": "parquet"},
    ]
    artifact = next(e for e in events if e["type"] == "artifact")
    assert artifact == {"type": "artifact", "kind": "file", **_HANDLE}
    told = json.loads(model_result["content"])
    assert told["redirect"] == _HANDLE and "artifact" not in told
    # A file alone would tell the model only how many rows it holds, which GPT-5.4 mini read as
    # its answer ("the shop has 1 customer"): it gets the rows too.
    assert told["rows"] == [{"n": 1}]


@pytest.mark.asyncio
async def test_a_result_too_large_for_the_chat_is_also_delivered_as_a_file(monkeypatch, sql_runs):
    events, model_result = await _chat(monkeypatch, "run_sql", {"sql": "SELECT n FROM t"})
    assert [r["redirect"] for r in sql_runs] == [False, True]
    assert sql_runs[1]["redirect_format"] == "csv"
    assert (
        next(e for e in events if e["type"] == "artifact")["redirect_url"]
        == (_HANDLE["redirect_url"])
    )
    told = json.loads(model_result["content"])
    assert told["rows"] == [{"n": 1}] and told["redirect"] == _HANDLE


@pytest.mark.asyncio
async def test_a_page_the_model_asked_for_is_not_delivered_as_a_file(monkeypatch, sql_runs):
    events, _ = await _chat(monkeypatch, "run_sql", {"sql": "SELECT n FROM t", "limit": 10})
    assert [r["redirect"] for r in sql_runs] == [False]
    assert not any(e["type"] == "artifact" for e in events)


def test_the_prompt_draws_a_graph_rather_than_asking_where():
    assert "never ask whether the user meant the chat or the Graph page" in chat_mod._SYSTEM
    assert "call run_sql with file_format" in chat_mod._SYSTEM
