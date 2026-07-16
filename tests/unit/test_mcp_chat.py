# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""MCP chat agent — the LLM chatbot that drives the governed MCP tools (REQ-1008).

The Anthropic client is faked with a scripted response sequence, so the tool-use
loop is exercised offline with no model call.
"""

from types import SimpleNamespace

import pytest

from provisa.api.mcp import chat as chat_mod
from provisa.api.mcp import tools as mcp_tools


def _block(**kw):
    return SimpleNamespace(**kw)


class _FakeMessages:
    def __init__(self, responses):
        self._responses = list(responses)
        self.calls = []

    async def create(self, **kwargs):
        self.calls.append(kwargs)
        return self._responses.pop(0)


class _FakeClient:
    def __init__(self, responses):
        self.messages = _FakeMessages(responses)


def _install_fake_anthropic(monkeypatch, responses):
    holder = {}

    def _factory(*_a, **_k):
        client = _FakeClient(responses)
        holder["client"] = client
        return client

    import anthropic

    monkeypatch.setattr(anthropic, "AsyncAnthropic", _factory)
    return holder


def _state():
    ctx = SimpleNamespace(tables={}, joins={})
    return SimpleNamespace(
        contexts={"analyst": ctx},
        config=SimpleNamespace(roles=[SimpleNamespace(id="analyst", domain_access=["sales"])]),
    )


@pytest.mark.asyncio
class TestChatLoop:
    async def test_tool_use_then_answer(self, monkeypatch):
        # Model turn 1: call search_catalog. Turn 2: final text answer.
        resp1 = SimpleNamespace(
            content=[
                _block(
                    type="tool_use",
                    name="search_catalog",
                    input={"query": "customer email"},
                    id="tu1",
                )
            ],
            stop_reason="tool_use",
        )
        resp2 = SimpleNamespace(
            content=[_block(type="text", text="The customers table has an email column.")],
            stop_reason="end_turn",
        )
        _install_fake_anthropic(monkeypatch, [resp1, resp2])

        seen = {}

        async def fake_search(state, role, query, k=5):
            seen["role"] = role
            seen["query"] = query
            return [{"schema": "sales", "table": "customers"}]

        monkeypatch.setattr(mcp_tools, "search_catalog", fake_search)

        events = [
            ev
            async for ev in chat_mod.run_chat(
                _state(), "analyst", [{"role": "user", "content": "where is customer email?"}]
            )
        ]
        types = [e["type"] for e in events]
        assert types == ["tool_use", "tool_result", "text", "done"]
        assert events[0]["name"] == "search_catalog"
        assert events[1]["is_error"] is False
        assert "email column" in events[2]["text"]
        # The tool ran under the pinned role, with the model-chosen query.
        assert seen == {"role": "analyst", "query": "customer email"}

    async def test_tool_error_reported_not_raised(self, monkeypatch):
        resp1 = SimpleNamespace(
            content=[
                _block(type="tool_use", name="list_tables", input={"schema": "ghost"}, id="t1")
            ],
            stop_reason="tool_use",
        )
        resp2 = SimpleNamespace(
            content=[_block(type="text", text="That schema does not exist.")],
            stop_reason="end_turn",
        )
        _install_fake_anthropic(monkeypatch, [resp1, resp2])

        async def fake_list_tables(state, role, schema):
            raise ValueError("Unknown schema 'ghost'")

        monkeypatch.setattr(mcp_tools, "list_tables", fake_list_tables)

        events = [
            ev
            async for ev in chat_mod.run_chat(
                _state(), "analyst", [{"role": "user", "content": "list ghost"}]
            )
        ]
        # The tool error is surfaced as an is_error tool_result, not raised — chat continues.
        tr = next(e for e in events if e["type"] == "tool_result")
        assert tr["is_error"] is True
        assert events[-1]["type"] == "done"

    async def test_unknown_role_fails_before_any_model_call(self, monkeypatch):
        holder = _install_fake_anthropic(monkeypatch, [])
        with pytest.raises(PermissionError):
            async for _ in chat_mod.run_chat(
                _state(), "intruder", [{"role": "user", "content": "hi"}]
            ):
                pass
        assert "client" not in holder  # never constructed the Anthropic client


class TestModelResolution:
    def test_env_override_wins(self, monkeypatch):
        monkeypatch.setenv("PROVISA_MCP_CHAT_MODEL", "claude-sonnet-5")
        assert chat_mod._resolve_model() == "claude-sonnet-5"

    def test_defaults_to_opus(self, monkeypatch, tmp_path):
        import os

        monkeypatch.delenv("PROVISA_MCP_CHAT_MODEL", raising=False)
        # Point config at an empty file so there's no ai_models.mcp_chat entry → default.
        cfg = tmp_path / "provisa.yaml"
        cfg.write_text("sources: []\n")
        monkeypatch.setitem(os.environ, "PROVISA_CONFIG", str(cfg))
        assert chat_mod._resolve_model() == "claude-opus-4-8"
