# Copyright (c) 2026 Kenneth Stott
# Canary: 4d174826-df75-45e5-a625-1620f07d1a41
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
    # `.model_dump()` mirrors the real Anthropic SDK content-block objects chat.py relies on to
    # serialize `resp.content` for the awaiting_client_tools event (REQ-1795).
    return SimpleNamespace(model_dump=lambda: dict(kw), **kw)


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


class _FakeAisuiteCompletions:
    def __init__(self, responses):
        self._responses = list(responses)
        self.calls = []

    def create(self, **kwargs):
        self.calls.append(kwargs)
        return self._responses.pop(0)


class _FakeAisuiteChat:
    def __init__(self, responses):
        self.completions = _FakeAisuiteCompletions(responses)


class _FakeAisuiteClient:
    def __init__(self, responses):
        self.chat = _FakeAisuiteChat(responses)


def _install_fake_aisuite(monkeypatch, responses):
    """REQ-1797: aisuite is sync (run via asyncio.to_thread), so its fake needs no `async def`."""
    holder = {}

    def _factory(*_a, **_k):
        client = _FakeAisuiteClient(responses)
        holder["client"] = client
        return client

    import aisuite

    monkeypatch.setattr(aisuite, "Client", _factory)
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

    async def test_propose_source_dispatches_to_mcp_tool(self, monkeypatch):  # REQ-1792/1795
        resp1 = SimpleNamespace(
            content=[
                _block(
                    type="tool_use",
                    name="propose_source",
                    input={
                        "source": {"id": "fred_cpi", "type": "postgresql"},
                        "reason": "found via web search for US inflation data",
                    },
                    id="p1",
                )
            ],
            stop_reason="tool_use",
        )
        resp2 = SimpleNamespace(
            content=[_block(type="text", text="Proposed — a rights-holder can approve it.")],
            stop_reason="end_turn",
        )
        _install_fake_anthropic(monkeypatch, [resp1, resp2])

        seen = {}

        async def fake_propose_source(state, role, source, reason, *, request=None):
            seen.update(role=role, source=source, reason=reason)
            return {"request_id": 7, "status": "pending", "message": "Queued as #7"}

        monkeypatch.setattr(mcp_tools, "propose_source", fake_propose_source)

        events = [
            ev
            async for ev in chat_mod.run_chat(
                _state(), "analyst", [{"role": "user", "content": "find a US inflation source"}]
            )
        ]
        types = [e["type"] for e in events]
        assert types == ["tool_use", "tool_result", "text", "done"]
        assert events[1]["is_error"] is False
        # Ran under the pinned role, with the model's proposed source/reason passed through.
        assert seen["role"] == "analyst"
        assert seen["source"] == {"id": "fred_cpi", "type": "postgresql"}
        assert "inflation" in seen["reason"]

    async def test_client_tool_pauses_the_loop(self, monkeypatch):  # REQ-1795
        resp1 = SimpleNamespace(
            content=[
                _block(type="tool_use", name="navigate", input={"route": "/sources"}, id="c1")
            ],
            stop_reason="tool_use",
        )
        holder = _install_fake_anthropic(monkeypatch, [resp1])

        events = [
            ev
            async for ev in chat_mod.run_chat(
                _state(), "analyst", [{"role": "user", "content": "go to sources"}]
            )
        ]
        types = [e["type"] for e in events]
        assert types == ["tool_use", "awaiting_client_tools", "done"]
        assert events[0]["client"] is True
        pending = events[1]["pending"]
        assert pending == [{"id": "c1", "name": "navigate", "input": {"route": "/sources"}}]
        assert events[1]["server_tool_results"] == []
        # The assistant content is serialized (JSON-safe), not the raw SDK block object.
        assert isinstance(events[1]["assistant_content"], list)
        assert events[1]["assistant_content"][0]["name"] == "navigate"
        # Only one model call happened — the loop stopped rather than iterating further.
        assert len(holder["client"].messages.calls) == 1

    async def test_mixed_turn_executes_server_tools_before_pausing(self, monkeypatch):  # REQ-1795
        resp1 = SimpleNamespace(
            content=[
                _block(type="tool_use", name="search_catalog", input={"query": "orders"}, id="s1"),
                _block(type="tool_use", name="navigate", input={"route": "/tables"}, id="c1"),
            ],
            stop_reason="tool_use",
        )
        _install_fake_anthropic(monkeypatch, [resp1])

        async def fake_search(state, role, query, k=5):
            return [{"schema": "sales", "table": "orders"}]

        monkeypatch.setattr(mcp_tools, "search_catalog", fake_search)

        events = [
            ev
            async for ev in chat_mod.run_chat(
                _state(), "analyst", [{"role": "user", "content": "find orders, go to tables"}]
            )
        ]
        types = [e["type"] for e in events]
        # The server tool ran and reported its result BEFORE the pause.
        assert types == ["tool_use", "tool_result", "tool_use", "awaiting_client_tools", "done"]
        awaiting = next(e for e in events if e["type"] == "awaiting_client_tools")
        assert len(awaiting["server_tool_results"]) == 1
        assert awaiting["server_tool_results"][0]["tool_use_id"] == "s1"
        assert awaiting["pending"] == [
            {"id": "c1", "name": "navigate", "input": {"route": "/tables"}}
        ]

    async def test_unknown_role_fails_before_any_model_call(self, monkeypatch):
        holder = _install_fake_anthropic(monkeypatch, [])
        with pytest.raises(PermissionError):
            async for _ in chat_mod.run_chat(
                _state(), "intruder", [{"role": "user", "content": "hi"}]
            ):
                pass
        assert "client" not in holder  # never constructed the Anthropic client


class TestLlmConfiguredPreflight:  # REQ-1794
    async def test_no_key_no_endpoint_is_unconfigured(self, monkeypatch, tmp_path):
        import os

        monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
        cfg = tmp_path / "provisa.yaml"
        cfg.write_text("sources: []\n")
        monkeypatch.setitem(os.environ, "PROVISA_CONFIG", str(cfg))
        ok, reason = await chat_mod._llm_configured(_state())
        assert ok is False
        assert "ANTHROPIC_API_KEY" in reason

    async def test_key_present_is_configured(self, monkeypatch, tmp_path):
        import os

        monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test-123")
        cfg = tmp_path / "provisa.yaml"
        cfg.write_text("sources: []\n")
        monkeypatch.setitem(os.environ, "PROVISA_CONFIG", str(cfg))
        ok, reason = await chat_mod._llm_configured(_state())
        assert ok is True
        assert reason == ""

    async def test_custom_endpoint_missing_its_key_env_is_unconfigured(self, monkeypatch, tmp_path):
        import os

        monkeypatch.delenv("MY_GATEWAY_KEY", raising=False)
        cfg = tmp_path / "provisa.yaml"
        cfg.write_text(
            "sources: []\n"
            "ai_models:\n"
            "  mcp_chat: {vendor: my-gateway, model: some-model}\n"
            "ai_endpoints:\n"
            "  - id: my-gateway\n"
            "    style: openai\n"
            "    base_url: https://gateway.internal/v1\n"
            "    api_key_env: MY_GATEWAY_KEY\n"
            "    enabled: true\n"
        )
        monkeypatch.setitem(os.environ, "PROVISA_CONFIG", str(cfg))
        ok, reason = await chat_mod._llm_configured(_state())
        assert ok is False
        assert "MY_GATEWAY_KEY" in reason

    async def test_run_chat_short_circuits_before_any_model_call(self, monkeypatch, tmp_path):
        import os

        monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
        cfg = tmp_path / "provisa.yaml"
        cfg.write_text("sources: []\n")
        monkeypatch.setitem(os.environ, "PROVISA_CONFIG", str(cfg))
        holder = _install_fake_anthropic(monkeypatch, [])

        events = [
            ev
            async for ev in chat_mod.run_chat(
                _state(), "analyst", [{"role": "user", "content": "hi"}]
            )
        ]
        assert "client" not in holder  # never constructed the Anthropic client
        assert events[0]["type"] == "error"
        assert events[0]["action"] == {
            "label": "Open AI Models settings",
            "route": "/admin/ai-models",
        }
        assert events[1]["type"] == "done"


class TestWebTools:  # REQ-1796
    async def test_resolve_vendor_defaults_to_anthropic(self, monkeypatch, tmp_path):
        cfg = tmp_path / "provisa.yaml"
        cfg.write_text("sources: []\n")
        monkeypatch.setenv("PROVISA_CONFIG", str(cfg))
        assert await chat_mod._resolve_vendor(_state()) == "anthropic"

    async def test_resolve_vendor_reads_configured_value(self, monkeypatch, tmp_path):
        cfg = tmp_path / "provisa.yaml"
        cfg.write_text("sources: []\nai_models:\n  mcp_chat: {vendor: openai, model: gpt-5}\n")
        monkeypatch.setenv("PROVISA_CONFIG", str(cfg))
        assert await chat_mod._resolve_vendor(_state()) == "openai"

    async def test_web_tools_sent_when_vendor_is_anthropic(self, monkeypatch, tmp_path):
        import os

        cfg = tmp_path / "provisa.yaml"
        cfg.write_text("sources: []\n")
        monkeypatch.setitem(os.environ, "PROVISA_CONFIG", str(cfg))
        monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")
        resp = SimpleNamespace(content=[_block(type="text", text="hi")], stop_reason="end_turn")
        holder = _install_fake_anthropic(monkeypatch, [resp])

        async for _ in chat_mod.run_chat(_state(), "analyst", [{"role": "user", "content": "hi"}]):
            pass

        tool_names = {t["name"] for t in holder["client"].messages.calls[0]["tools"]}
        assert {"web_search", "web_fetch"} <= tool_names

    async def test_web_tools_omitted_for_non_anthropic_vendor(
        self, monkeypatch, tmp_path
    ):  # REQ-1797
        # Non-anthropic vendors go through aisuite (_run_chat_aisuite), which only ever converts
        # _TOOLS + _CLIENT_TOOLS (never _WEB_TOOLS, Anthropic-hosted-only) — so there is no tools
        # list this test could construct that would include web_search/web_fetch by mistake.
        import os

        cfg = tmp_path / "provisa.yaml"
        cfg.write_text("sources: []\nai_models:\n  mcp_chat: {vendor: openai, model: gpt-5}\n")
        monkeypatch.setitem(os.environ, "PROVISA_CONFIG", str(cfg))
        monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
        resp = SimpleNamespace(
            choices=[
                SimpleNamespace(
                    message=SimpleNamespace(content="hi", tool_calls=None), finish_reason="stop"
                )
            ]
        )
        holder = _install_fake_aisuite(monkeypatch, [resp])

        async for _ in chat_mod.run_chat(_state(), "analyst", [{"role": "user", "content": "hi"}]):
            pass

        tool_names = {
            t["function"]["name"] for t in holder["client"].chat.completions.calls[0]["tools"]
        }
        assert "web_search" not in tool_names
        assert "web_fetch" not in tool_names

    async def test_hosted_web_search_surfaces_as_tool_badge(self, monkeypatch):
        # Anthropic executes web_search itself — the result arrives as ordinary content blocks in
        # the SAME response, with stop_reason "end_turn" (not "tool_use"): nothing for us to
        # dispatch, just a badge for the UI.
        resp = SimpleNamespace(
            content=[
                _block(
                    type="server_tool_use", name="web_search", input={"query": "FRED API"}, id="w1"
                ),
                _block(
                    type="web_search_tool_result",
                    content=_block(type="web_search_tool_result_success"),
                ),
                _block(type="text", text="Found the FRED API docs."),
            ],
            stop_reason="end_turn",
        )
        _install_fake_anthropic(monkeypatch, [resp])

        events = [
            ev
            async for ev in chat_mod.run_chat(
                _state(), "analyst", [{"role": "user", "content": "find FRED API docs"}]
            )
        ]
        types = [e["type"] for e in events]
        assert types == ["tool_use", "tool_result", "text", "done"]
        assert events[0]["name"] == "web_search"
        assert events[1]["name"] == "web_search"
        assert events[1]["is_error"] is False


@pytest.mark.asyncio
class TestSubscriptionSources:  # REQ-1798
    async def test_search_govdata_subjects_dispatches_to_mcp_tool(self, monkeypatch):
        resp1 = SimpleNamespace(
            content=[
                _block(
                    type="tool_use",
                    name="search_govdata_subjects",
                    input={"query": "inflation"},
                    id="g1",
                )
            ],
            stop_reason="tool_use",
        )
        resp2 = SimpleNamespace(
            content=[_block(type="text", text="GovData has an econ schema with CPI tables.")],
            stop_reason="end_turn",
        )
        _install_fake_anthropic(monkeypatch, [resp1, resp2])

        seen = {}

        async def fake_search_govdata_subjects(state, role, query):
            seen.update(role=role, query=query)
            return [
                {
                    "schema": "econ",
                    "subject": "ECONOMY",
                    "tables": ["metro_cpi"],
                    "subscribed": True,
                }
            ]

        monkeypatch.setattr(mcp_tools, "search_govdata_subjects", fake_search_govdata_subjects)

        events = [
            ev
            async for ev in chat_mod.run_chat(
                _state(), "analyst", [{"role": "user", "content": "find me inflation data"}]
            )
        ]
        types = [e["type"] for e in events]
        assert types == ["tool_use", "tool_result", "text", "done"]
        assert events[1]["is_error"] is False
        assert seen == {"role": "analyst", "query": "inflation"}

    async def test_search_kaggle_datasets_dispatches_without_a_token_argument(self, monkeypatch):
        # No secret_name/token argument at all — the tool resolves a FIXED secret name itself
        # (mcp_tools.KAGGLE_TOKEN_SECRET_NAME); the model never chooses or asks for one.
        resp1 = SimpleNamespace(
            content=[
                _block(
                    type="tool_use",
                    name="search_kaggle_datasets",
                    input={"query": "inflation"},
                    id="k1",
                )
            ],
            stop_reason="tool_use",
        )
        resp2 = SimpleNamespace(
            content=[_block(type="text", text="Found a matching Kaggle dataset.")],
            stop_reason="end_turn",
        )
        _install_fake_anthropic(monkeypatch, [resp1, resp2])

        seen = {}

        async def fake_search_kaggle_datasets(state, role, query):
            seen.update(role=role, query=query)
            return [{"ref": "owner/inflation-data", "title": "Inflation Data", "description": ""}]

        monkeypatch.setattr(mcp_tools, "search_kaggle_datasets", fake_search_kaggle_datasets)

        events = [
            ev
            async for ev in chat_mod.run_chat(
                _state(), "analyst", [{"role": "user", "content": "find me inflation data"}]
            )
        ]
        types = [e["type"] for e in events]
        assert types == ["tool_use", "tool_result", "text", "done"]
        assert events[1]["is_error"] is False
        assert seen == {"role": "analyst", "query": "inflation"}


@pytest.mark.asyncio
class TestDirectCreate:  # REQ-1799
    async def test_run_chat_passes_the_real_request_to_propose_source(self, monkeypatch):
        resp1 = SimpleNamespace(
            content=[
                _block(
                    type="tool_use",
                    name="propose_source",
                    input={"source": {"id": "x", "type": "postgresql"}, "reason": "found it"},
                    id="p1",
                )
            ],
            stop_reason="tool_use",
        )
        resp2 = SimpleNamespace(
            content=[_block(type="text", text="You already hold the rights — create it now?")],
            stop_reason="end_turn",
        )
        _install_fake_anthropic(monkeypatch, [resp1, resp2])

        seen = {}

        async def fake_propose_source(state, role, source, reason, *, request=None):
            seen["request"] = request
            return {"status": "confirm_required", "capability": "source_registration"}

        monkeypatch.setattr(mcp_tools, "propose_source", fake_propose_source)

        sentinel_request = object()
        events = [
            ev
            async for ev in chat_mod.run_chat(
                _state(),
                "analyst",
                [{"role": "user", "content": "find a source"}],
                request=sentinel_request,
            )
        ]
        assert [e["type"] for e in events] == ["tool_use", "tool_result", "text", "done"]
        assert seen["request"] is sentinel_request

    async def test_create_source_now_dispatches_with_the_request(self, monkeypatch):
        resp1 = SimpleNamespace(
            content=[
                _block(
                    type="tool_use",
                    name="create_source_now",
                    input={"source": {"id": "x", "type": "postgresql"}, "reason": "user said yes"},
                    id="c1",
                )
            ],
            stop_reason="tool_use",
        )
        resp2 = SimpleNamespace(
            content=[_block(type="text", text="Created.")], stop_reason="end_turn"
        )
        _install_fake_anthropic(monkeypatch, [resp1, resp2])

        seen = {}

        async def fake_create_source_now(state, role, source, reason, *, request):
            seen.update(role=role, source=source, reason=reason, request=request)
            return {"success": True, "message": "created", "code": "schema.source_created"}

        monkeypatch.setattr(mcp_tools, "create_source_now", fake_create_source_now)

        sentinel_request = object()
        events = [
            ev
            async for ev in chat_mod.run_chat(
                _state(),
                "analyst",
                [{"role": "user", "content": "yes create it"}],
                request=sentinel_request,
            )
        ]
        assert [e["type"] for e in events] == ["tool_use", "tool_result", "text", "done"]
        assert events[1]["is_error"] is False
        assert seen["request"] is sentinel_request
        assert seen["role"] == "analyst"

    async def test_create_source_now_without_a_request_context_errors(self, monkeypatch):
        # run_chat called with no `request` (e.g. an older/non-HTTP caller) — create_source_now
        # must refuse rather than silently proceeding with no verified identity to check.
        resp1 = SimpleNamespace(
            content=[
                _block(
                    type="tool_use",
                    name="create_source_now",
                    input={"source": {"id": "x", "type": "postgresql"}, "reason": "user said yes"},
                    id="c1",
                )
            ],
            stop_reason="tool_use",
        )
        resp2 = SimpleNamespace(content=[_block(type="text", text="done")], stop_reason="end_turn")
        _install_fake_anthropic(monkeypatch, [resp1, resp2])

        events = [
            ev
            async for ev in chat_mod.run_chat(
                _state(), "analyst", [{"role": "user", "content": "yes create it"}]
            )
        ]
        tr = next(e for e in events if e["type"] == "tool_result")
        assert tr["is_error"] is True


class TestCurrentRoute:  # REQ-1800
    def test_system_prompt_unchanged_when_no_route_given(self):
        assert chat_mod._system_prompt(None) == chat_mod._SYSTEM
        assert chat_mod._system_prompt("") == chat_mod._SYSTEM

    def test_system_prompt_appends_the_route_when_given(self):
        prompt = chat_mod._system_prompt("/admin/ai-models")
        assert prompt.startswith(chat_mod._SYSTEM)
        assert "/admin/ai-models" in prompt

    async def test_run_chat_passes_current_route_into_the_anthropic_system_prompt(
        self, monkeypatch
    ):
        resp = SimpleNamespace(content=[_block(type="text", text="hi")], stop_reason="end_turn")
        holder = _install_fake_anthropic(monkeypatch, [resp])

        async for _ in chat_mod.run_chat(
            _state(),
            "analyst",
            [{"role": "user", "content": "what page am i on"}],
            current_route="/admin/sources",
        ):
            pass

        system = holder["client"].messages.calls[0]["system"]
        assert "/admin/sources" in system


class TestModelResolution:
    async def test_env_override_wins(self, monkeypatch):
        monkeypatch.setenv("PROVISA_MCP_CHAT_MODEL", "claude-sonnet-5")
        assert await chat_mod._resolve_model(_state()) == "claude-sonnet-5"

    async def test_defaults_to_opus(self, monkeypatch, tmp_path):
        import os

        monkeypatch.delenv("PROVISA_MCP_CHAT_MODEL", raising=False)
        # Point config at an empty file so there's no ai_models.mcp_chat entry → default.
        cfg = tmp_path / "provisa.yaml"
        cfg.write_text("sources: []\n")
        monkeypatch.setitem(os.environ, "PROVISA_CONFIG", str(cfg))
        assert await chat_mod._resolve_model(_state()) == "claude-opus-4-8"
