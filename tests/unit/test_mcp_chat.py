# Copyright (c) 2026 Kenneth Stott
# Canary: 4d174826-df75-45e5-a625-1620f07d1a41
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""MCP chat agent — the LLM chatbot that drives the governed MCP tools (REQ-1008).

The Anthropic client is faked with a scripted response sequence, so the tool-use
loop is exercised offline with no model call.
"""

from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest

from provisa.api.mcp import chat as chat_mod
from provisa.api.mcp import tools as mcp_tools


def _block(**kw):
    # `.model_dump(exclude=...)` mirrors the real Anthropic SDK content-block objects chat.py
    # relies on to serialize `resp.content` for the awaiting_client_tools event (REQ-1795), and
    # to drop ParsedTextBlock's SDK-internal `parsed_output` field (REQ-1838/1839's regression fix)
    # via `exclude=getattr(b, "__api_exclude__", None)`.
    def _dump(exclude=None):
        d = dict(kw)
        for field in exclude or ():
            d.pop(field, None)
        return d

    return SimpleNamespace(model_dump=_dump, **kw)


class _FakeStream:
    """REQ-1839: chat.py now consumes client.messages.stream(...) as an async context manager,
    async-iterating raw events (content_block_delta/content_block_stop) then calling
    .get_final_message() — mirror both. One delta + one stop event per content block, in order,
    is enough to exercise chat.py's real interleaving logic (text streamed as deltas, hosted-tool
    badges looked up via current_message_snapshot at each block's stop event)."""

    def __init__(self, response):
        self._response = response
        self.current_message_snapshot = response

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def __aiter__(self):
        for i, block in enumerate(self._response.content):
            if block.type == "text" and block.text:
                yield SimpleNamespace(
                    type="content_block_delta",
                    index=i,
                    delta=SimpleNamespace(type="text_delta", text=block.text),
                )
            yield SimpleNamespace(type="content_block_stop", index=i)

    async def get_final_message(self):
        return self._response


class _FakeMessages:
    def __init__(self, responses):
        self._responses = list(responses)
        self.calls = []

    async def create(self, **kwargs):
        self.calls.append(kwargs)
        return self._responses.pop(0)

    def stream(self, **kwargs):
        self.calls.append(kwargs)
        return _FakeStream(self._responses.pop(0))


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


class _FakeAisuiteProvider:
    """REQ-1808/1809: chat.py calls the aisuite PROVIDER directly (ProviderFactory.create_provider
    + its chat_completions_create), not aisuite.Client().chat.completions.create() — see
    _run_chat_aisuite's own docstring for why (aisuite 0.1.14 silently drops `tools` through that
    wrapper unless `max_turns` is also given, which triggers a THEN-incompatible auto-exec loop)."""

    def __init__(self, responses):
        self._responses = list(responses)
        self.calls = []

    def chat_completions_create(self, model_name, messages, **kwargs):
        self.calls.append({"model": model_name, "messages": messages, **kwargs})
        return self._responses.pop(0)


def _install_fake_aisuite(monkeypatch, responses):
    """REQ-1797: aisuite is sync (run via asyncio.to_thread), so its fake needs no `async def`."""
    holder = {}

    def _factory(_provider_key, _config):
        provider = _FakeAisuiteProvider(responses)
        holder["provider"] = provider
        return provider

    from aisuite.provider import ProviderFactory

    monkeypatch.setattr(ProviderFactory, "create_provider", staticmethod(_factory))
    return holder


def _state():
    ctx = SimpleNamespace(tables={}, joins={})
    return SimpleNamespace(
        contexts={"analyst": ctx},
        config=SimpleNamespace(roles=[SimpleNamespace(id="analyst", domain_access=["sales"])]),
    )


async def _async_result(value):
    return value


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

    async def test_assistant_content_drops_parsed_output_for_a_client_tool_pause(
        self, monkeypatch
    ):  # REQ-1838/1839 regression
        # stream.get_final_message() returns ParsedTextBlock content, which carries an SDK-
        # internal `parsed_output` field (always None here, __api_exclude__'d by the SDK itself)
        # that plain model_dump() doesn't know to drop — sent back verbatim in a resume POST's
        # assistant_content, Anthropic's real API rejected it with a 400 invalid_request_error.
        # Reproduced live: any turn pausing for a client tool (e.g. present_choice) after the
        # REQ-1838/1839 switch to streaming hit this on every resume.
        text_block = _block(type="text", text="Sure, one moment.", parsed_output=None)
        text_block.__api_exclude__ = {"parsed_output"}
        resp1 = SimpleNamespace(
            content=[
                text_block,
                _block(type="tool_use", name="navigate", input={"route": "/sources"}, id="c1"),
            ],
            stop_reason="tool_use",
        )
        _install_fake_anthropic(monkeypatch, [resp1])

        events = [
            ev
            async for ev in chat_mod.run_chat(
                _state(), "analyst", [{"role": "user", "content": "go to sources"}]
            )
        ]
        awaiting = next(e for e in events if e["type"] == "awaiting_client_tools")
        dumped_text_block = next(
            b for b in awaiting["assistant_content"] if b.get("type") == "text"
        )
        assert "parsed_output" not in dumped_text_block

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

    async def test_prior_rounds_survive_a_later_pause_in_the_same_turn(
        self, monkeypatch
    ):  # REQ-1850
        # A server-only round (search_catalog) resolves and gets appended to `convo` BEFORE a
        # second, later round pauses on a client tool (navigate). The regression: the paused
        # turn's awaiting_client_tools event only carried that final round's assistant_content,
        # dropping the search_catalog round the model already ran — the resumed conversation then
        # had no memory of it and the model repeated the whole sequence, forever.
        resp1 = SimpleNamespace(
            content=[
                _block(type="tool_use", name="search_catalog", input={"query": "orders"}, id="s1")
            ],
            stop_reason="tool_use",
        )
        resp2 = SimpleNamespace(
            content=[
                _block(type="text", text="Found it, navigating."),
                _block(type="tool_use", name="navigate", input={"route": "/tables"}, id="c1"),
            ],
            stop_reason="tool_use",
        )
        _install_fake_anthropic(monkeypatch, [resp1, resp2])

        async def fake_search(state, role, query, k=5):
            return [{"schema": "sales", "table": "orders"}]

        monkeypatch.setattr(mcp_tools, "search_catalog", fake_search)

        events = [
            ev
            async for ev in chat_mod.run_chat(
                _state(), "analyst", [{"role": "user", "content": "find orders, go to tables"}]
            )
        ]
        awaiting = next(e for e in events if e["type"] == "awaiting_client_tools")
        prior = awaiting["prior_messages"]
        assert len(prior) == 2  # the search_catalog round's assistant turn + its tool_result
        assert prior[0]["role"] == "assistant"
        assert prior[0]["content"][0]["name"] == "search_catalog"
        assert prior[1]["role"] == "user"
        assert prior[1]["content"][0]["tool_use_id"] == "s1"

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

    async def test_custom_endpoint_env_key_is_configured(self, monkeypatch, tmp_path):
        import os

        monkeypatch.setenv("MY_GATEWAY_KEY", "sk-real-key")
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
        assert ok is True
        assert reason == ""

    async def test_custom_endpoint_secret_reference_is_configured(self, monkeypatch, tmp_path):
        # REQ-1826: api_key_env carrying a ${secret:NAME} reference (the pattern the AI Models UI
        # itself recommends, per REQ-1808) must resolve through the SAME grammar
        # _resolve_api_key_field uses at actual chat time — a bare os.environ.get(key_env) check
        # here always failed for this exact string (it is never itself a real env var name),
        # permanently reporting "not configured" for a correctly set-up vault-backed endpoint.
        import os

        cfg = tmp_path / "provisa.yaml"
        cfg.write_text(
            "sources: []\n"
            "ai_models:\n"
            "  mcp_chat: {vendor: my-gateway, model: some-model}\n"
            "ai_endpoints:\n"
            "  - id: my-gateway\n"
            "    style: openai\n"
            "    base_url: https://gateway.internal/v1\n"
            "    api_key_env: ${secret:my_gateway_key}\n"
            "    enabled: true\n"
        )
        monkeypatch.setitem(os.environ, "PROVISA_CONFIG", str(cfg))
        monkeypatch.setattr(
            chat_mod, "_resolve_api_key_field", lambda api_key_env: _async_result("sk-vault-value")
        )
        ok, reason = await chat_mod._llm_configured(_state())
        assert ok is True
        assert reason == ""

    async def test_custom_endpoint_secret_reference_unresolved_is_unconfigured(
        self, monkeypatch, tmp_path
    ):
        import os

        cfg = tmp_path / "provisa.yaml"
        cfg.write_text(
            "sources: []\n"
            "ai_models:\n"
            "  mcp_chat: {vendor: my-gateway, model: some-model}\n"
            "ai_endpoints:\n"
            "  - id: my-gateway\n"
            "    style: openai\n"
            "    base_url: https://gateway.internal/v1\n"
            "    api_key_env: ${secret:my_gateway_key}\n"
            "    enabled: true\n"
        )
        monkeypatch.setitem(os.environ, "PROVISA_CONFIG", str(cfg))
        monkeypatch.setattr(
            chat_mod, "_resolve_api_key_field", lambda api_key_env: _async_result(None)
        )
        ok, reason = await chat_mod._llm_configured(_state())
        assert ok is False
        assert "${secret:my_gateway_key}" in reason

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

        tool_names = {t["function"]["name"] for t in holder["provider"].calls[0]["tools"]}
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


class TestMaxTokensTruncation:  # REQ-1838
    """A response cut off mid-thought (adaptive thinking exhausted the max_tokens budget before
    the model emitted its tool_use block) must never end the turn silently — see chat.py's
    _run_chat_anthropic docstring/comment for the live Opus 4.6 repro this guards."""

    async def test_max_tokens_stop_reason_surfaces_a_message_instead_of_silence(self, monkeypatch):
        resp = SimpleNamespace(
            content=[_block(type="text", text="Let me fetch both now.")],
            stop_reason="max_tokens",
        )
        _install_fake_anthropic(monkeypatch, [resp])

        events = [
            ev
            async for ev in chat_mod.run_chat(
                _state(), "analyst", [{"role": "user", "content": "define these terms"}]
            )
        ]

        text_events = [e for e in events if e["type"] == "text"]
        assert len(text_events) == 2  # the partial reply, then the truncation notice
        assert "ran out" in text_events[1]["text"].lower()
        assert events[-1]["type"] == "done"

    async def test_max_tokens_call_uses_a_generous_cap(self, monkeypatch):
        resp = SimpleNamespace(
            content=[_block(type="text", text="hi")],
            stop_reason="end_turn",
        )
        holder = _install_fake_anthropic(monkeypatch, [resp])

        async for _ in chat_mod.run_chat(_state(), "analyst", [{"role": "user", "content": "hi"}]):
            pass

        # 8192 was the value that let adaptive thinking exhaust the budget before a tool_use
        # block was emitted — must stay well above it.
        assert holder["client"].messages.calls[0]["max_tokens"] > 8192


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


@pytest.mark.asyncio
class TestCustomEndpointRouting:  # REQ-1790, REQ-1808
    """A configured ai_endpoint is reached via aisuite's underlying openai/anthropic provider
    with base_url/api_key overridden — never by aisuite recognizing the endpoint's own id (it
    can't; Client.create only accepts its fixed provider registry)."""

    def _state_with_endpoint(self, tmp_path, endpoint: dict, monkeypatch):
        cfg = tmp_path / "provisa.yaml"
        import yaml

        cfg.write_text(yaml.dump({"sources": [], "ai_endpoints": [endpoint]}))
        monkeypatch.setenv("PROVISA_CONFIG", str(cfg))
        return _state()

    async def test_native_vendor_gets_no_provider_override(self, tmp_path, monkeypatch):
        state = self._state_with_endpoint(
            tmp_path, {"id": "my-gateway", "style": "openai", "base_url": "https://x"}, monkeypatch
        )
        model_id, provider_configs = await chat_mod._resolve_aisuite_routing(
            state, "ollama", "llama3.1:8b"
        )
        assert model_id == "ollama:llama3.1:8b"
        assert provider_configs == {}

    async def test_custom_endpoint_routes_through_its_style_provider(self, tmp_path, monkeypatch):
        state = self._state_with_endpoint(
            tmp_path,
            {"id": "ollama-local", "style": "openai", "base_url": "http://localhost:11434/v1"},
            monkeypatch,
        )
        model_id, provider_configs = await chat_mod._resolve_aisuite_routing(
            state, "ollama-local", "llama3.1:8b"
        )
        # The model string names "openai" (the provider aisuite actually knows), not the
        # endpoint's own id — that id only ever resolves through provider_configs.
        assert model_id == "openai:llama3.1:8b"
        assert provider_configs == {
            "openai": {"base_url": "http://localhost:11434/v1", "api_key": "not-required"}
        }

    async def test_anthropic_style_gets_no_placeholder_key(self, tmp_path, monkeypatch):
        state = self._state_with_endpoint(
            tmp_path,
            {"id": "claude-gateway", "style": "anthropic", "base_url": "https://gateway.internal"},
            monkeypatch,
        )
        model_id, provider_configs = await chat_mod._resolve_aisuite_routing(
            state, "claude-gateway", "claude-x"
        )
        assert model_id == "anthropic:claude-x"
        assert provider_configs == {"anthropic": {"base_url": "https://gateway.internal"}}

    async def test_api_key_env_names_a_plain_env_var(self, tmp_path, monkeypatch):
        monkeypatch.setenv("MY_GATEWAY_KEY", "literal-key-value")
        state = self._state_with_endpoint(
            tmp_path,
            {
                "id": "gw",
                "style": "openai",
                "base_url": "https://gw",
                "api_key_env": "MY_GATEWAY_KEY",
            },
            monkeypatch,
        )
        _, provider_configs = await chat_mod._resolve_aisuite_routing(state, "gw", "m")
        assert provider_configs["openai"]["api_key"] == "literal-key-value"

    async def test_api_key_env_value_is_itself_an_env_reference(self, tmp_path, monkeypatch):
        # The env var NAMED by api_key_env holds a further ${env:...} reference — one layer of
        # indirection resolves into another, per the general reference grammar.
        monkeypatch.setenv("REAL_KEY", "the-actual-key")
        monkeypatch.setenv("INDIRECTION_VAR", "${env:REAL_KEY}")
        state = self._state_with_endpoint(
            tmp_path,
            {
                "id": "gw",
                "style": "openai",
                "base_url": "https://gw",
                "api_key_env": "INDIRECTION_VAR",
            },
            monkeypatch,
        )
        _, provider_configs = await chat_mod._resolve_aisuite_routing(state, "gw", "m")
        assert provider_configs["openai"]["api_key"] == "the-actual-key"

    async def test_api_key_env_field_can_be_a_direct_env_reference(self, tmp_path, monkeypatch):
        # No indirection at all: api_key_env holds "${env:...}" directly rather than naming a
        # var — the field's value IS the reference, not a name to look up.
        monkeypatch.setenv("DIRECT_KEY", "direct-key-value")
        state = self._state_with_endpoint(
            tmp_path,
            {
                "id": "gw",
                "style": "openai",
                "base_url": "https://gw",
                "api_key_env": "${env:DIRECT_KEY}",
            },
            monkeypatch,
        )
        _, provider_configs = await chat_mod._resolve_aisuite_routing(state, "gw", "m")
        assert provider_configs["openai"]["api_key"] == "direct-key-value"

    async def test_base_url_can_be_a_direct_env_reference(self, tmp_path, monkeypatch):
        monkeypatch.setenv("GATEWAY_URL", "https://resolved-gateway.internal")
        state = self._state_with_endpoint(
            tmp_path, {"id": "gw", "style": "openai", "base_url": "${env:GATEWAY_URL}"}, monkeypatch
        )
        _, provider_configs = await chat_mod._resolve_aisuite_routing(state, "gw", "m")
        assert provider_configs["openai"]["base_url"] == "https://resolved-gateway.internal"

    async def test_base_url_can_be_a_secret_reference(self, tmp_path, monkeypatch):
        # ${secret:...} needs the org vault bound — verify it's actually invoked, not just that
        # ${env:...} happens to work (which needs no org context at all).
        def fake_resolve_secrets(value):
            assert value == "${secret:gateway_url}"
            return "https://from-the-vault.internal"

        @asynccontextmanager
        async def fake_bound_to_request_org():
            yield

        monkeypatch.setattr("provisa.core.secrets.resolve_secrets", fake_resolve_secrets)
        monkeypatch.setattr(
            "provisa.core.secrets_store.bound_to_request_org", fake_bound_to_request_org
        )
        state = self._state_with_endpoint(
            tmp_path,
            {"id": "gw", "style": "openai", "base_url": "${secret:gateway_url}"},
            monkeypatch,
        )
        _, provider_configs = await chat_mod._resolve_aisuite_routing(state, "gw", "m")
        assert provider_configs["openai"]["base_url"] == "https://from-the-vault.internal"

    async def test_no_api_key_env_and_anthropic_style_stays_keyless(self, tmp_path, monkeypatch):
        state = self._state_with_endpoint(
            tmp_path, {"id": "gw", "style": "anthropic", "base_url": "https://gw"}, monkeypatch
        )
        _, provider_configs = await chat_mod._resolve_aisuite_routing(state, "gw", "m")
        assert "api_key" not in provider_configs["anthropic"]
