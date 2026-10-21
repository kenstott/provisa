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
import yaml

from provisa.api.mcp import chat as chat_mod
from provisa.api.mcp import tools as mcp_tools
from tests.role_state import RoleState


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


def _with_usage(response):
    """A response as the SDK returns it: every one reports its token usage (REQ-2012)."""
    if not hasattr(response, "usage"):
        response.usage = SimpleNamespace(
            input_tokens=10,
            output_tokens=5,
            cache_read_input_tokens=0,
            cache_creation_input_tokens=0,
        )
    return response


def _with_openai_usage(response):
    """An OpenAI-shaped response as aisuite returns it, with its token usage (REQ-2012)."""
    if not hasattr(response, "usage"):
        response.usage = SimpleNamespace(
            prompt_tokens=10, completion_tokens=5, prompt_tokens_details=None
        )
    return response


class _FakeMessages:
    def __init__(self, responses):
        self._responses = list(responses)
        self.calls = []

    async def create(self, **kwargs):
        self.calls.append(kwargs)
        return self._responses.pop(0)

    def stream(self, **kwargs):
        self.calls.append(kwargs)
        return _FakeStream(_with_usage(self._responses.pop(0)))


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
        return _with_openai_usage(self._responses.pop(0))


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


@pytest.fixture(autouse=True)
def _a_configured_vendor_key(monkeypatch):
    """The chat runs only once its vendor's key is set (the readiness check, REQ-1794); these
    tests fake the vendor's client, so they state the key it is configured with rather than
    depend on one in the environment. A test of the unconfigured case removes it itself."""
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")


def _state():
    ctx = SimpleNamespace(tables={}, joins={})
    return RoleState(
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
        assert types == ["tool_use", "tool_result", "text", "usage", "done"]
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

    async def test_admin_query_dispatches_as_the_user(self, monkeypatch):  # REQ-2114
        resp1 = SimpleNamespace(
            content=[
                _block(
                    type="tool_use",
                    name="admin_query",
                    input={"query": "{ sources { id } }", "variables": {}},
                    id="q1",
                )
            ],
            stop_reason="tool_use",
        )
        resp2 = SimpleNamespace(
            content=[_block(type="text", text="You have one source.")], stop_reason="end_turn"
        )
        _install_fake_anthropic(monkeypatch, [resp1, resp2])

        seen = {}

        async def fake_run_query(query, variables, *, request):
            seen.update(query=query, variables=variables, request=request)
            return {"data": {"sources": [{"id": "pg"}]}, "errors": []}

        monkeypatch.setattr(chat_mod.admin_api, "run_query", fake_run_query)

        sentinel_request = object()
        events = [
            ev
            async for ev in chat_mod.run_chat(
                _state(),
                "analyst",
                [{"role": "user", "content": "what sources do I have"}],
                request=sentinel_request,
            )
        ]
        assert [e["type"] for e in events] == ["tool_use", "tool_result", "text", "usage", "done"]
        assert events[1]["is_error"] is False
        # Ran as the user: the chat's own request, with the model's query passed through.
        assert seen == {"query": "{ sources { id } }", "variables": {}, "request": sentinel_request}

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
        assert types == ["tool_use", "awaiting_client_tools", "usage", "done"]
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
        assert types == [
            "tool_use",
            "tool_result",
            "tool_use",
            "awaiting_client_tools",
            "usage",
            "done",
        ]
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
        assert types == ["tool_use", "tool_result", "text", "usage", "done"]
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
        assert types == ["tool_use", "tool_result", "text", "usage", "done"]
        assert events[1]["is_error"] is False
        assert seen == {"role": "analyst", "query": "inflation"}

    async def test_search_kaggle_datasets_dispatches_without_a_token_argument(self, monkeypatch):
        # No secret_name/token argument at all — the tool reads the operator's Kaggle token
        # setting itself; the model never chooses or asks for a secret name.
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
        assert types == ["tool_use", "tool_result", "text", "usage", "done"]
        assert events[1]["is_error"] is False
        assert seen == {"role": "analyst", "query": "inflation"}


@pytest.mark.asyncio
class TestAdminApiAsTheUser:  # REQ-2114
    async def test_discover_relationships_runs_with_the_real_request(self, monkeypatch):
        resp1 = SimpleNamespace(
            content=[
                _block(
                    type="tool_use",
                    name="discover_relationships",
                    input={"schema": "sales", "table": "orders"},
                    id="d1",
                )
            ],
            stop_reason="tool_use",
        )
        resp2 = SimpleNamespace(
            content=[_block(type="text", text="Two candidates.")], stop_reason="end_turn"
        )
        _install_fake_anthropic(monkeypatch, [resp1, resp2])

        seen = {}

        async def fake_discover(state, schema, table, *, request):
            seen.update(schema=schema, table=table, request=request)
            return {"table": "sales.orders", "candidates": []}

        monkeypatch.setattr(chat_mod.admin_api, "discover_relationships", fake_discover)

        sentinel_request = object()
        events = [
            ev
            async for ev in chat_mod.run_chat(
                _state(),
                "analyst",
                [{"role": "user", "content": "create any relationships to orders"}],
                request=sentinel_request,
            )
        ]
        assert [e["type"] for e in events] == ["tool_use", "tool_result", "text", "usage", "done"]
        assert seen == {"schema": "sales", "table": "orders", "request": sentinel_request}

    async def test_a_change_pauses_for_the_browser_to_confirm_and_send(self, monkeypatch):
        """admin_mutation is a client tool: the server never sends it; the chat pauses for the
        user's browser, which shows it and sends it as the user once they confirm."""
        resp1 = SimpleNamespace(
            content=[
                _block(
                    type="tool_use",
                    name="admin_mutation",
                    input={
                        "mutation": 'mutation { createSource(input: {id: "x"}) { success } }',
                        "summary": "Create the source x.",
                    },
                    id="m1",
                )
            ],
            stop_reason="tool_use",
        )
        _install_fake_anthropic(monkeypatch, [resp1])

        events = [
            ev
            async for ev in chat_mod.run_chat(
                _state(), "analyst", [{"role": "user", "content": "add source x"}]
            )
        ]
        assert [e["type"] for e in events] == ["tool_use", "awaiting_client_tools", "usage", "done"]
        assert events[0]["client"] is True
        assert events[1]["pending"][0]["name"] == "admin_mutation"

    async def test_admin_query_without_a_request_context_errors(self, monkeypatch):
        # run_chat called with no `request` (e.g. an older/non-HTTP caller) — admin_query must
        # refuse rather than run with no verified identity.
        resp1 = SimpleNamespace(
            content=[
                _block(type="tool_use", name="admin_query", input={"query": "{ x }"}, id="q1")
            ],
            stop_reason="tool_use",
        )
        resp2 = SimpleNamespace(content=[_block(type="text", text="done")], stop_reason="end_turn")
        _install_fake_anthropic(monkeypatch, [resp1, resp2])

        events = [
            ev
            async for ev in chat_mod.run_chat(
                _state(), "analyst", [{"role": "user", "content": "list sources"}]
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
        assert "/admin/sources" in system[0]["text"]


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


_NARRATION = "Let me look at the catalog's metadata to find the registered sources."
_ANSWER = "Here are the registered data sources."


@pytest.mark.asyncio
class TestTextBesideAToolCallIsNotShown:
    """ "Let me look at the catalog's metadata to find the registered sources.Here are...", and
    "Done -- the Graph page is now running this" written with the navigate that had not run yet:
    text the model writes beside a tool call is written before the call's result exists. It is
    never shown; it stays in the history the model is sent back."""

    async def test_anthropic(self, monkeypatch):
        resp1 = SimpleNamespace(
            content=[
                _block(type="text", text=_NARRATION),
                _block(type="tool_use", name="list_schemas", input={}, id="tu1"),
            ],
            stop_reason="tool_use",
        )
        resp2 = SimpleNamespace(content=[_block(type="text", text=_ANSWER)], stop_reason="end_turn")
        holder = _install_fake_anthropic(monkeypatch, [resp1, resp2])
        monkeypatch.setattr(mcp_tools, "list_schemas", lambda *_a, **_k: _async_result([]))
        convo = [{"role": "user", "content": "What data sources are registered?"}]
        events = [ev async for ev in chat_mod.run_chat(_state(), "analyst", convo)]
        assert [e["text"] for e in events if e["type"] == "text"] == [_ANSWER]
        resent = holder["client"].messages.calls[1]["messages"][-2]["content"]
        assert resent[0].text == _NARRATION

    async def test_anthropic_beside_a_client_tool(self, monkeypatch):
        resp = SimpleNamespace(
            content=[
                _block(type="text", text="Done -- the Graph page is now running this."),
                _block(type="tool_use", name="navigate", input={"route": "/sql"}, id="tu1"),
            ],
            stop_reason="tool_use",
        )
        _install_fake_anthropic(monkeypatch, [resp])
        convo = [{"role": "user", "content": "open the SQL page"}]
        events = [ev async for ev in chat_mod.run_chat(_state(), "analyst", convo)]
        assert not [e for e in events if e["type"] == "text"]
        paused = next(e for e in events if e["type"] == "awaiting_client_tools")
        assert paused["assistant_content"][0]["text"].startswith("Done --")

    async def test_aisuite(self, monkeypatch, tmp_path):
        import os

        cfg = tmp_path / "provisa.yaml"
        cfg.write_text("sources: []\nai_models:\n  mcp_chat: {vendor: openai, model: gpt-5}\n")
        monkeypatch.setitem(os.environ, "PROVISA_CONFIG", str(cfg))
        monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
        call = SimpleNamespace(
            id="c1", function=SimpleNamespace(name="list_schemas", arguments="{}")
        )
        holder = _install_fake_aisuite(
            monkeypatch,
            [
                SimpleNamespace(
                    choices=[
                        SimpleNamespace(
                            message=SimpleNamespace(content=_NARRATION, tool_calls=[call]),
                            finish_reason="tool_calls",
                        )
                    ]
                ),
                SimpleNamespace(
                    choices=[
                        SimpleNamespace(
                            message=SimpleNamespace(content=_ANSWER, tool_calls=None),
                            finish_reason="stop",
                        )
                    ]
                ),
            ],
        )
        monkeypatch.setattr(mcp_tools, "list_schemas", lambda *_a, **_k: _async_result([]))
        convo = [{"role": "user", "content": "What data sources are registered?"}]
        events = [ev async for ev in chat_mod.run_chat(_state(), "analyst", convo)]
        assert [e["text"] for e in events if e["type"] == "text"] == [_ANSWER]
        resent = holder["provider"].calls[1]["messages"]
        assert any(m["role"] == "assistant" and m["content"] == _NARRATION for m in resent)


class TestResponseGuidance:
    """The transcript's other faults, which only the prompt can steer."""

    def test_no_step_narration(self):
        assert "Never narrate your steps" in chat_mod._SYSTEM

    def test_a_large_result_is_summarized_not_pasted(self):
        # "list the sonnets" pasted all 154 first lines into the chat.
        assert "never paste hundreds of lines into the chat" in chat_mod._SYSTEM
        assert "a table on the SQL page via navigate" in chat_mod._SYSTEM

    def test_only_what_a_result_confirms_is_claimed(self):
        # "Done -- the graph is now grouped by theme", with nothing to say so.
        assert "State only what a tool result confirms" in chat_mod._SYSTEM

    def test_a_graph_path_returns_its_relationships(self):
        assert "binds every relationship on it and returns it with the nodes" in chat_mod._SYSTEM


def _stop_reply():
    return SimpleNamespace(
        choices=[
            SimpleNamespace(
                message=SimpleNamespace(content="hi", tool_calls=None), finish_reason="stop"
            )
        ]
    )


class TestEveryCallIsCapped:
    """No model call goes out without a cap on what it may generate: with none, OpenRouter
    reserves the model's whole output limit (65,536 or 131,072 tokens) against the account's
    credit and refuses the call with 402 when that is more than the credit left -- the bake-off's
    run 38613079392 -- and nothing bounds what one call can spend."""

    async def _first_call(self, monkeypatch, tmp_path, config: dict) -> dict:
        import os

        cfg = tmp_path / "provisa.yaml"
        cfg.write_text(yaml.safe_dump({"sources": [], **config}))
        monkeypatch.setitem(os.environ, "PROVISA_CONFIG", str(cfg))
        monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
        monkeypatch.setenv("OPENROUTER_API_KEY", "sk-test")
        holder = _install_fake_aisuite(monkeypatch, [_stop_reply()])
        convo = [{"role": "user", "content": "hi"}]
        async for _ in chat_mod.run_chat(_state(), "analyst", convo):
            pass
        return holder["provider"].calls[0]

    @pytest.mark.asyncio
    async def test_a_gateway_call_is_capped_with_max_tokens(self, monkeypatch, tmp_path):
        call = await self._first_call(
            monkeypatch,
            tmp_path,
            {
                "ai_endpoints": [
                    {
                        "id": "openrouter",
                        "style": "openai",
                        "base_url": "https://openrouter.ai/api/v1",
                        "api_key_env": "OPENROUTER_API_KEY",
                    }
                ],
                "ai_models": {"mcp_chat": {"vendor": "openrouter", "model": "qwen/qwen3.8-flash"}},
            },
        )
        assert call["max_tokens"] == chat_mod._MAX_TOKENS

    @pytest.mark.asyncio
    async def test_an_openai_call_is_capped_as_openai_takes_it(self, monkeypatch, tmp_path):
        call = await self._first_call(
            monkeypatch,
            tmp_path,
            {"ai_models": {"mcp_chat": {"vendor": "openai", "model": "gpt-5"}}},
        )
        assert call["max_completion_tokens"] == chat_mod._MAX_TOKENS
        assert "max_tokens" not in call

    def test_each_provider_takes_the_cap_by_its_own_name(self):
        assert chat_mod._output_cap("anthropic", custom_endpoint=True) == {
            "max_tokens": chat_mod._MAX_TOKENS
        }
        assert chat_mod._output_cap("ollama", custom_endpoint=False) == {
            "options": {"num_predict": chat_mod._MAX_TOKENS}
        }
        assert chat_mod._output_cap("mistral", custom_endpoint=False) == {
            "max_tokens": chat_mod._MAX_TOKENS
        }

    def test_a_provider_that_drops_the_cap_is_refused(self):
        with pytest.raises(ValueError, match="cannot cap a Google model"):
            chat_mod._output_cap("google", custom_endpoint=False)

    @pytest.mark.asyncio
    async def test_an_anthropic_call_is_capped_the_same(self, monkeypatch):
        holder = _install_fake_anthropic(
            monkeypatch,
            [SimpleNamespace(content=[_block(type="text", text="hi")], stop_reason="end_turn")],
        )
        async for _ in chat_mod.run_chat(_state(), "analyst", [{"role": "user", "content": "hi"}]):
            pass
        assert holder["client"].messages.calls[0]["max_tokens"] == chat_mod._MAX_TOKENS


def test_polly_searches_before_saying_it_has_no_tool():
    """Asked "Which pages are slowest, and which SQL makes them slow?", Haiku called no tool and
    answered that it had none for page load times -- the demo's slowest-pages view answers it."""
    assert (
        "never answer that you have no tool or data for something you have not searched the "
        "catalog for in this turn" in chat_mod._SYSTEM
    )


def _schemas_call(call_id: str):
    return SimpleNamespace(
        content=[_block(type="tool_use", name="list_schemas", input={}, id=call_id)],
        stop_reason="tool_use",
    )


@pytest.mark.asyncio
class TestTheLastRoundAnswers:
    """REQ-1838, never go silent: Gemini and Qwen spent all eight rounds of the bake-off's demo
    question still calling tools, and the turn ended with no text at all (run 38625083413)."""

    async def test_the_last_round_is_asked_to_call_no_tool(self, monkeypatch):
        holder = _install_fake_anthropic(
            monkeypatch,
            [
                _schemas_call("tu1"),
                SimpleNamespace(
                    content=[_block(type="text", text="Two schemas.")], stop_reason="end_turn"
                ),
            ],
        )
        monkeypatch.setattr(mcp_tools, "list_schemas", lambda *_a, **_k: _async_result([]))
        convo = [{"role": "user", "content": "go"}]
        events = [
            ev async for ev in chat_mod.run_chat(_state(), "analyst", convo, max_iterations=2)
        ]
        first, last = holder["client"].messages.calls
        from anthropic import omit

        assert first["tool_choice"] is omit  # the model's own choice, as on any other round
        assert last["tool_choice"] == {"type": "none"}
        assert [e["text"] for e in events if e["type"] == "text"] == ["Two schemas."]

    async def test_rounds_spent_still_calling_tools_end_saying_so(self, monkeypatch):
        _install_fake_anthropic(monkeypatch, [_schemas_call("tu1"), _schemas_call("tu2")])
        monkeypatch.setattr(mcp_tools, "list_schemas", lambda *_a, **_k: _async_result([]))
        convo = [{"role": "user", "content": "go"}]
        events = [
            ev async for ev in chat_mod.run_chat(_state(), "analyst", convo, max_iterations=2)
        ]
        assert events[-3] == chat_mod._STEPS_SPENT
        assert [e["type"] for e in events[-2:]] == ["usage", "done"]

    async def _aisuite_rounds(self, monkeypatch, tmp_path, config: str, replies: list) -> list:
        import os

        cfg = tmp_path / "provisa.yaml"
        cfg.write_text(config)
        monkeypatch.setitem(os.environ, "PROVISA_CONFIG", str(cfg))
        monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
        monkeypatch.setenv("GATEWAY_KEY", "sk-test")
        monkeypatch.setattr(mcp_tools, "list_schemas", lambda *_a, **_k: _async_result([]))
        holder = _install_fake_aisuite(monkeypatch, replies)
        convo = [{"role": "user", "content": "go"}]
        async for _ in chat_mod.run_chat(_state(), "analyst", convo, max_iterations=2):
            pass
        return holder["provider"].calls

    async def test_the_aisuite_last_round_is_sent_without_tools(self, monkeypatch, tmp_path):
        # OpenRouter's Gemini and Qwen routes answered tool_choice "none" with more tool calls
        # (bake-off run 38630859730): a round with no tools has none to call.
        call = SimpleNamespace(
            id="c1", function=SimpleNamespace(name="list_schemas", arguments="{}")
        )
        calls = await self._aisuite_rounds(
            monkeypatch,
            tmp_path,
            "sources: []\nai_models:\n  mcp_chat: {vendor: openai, model: gpt-5}\n",
            [
                SimpleNamespace(
                    choices=[
                        SimpleNamespace(
                            message=SimpleNamespace(content=None, tool_calls=[call]),
                            finish_reason="tool_calls",
                        )
                    ]
                ),
                _stop_reply(),
            ],
        )
        first, last = calls
        assert first["tools"] and "tool_choice" not in first
        assert "tools" not in last and "tool_choice" not in last

    async def test_an_anthropic_wire_last_round_keeps_its_tools_and_calls_none(
        self, monkeypatch, tmp_path
    ):
        calls = await self._aisuite_rounds(
            monkeypatch,
            tmp_path,
            yaml.safe_dump(
                {
                    "sources": [],
                    "ai_endpoints": [
                        {
                            "id": "gw",
                            "style": "anthropic",
                            "base_url": "https://gw.example",
                            "api_key_env": "GATEWAY_KEY",
                        }
                    ],
                    "ai_models": {"mcp_chat": {"vendor": "gw", "model": "claude-x"}},
                }
            ),
            [
                SimpleNamespace(
                    choices=[
                        SimpleNamespace(
                            message=SimpleNamespace(
                                content=None,
                                tool_calls=[
                                    SimpleNamespace(
                                        id="c1",
                                        function=SimpleNamespace(
                                            name="list_schemas", arguments="{}"
                                        ),
                                    )
                                ],
                            ),
                            finish_reason="tool_calls",
                        )
                    ]
                ),
                _stop_reply(),
            ],
        )
        assert calls[1]["tools"] and calls[1]["tool_choice"] == {"type": "none"}


@pytest.mark.asyncio
class TestABlankAnswerIsNeverShownAsIs:
    """REQ-1838: DeepSeek's final reply was blank twice in bake-off run 38635124027 ("\\n\\n" after
    one call, "" after eight) and the user would have seen an empty turn."""

    async def test_anthropic(self, monkeypatch):
        _install_fake_anthropic(
            monkeypatch,
            [SimpleNamespace(content=[_block(type="text", text="\n\n")], stop_reason="end_turn")],
        )
        events = [
            ev
            async for ev in chat_mod.run_chat(
                _state(), "analyst", [{"role": "user", "content": "q"}]
            )
        ]
        assert [e for e in events if e["type"] == "text"] == [chat_mod._NO_ANSWER]

    async def _aisuite(self, monkeypatch, tmp_path, message):
        import os

        cfg = tmp_path / "provisa.yaml"
        cfg.write_text("sources: []\nai_models:\n  mcp_chat: {vendor: openai, model: gpt-5}\n")
        monkeypatch.setitem(os.environ, "PROVISA_CONFIG", str(cfg))
        monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
        _install_fake_aisuite(
            monkeypatch,
            [SimpleNamespace(choices=[SimpleNamespace(message=message, finish_reason="stop")])],
        )
        convo = [{"role": "user", "content": "q"}]
        return [ev async for ev in chat_mod.run_chat(_state(), "analyst", convo)]

    async def test_aisuite(self, monkeypatch, tmp_path):
        events = await self._aisuite(
            monkeypatch, tmp_path, SimpleNamespace(content="\n\n", tool_calls=None)
        )
        assert [e for e in events if e["type"] == "text"] == [chat_mod._NO_ANSWER]
        assert not [e for e in events if e["type"] == "reasoning"]

    async def test_aisuite_reasoning_goes_out_on_its_own_event(self, monkeypatch, tmp_path):
        # OpenRouter returns a model's reasoning as the message's `reasoning`; it is recorded, not
        # shown as the answer.
        events = await self._aisuite(
            monkeypatch,
            tmp_path,
            SimpleNamespace(content="", tool_calls=None, reasoning="The top path is /products."),
        )
        assert [e for e in events if e["type"] == "reasoning"] == [
            {"type": "reasoning", "text": "The top path is /products."}
        ]
        assert [e for e in events if e["type"] == "text"] == [chat_mod._NO_ANSWER]


@pytest.mark.asyncio
class TestTheGatewayCachesTheConversation:
    """REQ-2012: an Anthropic or Google model behind OpenRouter caches only where a block says so;
    each round's last message carries a breakpoint, as the direct Anthropic path's does, beside
    the system prompt's."""

    async def _calls(self, monkeypatch, tmp_path, model: str) -> list[dict]:
        import os

        cfg = tmp_path / "provisa.yaml"
        cfg.write_text(
            yaml.safe_dump(
                {
                    "sources": [],
                    "ai_endpoints": [
                        {
                            "id": "openrouter",
                            "style": "openai",
                            "base_url": "https://openrouter.ai/api/v1",
                            "api_key_env": "OPENROUTER_API_KEY",
                        }
                    ],
                    "ai_models": {"mcp_chat": {"vendor": "openrouter", "model": model}},
                }
            )
        )
        monkeypatch.setitem(os.environ, "PROVISA_CONFIG", str(cfg))
        monkeypatch.setenv("OPENROUTER_API_KEY", "sk-test")
        monkeypatch.setattr(mcp_tools, "list_schemas", lambda *_a, **_k: _async_result(["sales"]))
        call = SimpleNamespace(
            id="c1", function=SimpleNamespace(name="list_schemas", arguments="{}")
        )
        holder = _install_fake_aisuite(
            monkeypatch,
            [
                SimpleNamespace(
                    choices=[
                        SimpleNamespace(
                            message=SimpleNamespace(content=None, tool_calls=[call]),
                            finish_reason="tool_calls",
                        )
                    ]
                ),
                _stop_reply(),
            ],
        )
        convo = [{"role": "user", "content": "what schemas?"}]
        async for _ in chat_mod.run_chat(_state(), "analyst", convo):
            pass
        return holder["provider"].calls

    @pytest.mark.parametrize("model", ["anthropic/claude-haiku-5.5", "google/gemini-3.8-flash"])
    async def test_each_rounds_last_message_is_a_breakpoint(self, monkeypatch, tmp_path, model):
        first, second = await self._calls(monkeypatch, tmp_path, model)
        assert first["messages"][-1] == {
            "role": "user",
            "content": [
                {"type": "text", "text": "what schemas?", "cache_control": {"type": "ephemeral"}}
            ],
        }
        tool_result = second["messages"][-1]
        assert tool_result["role"] == "tool"
        assert tool_result["content"][-1]["cache_control"] == {"type": "ephemeral"}
        # One on the conversation, beside the system prompt's where it has one: within four.
        marks = [
            part
            for m in second["messages"]
            if isinstance(m["content"], list)
            for part in m["content"]
            if "cache_control" in part
        ]
        assert 1 <= len(marks) <= 2

    async def test_a_model_that_caches_by_itself_gets_none(self, monkeypatch, tmp_path):
        first, second = await self._calls(monkeypatch, tmp_path, "qwen/qwen3.8-flash")
        assert first["messages"][-1] == {"role": "user", "content": "what schemas?"}
        assert isinstance(second["messages"][-1]["content"], str)


def _openai_call_reply(name: str = "list_schemas"):
    call = SimpleNamespace(id="c1", function=SimpleNamespace(name=name, arguments="{}"))
    return SimpleNamespace(
        choices=[
            SimpleNamespace(
                message=SimpleNamespace(content=None, tool_calls=[call]), finish_reason="tool_calls"
            )
        ]
    )


@pytest.mark.asyncio
class TestTheLastRoundIsToldToAnswer:
    """Bake-off run 38639158659: on a last round sent with no tools, OpenRouter's Gemini and Qwen
    routes still returned tool calls (and the server ran them), and Kimi wrote one out as text,
    which went out as its answer."""

    async def _run(self, monkeypatch, tmp_path, replies):
        import os

        cfg = tmp_path / "provisa.yaml"
        cfg.write_text("sources: []\nai_models:\n  mcp_chat: {vendor: openai, model: gpt-5}\n")
        monkeypatch.setitem(os.environ, "PROVISA_CONFIG", str(cfg))
        monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
        ran: list[str] = []

        async def _list_schemas(*_a, **_k):
            ran.append("list_schemas")
            return []

        monkeypatch.setattr(mcp_tools, "list_schemas", _list_schemas)
        holder = _install_fake_aisuite(monkeypatch, replies)
        convo = [{"role": "user", "content": "go"}]
        events = [
            ev async for ev in chat_mod.run_chat(_state(), "analyst", convo, max_iterations=2)
        ]
        return holder["provider"].calls, events, ran

    async def test_the_last_round_carries_the_instruction(self, monkeypatch, tmp_path):
        calls, events, _ = await self._run(
            monkeypatch, tmp_path, [_openai_call_reply(), _stop_reply()]
        )
        assert calls[0]["messages"][-1] != chat_mod._LAST_ROUND_INSTRUCTION
        assert calls[1]["messages"][-1] == chat_mod._LAST_ROUND_INSTRUCTION
        assert [e["text"] for e in events if e["type"] == "text"] == ["hi"]

    async def test_a_call_made_on_the_last_round_is_not_run(self, monkeypatch, tmp_path):
        _calls, events, ran = await self._run(
            monkeypatch, tmp_path, [_openai_call_reply(), _openai_call_reply()]
        )
        assert ran == ["list_schemas"]  # the first round's call only
        assert [e for e in events if e["type"] == "text"] == [chat_mod._STEPS_SPENT]

    async def test_a_tool_call_written_as_text_is_no_answer(self, monkeypatch, tmp_path):
        kimi = SimpleNamespace(
            choices=[
                SimpleNamespace(
                    message=SimpleNamespace(
                        content=' tool="run_sql" index="1" key="sql" type="string"SELECT 1',
                        tool_calls=None,
                    ),
                    finish_reason="stop",
                )
            ]
        )
        _calls, events, _ = await self._run(monkeypatch, tmp_path, [_openai_call_reply(), kimi])
        assert [e for e in events if e["type"] == "text"] == [chat_mod._NO_ANSWER]

    async def test_an_anthropic_call_made_on_the_last_round_is_not_run(self, monkeypatch):
        _install_fake_anthropic(monkeypatch, [_schemas_call("tu1"), _schemas_call("tu2")])
        ran: list[str] = []

        async def _list_schemas(*_a, **_k):
            ran.append("list_schemas")
            return []

        monkeypatch.setattr(mcp_tools, "list_schemas", _list_schemas)
        convo = [{"role": "user", "content": "go"}]
        events = [
            ev async for ev in chat_mod.run_chat(_state(), "analyst", convo, max_iterations=2)
        ]
        assert ran == ["list_schemas"]
        assert [e for e in events if e["type"] == "text"] == [chat_mod._STEPS_SPENT]


def test_an_answer_naming_a_tool_is_still_an_answer():
    assert chat_mod._is_answer("I ran `run_sql` over view_slowest_pages: /reports/top is slowest.")
    assert not chat_mod._is_answer("<tool_call>{}</tool_call>")
    assert not chat_mod._is_answer('{"name": "run_sql", "arguments": {"sql": "SELECT 1"}}')
