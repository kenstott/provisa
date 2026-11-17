# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""On a server of many organizations nothing AI comes from the server's environment (REQ-1395 as
amended 2026-11-16).

The server's environment sets every value a vendor's SDK or aisuite would read: each vendor's key,
Anthropic's auth token and address, OpenAI's address, organization, project and admin key, the
local model servers' addresses, and the variable a deployment's AI endpoint takes its key from.
Every request is caught at the HTTP transport (httpx, and httpx2 under the OpenAI SDK), so each
test says what left the server, if anything:

(a) a vendor's client is built with every value explicit: with nothing stored nothing is sent;
    with a key stored it goes to the vendor's own address carrying that key and nothing of the
    environment's; while the environment sets the headers an SDK sends whatever it is given, no
    client of that SDK is built;
(b) table search is refused for want of a key, rather than answering with the token ranking;
(c) an AI endpoint's key is taken from the deployment only for the sandbox tier's endpoint;
(d) a local model server at the server's own address is refused; an organization's own endpoint
    address is reached.

An install of one organization is driven too: what it sends is unchanged.
"""

# Requirements: REQ-1395, REQ-2006

from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any

import httpx
import httpx2
import pytest

# The API layer says at import whether the deployment holds many organizations; imported here so
# that a test's own answer (the ``many`` and ``one`` fixtures) is not replaced by a later import.
import provisa.api.app as app_mod  # noqa: F401
from provisa.core import org_secrets, secrets_providers
from provisa.core.org_secrets import AI_KEY_ENV, AiKeyRequired
from provisa.core.sandbox_ai import ENDPOINT_ID

_DB = object()

#: What the server's environment sets beside each vendor's key.
_ENVIRONMENT = {
    "ANTHROPIC_AUTH_TOKEN": "env-anthropic-token",
    "ANTHROPIC_BASE_URL": "https://env-anthropic.example",
    "OPENAI_BASE_URL": "https://env-openai.example/v1",
    "OPENAI_ORG_ID": "env-org",
    "OPENAI_PROJECT_ID": "env-project",
    "OPENAI_ADMIN_KEY": "env-admin-key",
    "OLLAMA_API_URL": "http://env-ollama.example:11434",
    "LMSTUDIO_API_URL": "http://env-lmstudio.example:1234",
    "GATEWAY_KEY": "env-gateway-key",
    "GATEWAY_URL": "https://env-gateway.example/v1",
}


def _env_key(vendor: str) -> str:
    return f"env-{vendor}"


@pytest.fixture(autouse=True)
def _the_servers_environment(monkeypatch):
    for vendor, variable in AI_KEY_ENV.items():
        monkeypatch.setenv(variable, _env_key(vendor))
    for variable, value in _ENVIRONMENT.items():
        monkeypatch.setenv(variable, value)
    for variable in ("ANTHROPIC_CUSTOM_HEADERS", "OPENAI_CUSTOM_HEADERS"):
        monkeypatch.delenv(variable, raising=False)


@pytest.fixture
def many(monkeypatch):
    monkeypatch.setattr(secrets_providers, "_many_organisations", lambda: True)


@pytest.fixture
def one(monkeypatch):
    monkeypatch.setattr(secrets_providers, "_many_organisations", lambda: False)


def _stored(monkeypatch, keys: dict[str, str]) -> None:
    """The keys the organization stored, as every site reads them."""

    async def read(_db):
        return dict(keys)

    monkeypatch.setattr(org_secrets, "read_org_api_keys", read)
    import provisa.api.mcp.chat as chat_mod

    monkeypatch.setattr(chat_mod, "read_org_api_keys", read)


# --- the transport -----------------------------------------------------------------------------


def _answer(url: str) -> dict[str, Any]:
    """A model's answer, in the shape the address's wire speaks."""
    if url.endswith("/v1/messages"):
        return {
            "id": "msg_1",
            "type": "message",
            "role": "assistant",
            "model": "m",
            "content": [{"type": "text", "text": "answer"}],
            "stop_reason": "end_turn",
            "stop_sequence": None,
            "usage": {"input_tokens": 1, "output_tokens": 1},
        }
    if url.endswith("/api/chat") or url.endswith("/api/v0/chat/completions"):
        return {"message": {"role": "assistant", "content": "answer"}}
    if url.endswith("/models"):
        return {"data": [{"id": "m1"}]}
    return {
        "id": "c1",
        "object": "chat.completion",
        "created": 0,
        "model": "m",
        "choices": [
            {
                "index": 0,
                "message": {"role": "assistant", "content": "answer"},
                "finish_reason": "stop",
            }
        ],
        "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
    }


def _streamed(message: dict[str, Any]) -> bytes:
    """An Anthropic message as the server-sent events its streaming wire answers with."""
    text = message["content"][0]["text"]
    events = [
        ("message_start", {"type": "message_start", "message": {**message, "content": []}}),
        (
            "content_block_start",
            {
                "type": "content_block_start",
                "index": 0,
                "content_block": {"type": "text", "text": ""},
            },
        ),
        (
            "content_block_delta",
            {
                "type": "content_block_delta",
                "index": 0,
                "delta": {"type": "text_delta", "text": text},
            },
        ),
        ("content_block_stop", {"type": "content_block_stop", "index": 0}),
        (
            "message_delta",
            {
                "type": "message_delta",
                "delta": {"stop_reason": "end_turn", "stop_sequence": None},
                "usage": {"output_tokens": 1},
            },
        ),
        ("message_stop", {"type": "message_stop"}),
    ]
    return "".join(f"event: {name}\ndata: {json.dumps(data)}\n\n" for name, data in events).encode()


class _Sent:
    """Every request that leaves through httpx or httpx2, answered as its wire answers."""

    def __init__(self, monkeypatch):
        self.requests: list[tuple[str, dict[str, str]]] = []
        outer = self

        def record(module: Any, request: Any) -> Any:
            url = str(request.url)
            outer.requests.append((url, {k.lower(): v for k, v in request.headers.items()}))
            body = request.content
            if body and json.loads(body).get("stream") is True:
                return module.Response(
                    200,
                    content=_streamed(_answer(url)),
                    headers={"content-type": "text/event-stream"},
                    request=request,
                )
            return module.Response(
                200,
                content=json.dumps(_answer(url)).encode(),
                headers={"content-type": "application/json"},
                request=request,
            )

        for module in (httpx, httpx2):

            def send(_self, request, *_a, _module=module, **_k):
                return record(_module, request)

            async def asend(_self, request, *_a, _module=module, **_k):
                return record(_module, request)

            monkeypatch.setattr(module.Client, "send", send)
            monkeypatch.setattr(module.AsyncClient, "send", asend)

    def hosts(self) -> list[str]:
        return [httpx.URL(url).host for url, _ in self.requests]

    def carries_nothing_of_the_environment(self) -> bool:
        environment = {*_ENVIRONMENT.values(), *(_env_key(v) for v in AI_KEY_ENV)}
        return not any(
            value in header
            for url, headers in self.requests
            for header in [url, *headers.values()]
            for value in environment
        )


@pytest.fixture
def sent(monkeypatch) -> _Sent:
    return _Sent(monkeypatch)


# --- the sites ---------------------------------------------------------------------------------


def _client(vendor: str, keys: dict[str, str], endpoints: list[dict] | None = None):
    from provisa.llm.client import ProvisaLLMClient

    config = {
        "ai_models": {"sql_generation": {"vendor": vendor, "model": "m"}},
        "ai_endpoints": endpoints or [],
    }
    return ProvisaLLMClient("sql_generation", config=config, api_keys=keys)


class _Budget:
    async def admit(self, _model):
        return None

    def over_cap(self, _usage, _model):
        return None


def _chat(monkeypatch, vendor: str, endpoints: list[dict] | None = None):
    import provisa.api.mcp.chat as chat_mod

    config = {
        "ai_models": {"mcp_chat": {"vendor": vendor, "model": "m"}},
        "ai_endpoints": endpoints or [],
    }

    async def effective(_state):
        return config

    monkeypatch.setattr(chat_mod, "_effective_config", effective)
    return chat_mod


async def _chat_turn(chat_mod, vendor: str) -> list[dict]:
    state = SimpleNamespace(model_db=_DB)
    convo = [{"role": "user", "content": "hi"}]
    if vendor == "anthropic":
        turns = chat_mod._run_chat_anthropic(
            state, "analyst", convo, max_iterations=1, budget=_Budget()
        )
    else:
        turns = chat_mod._run_chat_aisuite(
            state, "analyst", convo, vendor, max_iterations=1, budget=_Budget()
        )
    return [event async for event in turns]


async def _code_source_call(vendor: str, endpoints: list[dict] | None = None) -> None:
    from provisa.codesource import llm_service

    provider, _ = await llm_service._provider({"ai_endpoints": endpoints or []}, _DB, vendor)  # type: ignore[arg-type]
    provider.chat_completions_create("m", [{"role": "user", "content": "hi"}])


# --- (a) every vendor client is built with every value explicit --------------------------------

_VENDOR_ADDRESS = {"openai": "api.openai.com", "anthropic": "api.anthropic.com"}


@pytest.mark.parametrize("vendor", ["openai", "anthropic"])
def test_the_client_sends_nothing_with_nothing_stored_on_a_server_of_many(many, sent, vendor):
    with pytest.raises(AiKeyRequired) as refused:
        _client(vendor, {}).complete_sync("q")
    assert refused.value.code == "ai.key_required"
    assert sent.requests == []


@pytest.mark.parametrize("vendor", ["openai", "anthropic"])
def test_the_client_sends_the_stored_key_and_nothing_of_the_environment(many, sent, vendor):
    assert _client(vendor, {vendor: "stored"}).complete_sync("q") == "answer"
    assert sent.hosts() == [_VENDOR_ADDRESS[vendor]]
    headers = sent.requests[0][1]
    if vendor == "openai":
        assert headers["authorization"] == "Bearer stored"
        assert "openai-organization" not in headers and "openai-project" not in headers
    else:
        assert headers["x-api-key"] == "stored" and "authorization" not in headers
    assert sent.carries_nothing_of_the_environment()


@pytest.mark.parametrize("vendor", ["openai", "anthropic"])
async def test_the_chat_sends_nothing_with_nothing_stored_on_a_server_of_many(
    many, monkeypatch, sent, vendor
):
    _stored(monkeypatch, {})
    chat_mod = _chat(monkeypatch, vendor)
    configured, reason = await chat_mod._llm_configured(SimpleNamespace(model_db=_DB))
    assert configured is False and "Admin, AI Models" in reason
    with pytest.raises(AiKeyRequired) as refused:
        await _chat_turn(chat_mod, vendor)
    assert refused.value.code == "ai.key_required"
    assert sent.requests == []


@pytest.mark.parametrize("vendor", ["openai", "anthropic"])
async def test_the_chat_sends_the_stored_key_and_nothing_of_the_environment(
    many, monkeypatch, sent, vendor
):
    _stored(monkeypatch, {vendor: "stored"})
    chat_mod = _chat(monkeypatch, vendor)
    await _chat_turn(chat_mod, vendor)
    assert sent.hosts() == [_VENDOR_ADDRESS[vendor]]
    assert sent.carries_nothing_of_the_environment()


async def test_the_code_sources_model_sends_the_stored_key_and_nothing_of_the_environment(
    many, monkeypatch, sent
):
    _stored(monkeypatch, {"openai": "stored"})
    await _code_source_call("openai")
    assert sent.hosts() == ["api.openai.com"]
    assert sent.requests[0][1]["authorization"] == "Bearer stored"
    assert sent.carries_nothing_of_the_environment()


@pytest.mark.parametrize("vendor", ["openai", "anthropic"])
def test_no_client_is_built_while_the_environment_sets_its_sdks_headers(
    many, monkeypatch, sent, vendor
):
    variable = f"{vendor.upper()}_CUSTOM_HEADERS"
    monkeypatch.setenv(variable, "X-Env-Header: env-value")
    with pytest.raises(AiKeyRequired) as refused:
        _client(vendor, {vendor: "stored"}).complete_sync("q")
    assert refused.value.code == "ai.server_environment_sets"
    assert refused.value.params == {"vendor": vendor, "variable": variable}
    assert sent.requests == []


def test_an_install_of_one_sends_as_before(one, sent):
    """The deployment's key, and the address and organization its environment names."""
    assert _client("openai", {}).complete_sync("q") == "answer"
    assert sent.hosts() == ["env-openai.example"]
    headers = sent.requests[0][1]
    assert headers["authorization"] == f"Bearer {_env_key('openai')}"
    assert headers["openai-organization"] == "env-org"


# --- (b) table search ------------------------------------------------------------------------------


async def test_table_search_is_refused_rather_than_ranked_by_tokens(many, sent):
    from provisa.discovery.table_search import TableCandidate, search_tables

    candidates = [TableCandidate(name="orders", comment=None, columns=[], schema_name="public")]
    with pytest.raises(AiKeyRequired) as refused:
        await search_tables("orders", candidates, api_keys={})
    assert refused.value.code == "ai.key_required"
    assert sent.requests == []


async def test_table_search_asks_the_model_on_an_install_of_one(one, sent):
    from provisa.discovery.table_search import TableCandidate, search_tables

    candidates = [TableCandidate(name="orders", comment=None, columns=[], schema_name="public")]
    await search_tables("orders", candidates, api_keys={})
    assert len(sent.requests) == 1


# --- (c) only the sandbox tier's endpoint takes its key from the deployment ----------------------


def _gateway(api_key_env: str = "GATEWAY_KEY", base_url: str = "https://gateway.example/v1"):
    return {"id": "gateway", "style": "openai", "base_url": base_url, "api_key_env": api_key_env}


_FROM_THE_ENVIRONMENT = pytest.mark.parametrize(
    "api_key_env", ["GATEWAY_KEY", "${env:GATEWAY_KEY}"]
)


@_FROM_THE_ENVIRONMENT
def test_the_client_refuses_an_endpoints_key_from_the_environment(many, sent, api_key_env):
    endpoint = _gateway(api_key_env)
    client = _client("gateway", {}, [endpoint])
    assert client.key_problem() == "gateway has no API key"
    with pytest.raises(AiKeyRequired) as refused:
        client.complete_sync("q")
    assert refused.value.code == "ai.endpoint_key_from_server"
    assert sent.requests == []


@_FROM_THE_ENVIRONMENT
async def test_the_chat_refuses_an_endpoints_key_from_the_environment(
    many, monkeypatch, sent, api_key_env
):
    _stored(monkeypatch, {})
    chat_mod = _chat(monkeypatch, "gateway", [_gateway(api_key_env)])
    configured, reason = await chat_mod._llm_configured(SimpleNamespace(model_db=_DB))
    assert configured is False and "server's environment" in reason
    with pytest.raises(AiKeyRequired) as refused:
        await _chat_turn(chat_mod, "gateway")
    assert refused.value.code == "ai.endpoint_key_from_server"
    assert sent.requests == []


@_FROM_THE_ENVIRONMENT
async def test_the_code_sources_model_refuses_an_endpoints_key_from_the_environment(
    many, monkeypatch, sent, api_key_env
):
    _stored(monkeypatch, {})
    with pytest.raises(AiKeyRequired) as refused:
        await _code_source_call("gateway", [_gateway(api_key_env)])
    assert refused.value.code == "ai.endpoint_key_from_server"
    assert sent.requests == []


def _ai_models_route(monkeypatch, endpoints):
    from provisa.api.admin import ai_models_router

    async def effective():
        return {"ai_endpoints": endpoints}

    monkeypatch.setattr(ai_models_router, "require_org_settings", lambda request: None)
    monkeypatch.setattr(ai_models_router, "_effective_config", effective)
    monkeypatch.setattr(app_mod, "state", SimpleNamespace(model_db=_DB, org_id="default"))
    return lambda vendor: ai_models_router.get_vendor_models(SimpleNamespace(), vendor)  # type: ignore[arg-type]


def _discovery_route(monkeypatch, endpoints):
    from provisa.api.admin import discovery
    from provisa.core import org_settings

    async def resolved(_db):
        return {"ai_endpoints": endpoints}

    monkeypatch.setattr(discovery, "require_capability_request", lambda request, cap: None)
    monkeypatch.setattr(discovery, "state", SimpleNamespace(model_db=_DB))
    monkeypatch.setattr(org_settings, "resolve_org_config", resolved)
    return lambda vendor: discovery.discovery_vendor_models(SimpleNamespace(), vendor)  # type: ignore[arg-type]


_LISTINGS = pytest.mark.parametrize("route", [_ai_models_route, _discovery_route])


@_LISTINGS
async def test_a_listing_refuses_an_endpoints_key_from_the_environment(
    many, monkeypatch, sent, route
):
    _stored(monkeypatch, {})
    with pytest.raises(AiKeyRequired) as refused:
        await route(monkeypatch, [_gateway()])("gateway")
    assert refused.value.code == "ai.endpoint_key_from_server"
    assert sent.requests == []


@_LISTINGS
async def test_a_listing_reads_an_endpoints_key_from_the_environment_on_an_install_of_one(
    one, monkeypatch, sent, route
):
    _stored(monkeypatch, {})
    assert await route(monkeypatch, [_gateway()])("gateway") == {
        "vendor": "gateway",
        "models": ["m1"],
    }
    assert sent.requests[0][1]["authorization"] == f"Bearer {_ENVIRONMENT['GATEWAY_KEY']}"


def _tier() -> dict[str, Any]:
    return {
        "id": ENDPOINT_ID,
        "style": "openai",
        "base_url": "https://tier.example/v1",
        "api_key_env": "GATEWAY_KEY",
    }


def test_the_sandbox_tiers_endpoint_is_called_with_the_deployments_key(many, sent):
    """Its key is the one the organization's keys carry for it (provisa.core.sandbox_ai)."""
    assert _client(ENDPOINT_ID, {ENDPOINT_ID: "tier-key"}, [_tier()]).complete_sync("q") == (
        "answer"
    )
    assert sent.hosts() == ["tier.example"]
    assert sent.requests[0][1]["authorization"] == "Bearer tier-key"


async def test_the_chat_calls_the_sandbox_tiers_endpoint_with_the_deployments_key(
    many, monkeypatch, sent
):
    _stored(monkeypatch, {ENDPOINT_ID: "tier-key"})
    chat_mod = _chat(monkeypatch, ENDPOINT_ID, [_tier()])
    assert await chat_mod._llm_configured(SimpleNamespace(model_db=_DB)) == (True, "")
    await _chat_turn(chat_mod, ENDPOINT_ID)
    assert sent.hosts() == ["tier.example"]
    assert sent.requests[0][1]["authorization"] == "Bearer tier-key"


def test_an_install_of_one_calls_an_endpoint_with_the_key_its_variable_names(one, sent):
    assert _client("gateway", {}, [_gateway()]).complete_sync("q") == "answer"
    assert sent.requests[0][1]["authorization"] == f"Bearer {_ENVIRONMENT['GATEWAY_KEY']}"


def test_an_endpoint_whose_variable_is_unset_is_refused(one, monkeypatch, sent):
    monkeypatch.delenv("GATEWAY_KEY")
    with pytest.raises(AiKeyRequired) as refused:
        _client("gateway", {}, [_gateway()]).complete_sync("q")
    assert refused.value.code == "ai.endpoint_key_unset"
    assert refused.value.params == {"vendor": "gateway", "field": "GATEWAY_KEY"}
    assert sent.requests == []


# --- (d) no local model server at the server's own address ----------------------------------------

_LOCAL = pytest.mark.parametrize("vendor", ["ollama", "lmstudio"])


@_LOCAL
def test_the_client_refuses_a_local_server_on_a_server_of_many(many, sent, vendor):
    client = _client(vendor, {})
    assert client.key_problem() == f"{vendor} has no API key"
    with pytest.raises(AiKeyRequired) as refused:
        client.complete_sync("q")
    assert refused.value.code == "ai.local_server_from_environment"
    assert sent.requests == []


@_LOCAL
async def test_the_chat_refuses_a_local_server_on_a_server_of_many(many, monkeypatch, sent, vendor):
    _stored(monkeypatch, {})
    chat_mod = _chat(monkeypatch, vendor)
    configured, _reason = await chat_mod._llm_configured(SimpleNamespace(model_db=_DB))
    assert configured is False
    with pytest.raises(AiKeyRequired) as refused:
        await _chat_turn(chat_mod, vendor)
    assert refused.value.code == "ai.local_server_from_environment"
    assert sent.requests == []


@_LOCAL
async def test_the_code_sources_model_refuses_a_local_server_on_a_server_of_many(
    many, monkeypatch, sent, vendor
):
    _stored(monkeypatch, {})
    with pytest.raises(AiKeyRequired) as refused:
        await _code_source_call(vendor)
    assert refused.value.code == "ai.local_server_from_environment"
    assert sent.requests == []


def test_an_endpoint_address_from_the_environment_is_refused(many, sent):
    endpoint = {"id": "gateway", "style": "openai", "base_url": "${env:GATEWAY_URL}"}
    with pytest.raises(AiKeyRequired) as refused:
        _client("gateway", {}, [endpoint]).complete_sync("q")
    assert refused.value.code == "ai.endpoint_address_from_server"
    assert sent.requests == []


def test_an_organizations_own_endpoint_address_is_reached(many, sent):
    endpoint = {"id": "mine", "style": "openai", "base_url": "http://models.acme.example:11434/v1"}
    assert _client("mine", {}, [endpoint]).complete_sync("q") == "answer"
    assert sent.hosts() == ["models.acme.example"]
    assert sent.carries_nothing_of_the_environment()


def test_an_install_of_one_reaches_the_local_server_its_environment_names(one, sent):
    assert _client("ollama", {}).complete_sync("q") == "answer"
    assert sent.hosts() == ["env-ollama.example"]
