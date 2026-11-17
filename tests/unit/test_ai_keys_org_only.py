# Copyright (c) 2026 Kenneth Stott
# Canary: 5d0e7c1a-93b4-4f6e-8a27-b1c64e09d3f8
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""On a server of many organizations an AI key is only the one the organization stored (REQ-1395).

Each site that calls a model vendor, or Jev, is driven three ways: on a server of many
organizations with the vendor's environment variable set and no key stored (the vendor has no key,
the call is refused with the catalogued code, and nothing is handed to the vendor's client); with
a key stored (that key is the one handed over); and on an install of one organization with only
the environment variable (the deployment's key is used). One test walks ``provisa/`` and fails on
any AI key environment variable named outside the resolver, and on a vendor's client constructed
with no key.
"""

# Requirements: REQ-1395

from __future__ import annotations

import ast
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

# The API layer says at import whether the deployment holds many organizations; imported here so
# that a test's own answer (the ``many`` and ``one`` fixtures) is not replaced by a later import.
import provisa.api.app as app_mod
from provisa.core import org_secrets, secrets_providers
from provisa.core.org_secrets import (
    AI_KEY_ENV,
    JEV,
    KEYLESS_VENDORS,
    LLM_VENDORS,
    NO_KEY,
    AiKeyRequired,
)

_PACKAGE = Path(org_secrets.__file__).resolve().parents[1]
_RESOLVER = Path(org_secrets.__file__).resolve()
_DB = object()


def _env_key(vendor: str) -> str:
    return f"env-{vendor}"


@pytest.fixture(autouse=True)
def _every_ai_key_in_the_environment(monkeypatch):
    """The server's environment holds a key for every vendor and for Jev."""
    for vendor, variable in AI_KEY_ENV.items():
        monkeypatch.setenv(variable, _env_key(vendor))


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


# --- the resolver ----------------------------------------------------------------------------------


def test_the_resolver_names_a_variable_for_every_key_taking_vendor_and_jev():
    assert set(AI_KEY_ENV) == LLM_VENDORS | {JEV}


@pytest.mark.parametrize("vendor", sorted(AI_KEY_ENV))
def test_on_a_server_of_many_the_environment_key_is_not_a_key(many, vendor):
    assert org_secrets.ai_key_or_none({}, vendor) is None
    with pytest.raises(AiKeyRequired) as refused:
        org_secrets.ai_key({}, vendor)
    assert refused.value.code == "ai.key_required"
    assert refused.value.params == {"vendor": vendor}
    assert "Admin, AI Models" in str(refused.value) and "REQ" not in str(refused.value)


@pytest.mark.parametrize("vendor", sorted(AI_KEY_ENV))
def test_on_a_server_of_many_the_stored_key_is_the_key(many, vendor):
    assert org_secrets.ai_key({vendor: "stored"}, vendor) == "stored"


@pytest.mark.parametrize("vendor", sorted(AI_KEY_ENV))
def test_an_install_of_one_reads_the_environment_when_none_is_stored(one, vendor):
    assert org_secrets.ai_key({}, vendor) == _env_key(vendor)
    assert org_secrets.ai_key({vendor: "stored"}, vendor) == "stored"


def test_an_install_of_one_with_no_key_anywhere_is_refused(one, monkeypatch):
    monkeypatch.delenv("OPENAI_API_KEY")
    with pytest.raises(AiKeyRequired):
        org_secrets.ai_key({}, "openai")


@pytest.mark.parametrize("vendor", ["google", "aws", "azure", "huggingface", "watsonx"])
def test_a_vendor_reached_with_the_servers_credentials_is_refused_on_a_server_of_many(many, vendor):
    with pytest.raises(AiKeyRequired) as refused:
        org_secrets.provider_key({}, vendor)
    assert refused.value.code == "ai.vendor_uses_server_credentials"


def test_such_a_vendor_takes_no_key_from_here_on_an_install_of_one(one):
    assert org_secrets.provider_key({}, "google") is None


@pytest.mark.parametrize("vendor", sorted(KEYLESS_VENDORS))
def test_a_local_server_takes_no_key_on_an_install_of_one(one, vendor):
    assert org_secrets.provider_key({}, vendor) is None


@pytest.mark.parametrize("vendor", sorted(KEYLESS_VENDORS))
def test_a_local_server_is_refused_on_a_server_of_many(many, vendor):
    with pytest.raises(AiKeyRequired) as refused:
        org_secrets.provider_key({}, vendor)
    assert refused.value.code == "ai.local_server_from_environment"


def test_an_endpoint_with_no_key_is_given_a_placeholder_on_a_server_of_many(many):
    assert org_secrets.endpoint_provider_key(None) == NO_KEY
    assert org_secrets.endpoint_provider_key("its-own") == "its-own"


def test_an_endpoint_with_no_key_is_left_as_it_is_on_an_install_of_one(one):
    assert org_secrets.endpoint_provider_key(None) is None


async def test_the_org_forms_read_the_organizations_store(many, monkeypatch):
    _stored(monkeypatch, {"openai": "stored", JEV: "jev-stored"})
    assert await org_secrets.org_ai_key(_DB, "openai") == "stored"  # type: ignore[arg-type]
    assert await org_secrets.org_ai_key_or_none(_DB, JEV) == "jev-stored"  # type: ignore[arg-type]
    assert await org_secrets.org_ai_key_or_none(_DB, "anthropic") is None  # type: ignore[arg-type]
    assert await org_secrets.org_ai_key_or_none(None, JEV) is None


# --- the LLM client (every model call but the chat's) ------------------------------------------------


def _openai_explicit(config: dict[str, Any]) -> dict[str, Any]:
    """``config`` as provisa.core.org_secrets.sdk_config gives an OpenAI client it on a server of
    many organizations, with its omitted headers named."""
    return {
        "base_url": "https://api.openai.com/v1",
        **config,
        "admin_api_key": "",
        "default_headers": {"OpenAI-Organization": "omitted", "OpenAI-Project": "omitted"},
    }


def _named(configs: Any) -> Any:
    """``configs`` with each of the OpenAI SDK's Omit markers named, so two compare."""
    from openai import Omit

    if isinstance(configs, dict):
        return {k: _named(v) for k, v in configs.items()}
    if isinstance(configs, list):
        return [_named(v) for v in configs]
    if isinstance(configs, tuple):
        return tuple(_named(v) for v in configs)
    return "omitted" if isinstance(configs, Omit) else configs


class _Aisuite:
    """aisuite's client, recording the configuration each was constructed with."""

    def __init__(self, monkeypatch):
        self.configs: list[Any] = []
        outer = self

        class Client:
            def __init__(self, config: Any = None):
                outer.configs.append(config)
                message = SimpleNamespace(content="answer")
                answer = SimpleNamespace(
                    choices=[SimpleNamespace(message=message, finish_reason="stop")], usage=None
                )
                self.chat = SimpleNamespace(
                    completions=SimpleNamespace(create=lambda **_kwargs: answer)
                )

        import aisuite

        monkeypatch.setattr(aisuite, "Client", Client)


def _client(vendor: str, keys: dict[str, str] | None, endpoints: list[dict] | None = None):
    from provisa.llm.client import ProvisaLLMClient

    config = {
        "ai_models": {"sql_generation": {"vendor": vendor, "model": "m"}},
        "ai_endpoints": endpoints or [],
    }
    return ProvisaLLMClient("sql_generation", config=config, api_keys=keys)


@pytest.mark.parametrize("keys", [None, {}])
def test_the_client_makes_no_call_with_the_environments_key_on_a_server_of_many(
    many, monkeypatch, keys
):
    sent = _Aisuite(monkeypatch)
    client = _client("openai", keys)
    assert client.key_problem() == "openai has no API key"
    with pytest.raises(AiKeyRequired) as refused:
        client.complete_sync("q")
    assert refused.value.code == "ai.key_required"
    assert sent.configs == []


def test_the_client_sends_the_stored_key_on_a_server_of_many(many, monkeypatch):
    sent = _Aisuite(monkeypatch)
    client = _client("openai", {"openai": "stored"})
    assert client.key_problem() is None
    assert client.complete_sync("q") == "answer"
    assert _named(sent.configs) == [{"openai": _openai_explicit({"api_key": "stored"})}]


def test_the_client_sends_the_deployments_key_on_an_install_of_one(one, monkeypatch):
    sent = _Aisuite(monkeypatch)
    client = _client("openai", {})
    assert client.key_problem() is None
    assert client.complete_sync("q") == "answer"
    assert sent.configs == [{"openai": {"api_key": _env_key("openai")}}]


def test_the_clients_refusal_is_reported_by_its_catalogued_code(many):
    from provisa.llm.client import failure

    with pytest.raises(AiKeyRequired) as refused:
        _client("mistral", {}).complete_sync("q")
    said = failure(refused.value)
    assert said["code"] == "ai.key_required" and said["params"]["vendor"] == "mistral"


def test_the_client_refuses_vertex_on_a_server_of_many(many, monkeypatch):
    sent = _Aisuite(monkeypatch)
    client = _client("google", {})
    assert client.key_problem() == "google has no API key"
    with pytest.raises(AiKeyRequired) as refused:
        client.complete_sync("q")
    assert refused.value.code == "ai.vendor_uses_server_credentials"
    assert sent.configs == []


def test_the_client_never_lets_an_endpoint_with_no_key_read_the_vendors(many, monkeypatch):
    sent = _Aisuite(monkeypatch)
    endpoint = {"id": "gateway", "style": "openai", "base_url": "https://gateway.example/v1"}
    assert _client("gateway", {}, [endpoint]).complete_sync("q") == "answer"
    assert _named(sent.configs) == [
        {"openai": _openai_explicit({"base_url": "https://gateway.example/v1", "api_key": NO_KEY})}
    ]


async def test_table_search_is_refused_with_no_model_call_on_a_server_of_many(many, monkeypatch):
    from provisa.discovery.table_search import TableCandidate, llm_rank

    sent = _Aisuite(monkeypatch)
    candidates = [TableCandidate(name="orders", comment=None, columns=[], schema_name="public")]
    with pytest.raises(AiKeyRequired) as refused:
        await llm_rank("orders", candidates, api_keys={})
    assert refused.value.code == "ai.key_required"
    assert sent.configs == []


# --- listing a vendor's models (Admin, AI Models and the discovery dialog) -------------------------


class _Http:
    """httpx's client, recording the headers of each request made."""

    def __init__(self, monkeypatch):
        self.headers: list[dict[str, str]] = []
        outer = self

        class AsyncClient:
            def __init__(self, **_kwargs):
                pass

            async def __aenter__(self):
                return self

            async def __aexit__(self, *_exc):
                return False

            async def get(self, _url, headers):
                outer.headers.append(headers)
                return SimpleNamespace(
                    raise_for_status=lambda: None, json=lambda: {"data": [{"id": "m1"}]}
                )

        import httpx

        monkeypatch.setattr(httpx, "AsyncClient", AsyncClient)


def _ai_models_route(monkeypatch):
    from provisa.api.admin import ai_models_router

    monkeypatch.setattr(ai_models_router, "require_org_settings", lambda request: None)
    monkeypatch.setattr(app_mod, "state", SimpleNamespace(model_db=_DB))
    return lambda vendor: ai_models_router.get_vendor_models(SimpleNamespace(), vendor)  # type: ignore[arg-type]


def _discovery_route(monkeypatch):
    from provisa.api.admin import discovery

    monkeypatch.setattr(discovery, "require_capability_request", lambda request, cap: None)
    monkeypatch.setattr(discovery, "state", SimpleNamespace(model_db=_DB))
    return lambda vendor: discovery.discovery_vendor_models(SimpleNamespace(), vendor)  # type: ignore[arg-type]


_LISTINGS = pytest.mark.parametrize("route", [_ai_models_route, _discovery_route])


@_LISTINGS
async def test_a_listing_is_refused_with_no_request_on_a_server_of_many(many, monkeypatch, route):
    _stored(monkeypatch, {})
    sent = _Http(monkeypatch)
    with pytest.raises(AiKeyRequired) as refused:
        await route(monkeypatch)("anthropic")
    assert refused.value.code == "ai.key_required" and refused.value.vendor == "anthropic"
    assert sent.headers == []


@_LISTINGS
async def test_a_listing_sends_the_stored_key_on_a_server_of_many(many, monkeypatch, route):
    _stored(monkeypatch, {"anthropic": "stored"})
    sent = _Http(monkeypatch)
    assert await route(monkeypatch)("anthropic") == {"vendor": "anthropic", "models": ["m1"]}
    assert [h["x-api-key"] for h in sent.headers] == ["stored"]


@_LISTINGS
async def test_a_listing_sends_the_deployments_key_on_an_install_of_one(one, monkeypatch, route):
    _stored(monkeypatch, {})
    sent = _Http(monkeypatch)
    await route(monkeypatch)("openai")
    assert [h["Authorization"] for h in sent.headers] == [f"Bearer {_env_key('openai')}"]


def test_the_api_answers_the_refusal_with_its_code_and_where_to_add_a_key(many, monkeypatch):
    from fastapi.testclient import TestClient

    from fastapi import FastAPI

    from provisa.api.admin import ai_models_router

    handler = app_mod.create_app().exception_handlers[AiKeyRequired]
    _stored(monkeypatch, {})
    sent = _Http(monkeypatch)
    monkeypatch.setattr(ai_models_router, "require_org_settings", lambda request: None)
    monkeypatch.setattr(app_mod, "state", SimpleNamespace(model_db=_DB))
    app = FastAPI()
    app.include_router(ai_models_router.router)
    app.add_exception_handler(AiKeyRequired, handler)
    answer = TestClient(app).get("/admin/ai-models/vendors/openai/models")
    assert answer.status_code == 409
    assert answer.json() == {
        "detail": "openai has no API key. Add one under Admin, AI Models.",
        "code": "ai.key_required",
        "params": {"vendor": "openai"},
    }
    assert sent.headers == []


# --- the chat --------------------------------------------------------------------------------------


def _chat(monkeypatch, tmp_path, vendor: str):
    import provisa.api.mcp.chat as chat_mod

    config = tmp_path / "provisa.yaml"
    config.write_text(f"sources: []\nai_models:\n  mcp_chat: {{vendor: {vendor}, model: m}}\n")
    monkeypatch.setenv("PROVISA_CONFIG", str(config))
    return chat_mod


def _anthropic_sdk(monkeypatch) -> list[dict]:
    made: list[dict] = []

    def construct(*args, **kwargs):
        made.append({"args": args, **kwargs})
        raise _Constructed

    import anthropic

    monkeypatch.setattr(anthropic, "AsyncAnthropic", construct)
    return made


class _Constructed(Exception):
    """The vendor's client was constructed: as far as these tests follow a chat."""


async def _run_anthropic_chat(chat_mod) -> None:
    turns = chat_mod._run_chat_anthropic(
        SimpleNamespace(), "analyst", [{"role": "user", "content": "hi"}],
        max_iterations=1, budget=None,
    )  # fmt: skip
    async for _ in turns:
        pass


@pytest.mark.parametrize("vendor", ["anthropic", "openai", "google"])
async def test_the_chat_is_not_configured_by_the_environment_on_a_server_of_many(
    many, monkeypatch, tmp_path, vendor
):
    chat_mod = _chat(monkeypatch, tmp_path, vendor)
    configured, reason = await chat_mod._llm_configured(SimpleNamespace())
    assert configured is False and "Admin, AI Models" in reason


async def test_the_chat_constructs_no_client_with_no_stored_key_on_a_server_of_many(
    many, monkeypatch, tmp_path
):
    chat_mod = _chat(monkeypatch, tmp_path, "anthropic")
    made = _anthropic_sdk(monkeypatch)
    with pytest.raises(AiKeyRequired):
        await _run_anthropic_chat(chat_mod)
    assert made == []
    with pytest.raises(AiKeyRequired):
        await chat_mod._resolve_aisuite_routing(SimpleNamespace(), "openai", "m")


async def test_the_chat_hands_its_client_the_stored_key_on_a_server_of_many(
    many, monkeypatch, tmp_path
):
    chat_mod = _chat(monkeypatch, tmp_path, "anthropic")
    _stored(monkeypatch, {"anthropic": "stored", "openai": "stored-openai"})
    state = SimpleNamespace(model_db=_DB)

    async def config(_state):
        return {"ai_models": {"mcp_chat": {"vendor": "anthropic", "model": "m"}}}

    monkeypatch.setattr(chat_mod, "_effective_config", config)
    assert await chat_mod._llm_configured(state) == (True, "")
    assert await chat_mod._vendor_key(state, "anthropic") == "stored"
    assert await chat_mod._resolve_aisuite_routing(state, "openai", "m") == (
        "openai:m",
        {"openai": {"api_key": "stored-openai"}},
    )


async def test_the_chat_hands_its_client_the_deployments_key_on_an_install_of_one(
    one, monkeypatch, tmp_path
):
    chat_mod = _chat(monkeypatch, tmp_path, "anthropic")
    made = _anthropic_sdk(monkeypatch)
    with pytest.raises(_Constructed):
        await _run_anthropic_chat(chat_mod)
    assert made == [{"args": (), "api_key": _env_key("anthropic")}]
    assert await chat_mod._resolve_aisuite_routing(SimpleNamespace(), "openai", "m") == (
        "openai:m",
        {"openai": {"api_key": _env_key("openai")}},
    )


# --- Jev -------------------------------------------------------------------------------------------


def _jev_calls(monkeypatch) -> list[str]:
    keys: list[str] = []

    async def evaluate(key, _state, _questions):
        keys.append(key)
        return {"answers": {}}

    monkeypatch.setattr("provisa.jev.client.evaluate", evaluate)
    return keys


def _mcp_state() -> Any:
    from tests.role_state import RoleState

    context = SimpleNamespace(tables={}, joins={})
    return RoleState(
        contexts={"analyst": context},
        config=SimpleNamespace(roles=[SimpleNamespace(id="analyst", domain_access=["sales"])]),
    )


async def test_the_jev_tool_is_refused_with_no_call_on_a_server_of_many(many, monkeypatch):
    from provisa.api.mcp import tools

    called = _jev_calls(monkeypatch)
    with pytest.raises(AiKeyRequired) as refused:
        await tools.jev_evaluate(_mcp_state(), "analyst", None, [{"id": "q", "type": "noul"}])
    assert refused.value.code == "ai.key_required" and refused.value.vendor == JEV
    assert called == []


async def test_the_jev_tool_calls_with_the_stored_key_on_a_server_of_many(many, monkeypatch):
    from provisa.api.mcp import tools

    _stored(monkeypatch, {JEV: "jev-stored"})
    called = _jev_calls(monkeypatch)
    state = _mcp_state()
    state.model_db = _DB
    await tools.jev_evaluate(state, "analyst", None, [{"id": "q", "type": "noul"}])
    assert called == ["jev-stored"]


async def test_the_jev_tool_calls_with_the_deployments_key_on_an_install_of_one(one, monkeypatch):
    from provisa.api.mcp import tools

    called = _jev_calls(monkeypatch)
    await tools.jev_evaluate(_mcp_state(), "analyst", None, [{"id": "q", "type": "noul"}])
    assert called == [_env_key(JEV)]


async def _listed_tools(state: Any) -> set[str]:
    from provisa.api.mcp.server import build_mcp_server

    return {tool.name for tool in await build_mcp_server(state).list_tools()}


async def test_the_jev_tool_is_listed_only_to_an_organization_with_a_jev_key(many, monkeypatch):
    assert "jev_evaluate" not in await _listed_tools(SimpleNamespace())
    _stored(monkeypatch, {JEV: "jev-stored"})
    assert "jev_evaluate" in await _listed_tools(SimpleNamespace(model_db=_DB))


async def test_the_jev_tool_is_listed_on_an_install_of_one_with_the_deployments_key(one):
    assert "jev_evaluate" in await _listed_tools(SimpleNamespace())


async def test_discovery_is_not_rescored_by_jev_on_a_server_of_many(many, monkeypatch):
    from provisa.discovery import run

    _stored(monkeypatch, {})
    assert await run._refiner(SimpleNamespace(model_db=_DB), {}) is None
    _stored(monkeypatch, {JEV: "jev-stored"})
    assert await run._refiner(SimpleNamespace(model_db=_DB), {}) is not None


async def test_discovery_is_rescored_by_jev_on_an_install_of_one(one, monkeypatch):
    from provisa.discovery import run

    _stored(monkeypatch, {})
    assert await run._refiner(SimpleNamespace(model_db=_DB), {}) is not None


# --- the code source's model service ---------------------------------------------------------------


def _providers(monkeypatch) -> list[tuple[str, dict]]:
    made: list[tuple[str, dict]] = []

    def create(vendor, config):
        made.append((vendor, config))
        return object()

    from aisuite.provider import ProviderFactory

    monkeypatch.setattr(ProviderFactory, "create_provider", staticmethod(create))
    return made


async def test_the_code_sources_provider_is_refused_on_a_server_of_many(many, monkeypatch):
    from provisa.codesource import llm_service

    _stored(monkeypatch, {})
    made = _providers(monkeypatch)
    with pytest.raises(AiKeyRequired):
        await llm_service._provider({}, _DB, "openai")  # type: ignore[arg-type]
    assert made == []


async def test_the_code_sources_provider_is_given_the_stored_key(many, monkeypatch):
    from provisa.codesource import llm_service

    _stored(monkeypatch, {"openai": "stored"})
    made = _providers(monkeypatch)
    await llm_service._provider({}, _DB, "openai")  # type: ignore[arg-type]
    assert _named(made) == [("openai", _openai_explicit({"api_key": "stored"}))]


async def test_the_code_sources_provider_is_given_the_deployments_key_on_an_install_of_one(
    one, monkeypatch
):
    from provisa.codesource import llm_service

    _stored(monkeypatch, {})
    made = _providers(monkeypatch)
    await llm_service._provider({}, _DB, "openai")  # type: ignore[arg-type]
    assert made == [("openai", {"api_key": _env_key("openai")})]


# --- the walk --------------------------------------------------------------------------------------

#: What builds a vendor's client: the SDKs' own clients, and aisuite's client and provider factory,
#: which build them. Each reads from the environment whatever it is not given (a key, an
#: address, an organization id).
_SDK_CLIENTS = frozenset({"Anthropic", "AsyncAnthropic", "OpenAI", "AsyncOpenAI"})
_AISUITE_BUILDERS = frozenset({"Client", "create_provider"})


def _sources() -> list[Path]:
    return sorted(p for p in _PACKAGE.rglob("*.py") if p.resolve() != _RESOLVER)


def test_no_ai_key_variable_is_named_outside_the_resolver():
    named = [
        f"{path.relative_to(_PACKAGE.parent)}: {variable}"
        for path in _sources()
        for variable in sorted(set(AI_KEY_ENV.values()))
        if variable in path.read_text(encoding="utf-8")
    ]
    assert named == [], (
        "An AI key's environment variable is read only by provisa.core.org_secrets "
        f"(ai_key_or_none); these files name one: {named}"
    )


def _builds_a_vendor_client(node: ast.Call) -> str | None:
    called = node.func
    if isinstance(called, ast.Attribute):
        if called.attr in _SDK_CLIENTS:
            return called.attr
        owner = called.value
        owner_name = owner.id if isinstance(owner, ast.Name) else getattr(owner, "attr", None)
        if called.attr == "Client" and owner_name in {"ai", "aisuite"}:
            return f"{owner_name}.Client"
        if called.attr == "create_provider":
            return "create_provider"
        return None
    name = getattr(called, "id", None)
    return name if name in _SDK_CLIENTS else None


def _given_sdk_config(node: ast.Call) -> bool:
    """Whether the call's arguments are built by provisa.core.org_secrets.sdk_config."""
    for argument in [*node.args, *(k.value for k in node.keywords)]:
        for inner in ast.walk(argument):
            if isinstance(inner, ast.Call) and getattr(inner.func, "id", None) == "sdk_config":
                return True
    return False


def test_no_vendor_client_is_built_without_every_value_explicit():
    """REQ-1395 as amended 2026-11-16: every vendor client is built from the resolver's
    configuration (provisa.core.org_secrets.sdk_config), so on a server of many organizations no
    key, token, address or organization id is picked up from the server's environment."""
    bare = []
    for path in _sources():
        text = path.read_text(encoding="utf-8")
        if not any(name in text for name in (*_SDK_CLIENTS, *_AISUITE_BUILDERS)):
            continue
        for node in ast.walk(ast.parse(text)):
            if not isinstance(node, ast.Call):
                continue
            name = _builds_a_vendor_client(node)
            if name is not None and not _given_sdk_config(node):
                bare.append(f"{path.relative_to(_PACKAGE.parent)}:{node.lineno}: {name}()")
    assert bare == [], (
        "A vendor's client built from anything but provisa.core.org_secrets.sdk_config takes "
        f"what it is not given from the server's environment: {bare}"
    )
