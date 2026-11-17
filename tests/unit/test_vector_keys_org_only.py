# Copyright (c) 2026 Kenneth Stott
# Canary: 3f0c9b1e-6a2d-4b7e-9e51-2c8d7a4f1b60
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""REQ-1395 as amended 2026-11-16: an embedding model follows the rule every other AI call does.
On a server of many organizations no key or address an embedding request carries comes from the
server's environment; an install of one organization keeps reading it.

Every request is caught at the httpx transport, so each test sees exactly what would have left
the server, or that nothing did.
"""

# Requirements: REQ-1395, REQ-420

from __future__ import annotations

import json
import sys
from types import ModuleType
from typing import Any

import httpx
import pytest

# The API layer says at import which org is the deployment's own (the held-org refusals ask it);
# imported here so that a test's own answer (the ``many`` and ``one`` fixtures) is not replaced by
# a later import.
import provisa.api.app  # noqa: F401
from provisa.core import secrets, secrets_providers
from provisa.core.org_secrets import AiKeyRequired
from provisa.vector.providers import HuggingFaceLocalProvider, get_provider
from provisa.vector.registry import VectorModel

_ENVIRONMENT = {
    "EMBED_KEY": "env-embed-key",
    "EMBED_URL": "https://env-embed.example/v1",
    "HF_TOKEN": "env-hf-token",
}


@pytest.fixture(autouse=True)
def _the_servers_environment(monkeypatch):
    for variable, value in _ENVIRONMENT.items():
        monkeypatch.setenv(variable, value)


@pytest.fixture
def many(monkeypatch):
    monkeypatch.setattr(secrets_providers, "_many_organisations", lambda: True)


@pytest.fixture
def one(monkeypatch):
    monkeypatch.setattr(secrets_providers, "_many_organisations", lambda: False)


def _answer(url: str) -> dict[str, Any]:
    if url.endswith("/api/embeddings"):
        return {"embedding": [0.1, 0.2]}
    if url.endswith("/api/tags"):
        return {"models": [{"name": "nomic:latest", "digest": "sha-1"}]}
    return {"data": [{"embedding": [0.1, 0.2]}], "usage": {"prompt_tokens": 3}}


class _Sent:
    """Every request that leaves through httpx, answered as its wire answers."""

    def __init__(self, monkeypatch):
        self.requests: list[tuple[str, dict[str, str]]] = []
        outer = self

        async def asend(_self, request, *_a, **_k):
            url = str(request.url)
            outer.requests.append((url, {k.lower(): v for k, v in request.headers.items()}))
            return httpx.Response(
                200,
                content=json.dumps(_answer(url)).encode(),
                headers={"content-type": "application/json"},
                request=request,
            )

        monkeypatch.setattr(httpx.AsyncClient, "send", asend)

    def hosts(self) -> list[str]:
        return [httpx.URL(url).host for url, _ in self.requests]

    def carries_nothing_of_the_environment(self) -> bool:
        return not any(
            value in header
            for url, headers in self.requests
            for header in [url, *headers.values()]
            for value in _ENVIRONMENT.values()
        )


@pytest.fixture
def sent(monkeypatch) -> _Sent:
    return _Sent(monkeypatch)


def _openai(key: str | None = None, base_url: str | None = "https://embed.example/v1"):
    return VectorModel(
        id="text-embedding-3-small",
        provider="openai",
        dimensions=2,
        api_key_env=key,
        base_url=base_url,
    )


def _ollama(base_url: str | None = None):
    return VectorModel(id="nomic", provider="ollama", dimensions=2, base_url=base_url)


def _vault(monkeypatch, held: dict[str, str]) -> None:
    """The organization's vault, holding ``held`` by reference."""

    async def resolve(field):
        return held.get(field)

    monkeypatch.setattr(secrets, "resolve_api_key_field", resolve)


# --- the key -------------------------------------------------------------------------------------


_FROM_THE_ENVIRONMENT = pytest.mark.parametrize("field", ["EMBED_KEY", "${env:EMBED_KEY}"])


@_FROM_THE_ENVIRONMENT
async def test_a_key_from_the_servers_environment_is_refused_on_a_server_of_many(many, sent, field):
    with pytest.raises(AiKeyRequired) as refused:
        await get_provider("openai").embed(["q"], _openai(field))
    assert refused.value.code == "ai.vector_model_key_from_server"
    assert refused.value.params == {"vendor": "text-embedding-3-small"}
    assert sent.requests == []


@_FROM_THE_ENVIRONMENT
async def test_an_install_of_one_sends_the_key_its_environment_holds(one, sent, field):
    assert await get_provider("openai").embed(["q"], _openai(field)) == [[0.1, 0.2]]
    assert sent.hosts() == ["embed.example"]
    assert sent.requests[0][1]["authorization"] == "Bearer env-embed-key"


async def test_an_organizations_own_key_in_its_vault_is_sent(many, monkeypatch, sent):
    _vault(monkeypatch, {"${secret:embed_key}": "vault-key"})
    vectors, tokens = await get_provider("openai").embed_with_usage(
        ["q"], _openai("${secret:embed_key}")
    )
    assert (vectors, tokens) == ([[0.1, 0.2]], 3)
    assert sent.hosts() == ["embed.example"]
    assert sent.requests[0][1]["authorization"] == "Bearer vault-key"
    assert sent.carries_nothing_of_the_environment()


async def test_a_key_the_vault_does_not_hold_is_refused(many, monkeypatch, sent):
    _vault(monkeypatch, {})
    with pytest.raises(AiKeyRequired) as refused:
        await get_provider("openai").embed(["q"], _openai("${secret:embed_key}"))
    assert refused.value.code == "ai.vector_model_key_unset"
    assert refused.value.params == {
        "vendor": "text-embedding-3-small",
        "field": "${secret:embed_key}",
    }
    assert sent.requests == []


async def test_a_key_variable_that_is_unset_is_refused_on_an_install_of_one(one, monkeypatch, sent):
    monkeypatch.delenv("EMBED_KEY")
    with pytest.raises(AiKeyRequired) as refused:
        await get_provider("openai").embed(["q"], _openai("EMBED_KEY"))
    assert refused.value.code == "ai.vector_model_key_unset"
    assert sent.requests == []


async def test_a_model_that_declares_no_key_is_sent_none(many, sent):
    await get_provider("openai").embed(["q"], _openai(None))
    assert "authorization" not in sent.requests[0][1]
    assert sent.carries_nothing_of_the_environment()


# --- the address ---------------------------------------------------------------------------------


async def test_an_address_from_the_servers_environment_is_refused_on_a_server_of_many(
    many, monkeypatch, sent
):
    _vault(monkeypatch, {"${secret:embed_key}": "vault-key"})
    with pytest.raises(AiKeyRequired) as refused:
        await get_provider("openai").embed(
            ["q"], _openai("${secret:embed_key}", base_url="${env:EMBED_URL}")
        )
    assert refused.value.code == "ai.vector_model_address_from_server"
    assert sent.requests == []


async def test_an_openai_model_with_no_address_reaches_openai(many, monkeypatch, sent):
    _vault(monkeypatch, {"${secret:embed_key}": "vault-key"})
    await get_provider("openai").embed(["q"], _openai("${secret:embed_key}", base_url=None))
    assert sent.hosts() == ["api.openai.com"]


async def test_ollama_at_the_servers_own_address_is_refused_on_a_server_of_many(many, sent):
    provider = get_provider("ollama")
    with pytest.raises(AiKeyRequired) as refused:
        await provider.embed(["q"], _ollama())
    assert refused.value.code == "ai.vector_model_local_server"
    with pytest.raises(AiKeyRequired) as refused:
        await provider.reported_version(_ollama())
    assert refused.value.code == "ai.vector_model_local_server"
    assert sent.requests == []


async def test_ollama_at_an_organizations_own_address_is_reached(many, sent):
    model = _ollama("http://ollama.example:11434")
    assert await get_provider("ollama").embed(["q"], model) == [[0.1, 0.2]]
    assert await get_provider("ollama").reported_version(model) == "sha-1"
    assert sent.hosts() == ["ollama.example", "ollama.example"]


async def test_an_install_of_one_reaches_its_own_ollama(one, sent):
    assert await get_provider("ollama").embed(["q"], _ollama()) == [[0.1, 0.2]]
    assert sent.hosts() == ["localhost"]


# --- a local HuggingFace model -------------------------------------------------------------------


@pytest.fixture
def loaded(monkeypatch) -> list[dict[str, Any]]:
    """sentence-transformers, recording what each model is loaded with."""
    calls: list[dict[str, Any]] = []

    class SentenceTransformer:
        def __init__(self, model_id, **kwargs):
            calls.append({"id": model_id, **kwargs})

        def encode(self, texts):
            return [[0.5, 0.5] for _ in texts]

    module = ModuleType("sentence_transformers")
    module.SentenceTransformer = SentenceTransformer  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "sentence_transformers", module)
    monkeypatch.setattr(HuggingFaceLocalProvider, "_cache", {})
    return calls


def _local():
    return VectorModel(
        id="sentence-transformers/all-MiniLM-L6-v2", provider="huggingface", dimensions=2
    )


async def test_a_local_model_is_fetched_with_no_server_token_on_a_server_of_many(many, loaded):
    """huggingface_hub sends the server's HF_TOKEN with a download when it is given no token."""
    assert await HuggingFaceLocalProvider().embed(["q"], _local()) == [[0.5, 0.5]]
    assert loaded == [{"id": "sentence-transformers/all-MiniLM-L6-v2", "token": False}]


async def test_an_install_of_one_fetches_a_local_model_as_its_environment_says(one, loaded):
    await HuggingFaceLocalProvider().embed(["q"], _local())
    assert loaded == [{"id": "sentence-transformers/all-MiniLM-L6-v2", "token": None}]
