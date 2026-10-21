# Copyright (c) 2026 Kenneth Stott
# Canary: 65f86817-3eb8-4dad-ae73-9a53eb941dda
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Provisa's OpenAI-compatible LLM service for a code source's worker (REQ-2105): a model named by
its AI Models role or by a model the org assigns, routed as the org's AI calls are, what goes out
redacted, each call metered and audited for its source, a capped source held to priced models and
to its cap, no streaming -- and any OpenAI SDK reaching it through the egress proxy with no key."""

# Requirements: REQ-2105

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from provisa.codesource import llm_service, services
from provisa.codesource.bundle import BundleRefused
from provisa.codesource.egress import EgressProxy
from provisa.core import secrets_store

pytestmark = pytest.mark.unit

_CFG = {
    "ai_models": {
        "sql_generation": {"vendor": "openrouter-gw", "model": "anthropic/claude-haiku-5.5"},
        "table_description": {"vendor": "anthropic", "model": "claude-haiku-5-5"},
        "column_description": {"vendor": "anthropic"},  # assigns no model
    },
    "ai_endpoints": [
        {"id": "openrouter-gw", "style": "openai", "base_url": "https://openrouter.ai/api/v1"}
    ],
    "vector_models": [
        {"id": "text-embedding-3-small", "provider": "openai", "dimensions": 3},
    ],
}


class _Provider:
    def __init__(self, held: dict) -> None:
        self.held = held

    def chat_completions_create(self, model, messages, **params):
        self.held["sent"] = {"model": model, "messages": messages, "params": params}
        message = SimpleNamespace(
            model_dump=lambda exclude_none=True: {"role": "assistant", "content": "churn is 4%"}
        )
        return SimpleNamespace(
            id="gen-1",
            choices=[SimpleNamespace(message=message, finish_reason="stop")],
            usage=SimpleNamespace(
                prompt_tokens=120, completion_tokens=8, prompt_tokens_details=None
            ),
        )


class _Embedder:
    async def embed_with_usage(self, texts, model):
        return [[0.1, 0.2, 0.3] for _ in texts], 5 * len(texts)


@pytest.fixture
def service(monkeypatch: pytest.MonkeyPatch) -> dict:
    """The org's config, its vendors and its meter, as the service reads them."""
    held: dict = {"metered": [], "spent": 0.0, "prices": {}, "providers": []}

    async def decrypted(admin_db, org_id, owner_id):
        return {}

    async def model_db(org_id):
        return SimpleNamespace(org=org_id)

    async def org_config(_db):
        return _CFG

    async def provider(cfg, _db, vendor):
        held["providers"].append(vendor)
        style = {e["id"]: e["style"] for e in cfg["ai_endpoints"]}.get(vendor, vendor)
        return _Provider(held), style

    async def spent(_db, source_id, month):
        held["month"] = month
        return held["spent"]

    async def meter(grant, _db, model, usage, price):
        held["metered"].append((grant.source_id, model, usage.input_tokens, usage.output_tokens))

    monkeypatch.setattr(secrets_store, "_decrypted", decrypted)
    monkeypatch.setattr(llm_service, "_model_db", model_db)
    monkeypatch.setattr(llm_service, "_org_config", org_config)
    monkeypatch.setattr(llm_service, "_provider", provider)
    monkeypatch.setattr(llm_service, "spent_usd", spent)
    monkeypatch.setattr(llm_service, "_meter", meter)
    monkeypatch.setattr(llm_service, "_price", lambda model: held["prices"].get(model))
    monkeypatch.setattr("provisa.vector.providers.get_provider", lambda name: _Embedder())
    return held


def _token(cap: float | None = None) -> str:
    return services.issue(
        services.ServiceGrant(org_id="acme", source_id="scorer", admin_db=None, llm_cap_usd=cap)
    )


def _call(token: str, path: str, body: dict) -> tuple[int, dict]:
    status, content_type, answered = services.answer(
        "POST", f"/{token}/llm/v1/{path}", json.dumps(body).encode()
    )
    assert content_type == services.JSON
    return status, json.loads(answered)


def _chat(token: str, model: str, **extra) -> tuple[int, dict]:
    return _call(
        token,
        "chat/completions",
        {"model": model, "messages": [{"role": "user", "content": "churn?"}], **extra},
    )


# -- which model --------------------------------------------------------------------------------


def test_a_role_names_the_model_the_orgs_ai_models_assign_it(service):
    token = _token()
    status, answered = _chat(token, "provisa:sql_generation", temperature=0)
    assert status == 200, answered
    assert service["sent"]["model"] == "anthropic/claude-haiku-5.5"
    assert service["sent"]["params"] == {"temperature": 0}
    assert service["providers"] == ["openrouter-gw"]
    assert answered["object"] == "chat.completion"
    assert answered["model"] == "anthropic/claude-haiku-5.5"
    assert answered["choices"][0]["message"] == {"role": "assistant", "content": "churn is 4%"}
    assert answered["usage"] == {"prompt_tokens": 120, "completion_tokens": 8, "total_tokens": 128}
    assert service["metered"] == [("scorer", "anthropic/claude-haiku-5.5", 120, 8)]


def test_a_model_a_role_is_assigned_may_be_named_itself(service):
    status, _ = _chat(_token(), "claude-haiku-5-5")
    assert status == 200
    assert service["providers"] == ["anthropic"]


@pytest.mark.parametrize(
    ("model", "named"),
    [
        ("gpt-9", "'gpt-9'"),
        ("provisa:column_description", "'column_description'"),
        ("provisa:nope", "'nope'"),
    ],
)
def test_any_other_model_is_refused_by_name_before_any_call(service, model, named):
    status, answered = _chat(_token(), model)
    assert status == 403
    assert named in answered["error"]["message"]
    assert "sent" not in service and service["metered"] == []


def test_what_goes_out_is_redacted(service):
    secret = "Bearer abcdefghijklmnopqrstuvwxyz0123456789"
    _call(
        _token(),
        "chat/completions",
        {"model": "provisa:sql_generation", "messages": [{"role": "user", "content": secret}]},
    )
    assert service["sent"]["messages"] == [{"role": "user", "content": "[REDACTED:bearer_token]"}]


def test_streaming_is_refused_by_name(service):
    status, answered = _chat(_token(), "provisa:sql_generation", stream=True)
    assert status == 400 and "stream" in answered["error"]["message"]


def test_a_revoked_token_reaches_no_model(service):
    token = _token()
    services.revoke(token)
    status, answered = _chat(token, "provisa:sql_generation")
    assert status == 401 and answered["error"]["code"] == 401


# -- the cap ------------------------------------------------------------------------------------


def test_a_capped_source_uses_only_priced_models(service):
    status, answered = _chat(_token(cap=5.0), "provisa:sql_generation")
    assert status == 403
    assert "no polly.model_prices entry" in answered["error"]["message"]
    assert "'scorer'" in answered["error"]["message"]
    assert "sent" not in service


def test_a_capped_source_is_refused_once_its_month_reaches_the_cap(service):
    service["prices"]["anthropic/claude-haiku-5.5"] = object()
    token = _token(cap=5.0)
    service["spent"] = 4.99
    assert _chat(token, "provisa:sql_generation")[0] == 200
    service["spent"] = 5.0
    status, answered = _chat(token, "provisa:sql_generation")
    assert status == 429 and "'scorer'" in answered["error"]["message"]
    assert len(service["month"]) == 7  # YYYY-MM


def test_an_uncapped_source_calls_an_unpriced_model_metered_as_tokens(service):
    assert _chat(_token(), "provisa:sql_generation")[0] == 200
    assert service["metered"] == [("scorer", "anthropic/claude-haiku-5.5", 120, 8)]


def test_a_cap_is_a_number_of_zero_or_more():
    assert services.llm_cap(None) is None
    assert services.llm_cap({"cap_usd": 20}) == 20.0
    for bad in ({"cap_usd": -1}, {"cap_usd": "20"}, {"cap_usd": True}):
        with pytest.raises(BundleRefused):
            services.llm_cap(bad)


# -- embeddings ---------------------------------------------------------------------------------


def test_an_orgs_vector_model_embeds_and_is_metered(service):
    status, answered = _call(
        _token(), "embeddings", {"model": "text-embedding-3-small", "input": ["a", "b"]}
    )
    assert status == 200, answered
    assert [d["index"] for d in answered["data"]] == [0, 1]
    assert answered["usage"] == {"prompt_tokens": 10, "total_tokens": 10}
    assert service["metered"] == [("scorer", "text-embedding-3-small", 10, 0)]


def test_an_embedding_model_the_org_does_not_list_is_refused(service):
    status, answered = _call(_token(), "embeddings", {"model": "ada-002", "input": "a"})
    assert status == 403 and "'ada-002'" in answered["error"]["message"]


# -- any SDK, through the egress proxy -----------------------------------------------------------


def test_the_openai_sdk_reaches_the_service_through_the_proxy_with_no_key(service):
    import httpx
    import openai

    token = _token()
    proxy = EgressProxy(allow=[])
    try:
        client = openai.OpenAI(
            base_url=f"{services.address(token)}/llm/v1",
            api_key="unused",  # the SDK requires one; the token in the address is the credential
            http_client=httpx.Client(proxy=proxy.url),
        )
        answered = client.chat.completions.create(
            model="provisa:sql_generation", messages=[{"role": "user", "content": "churn?"}]
        )
        assert answered.choices[0].message.content == "churn is 4%"
        with pytest.raises(openai.PermissionDeniedError) as refused:
            client.chat.completions.create(
                model="gpt-9", messages=[{"role": "user", "content": "x"}]
            )
        assert "'gpt-9'" in str(refused.value)
    finally:
        proxy.stop()
        services.revoke(token)
