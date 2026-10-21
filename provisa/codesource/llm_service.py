# Copyright (c) 2026 Kenneth Stott
# Canary: 6d2a9e41-8b3c-4f57-a0d1-5c7e92b4f318
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An OpenAI-compatible LLM service for a code source's worker (REQ-2105).

``$PROVISA_SERVICES/llm/v1/chat/completions`` and ``$PROVISA_SERVICES/llm/v1/embeddings``, so any
SDK works with its base URL pointed at ``$PROVISA_SERVICES/llm/v1`` and no key: the worker's token
(provisa/codesource/services.py) is in the address, and the egress proxy answers it.

A request names a model as ``provisa:<role>`` -- the model the org's AI Models assign that role --
or by a model id the org's AI Models assign some role (for embeddings, one its vector models
list). Anything else is refused, naming it. The call is routed as the org's own AI calls are:
its vendors, endpoints and keys, a sandbox-tier org's through the tier's (provisa.core.sandbox_ai).
What it sends out is redacted centrally (provisa.security.redaction). Streaming is not offered.

Each call is metered for its source in ``code_source_llm_usage`` -- its tokens, and its cost at
the model's ``polly.model_prices`` entry -- and audited in the org's trail. A source with a
monthly cap (``llm.cap_usd`` in its mapping) uses only models with a price, and once its month's
cost reaches the cap its calls are refused until the month turns. Two calls that start together
under the cap can both run, so the cap can be passed by the calls in flight when it is reached.
"""

# Requirements: REQ-2105
from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from provisa.codesource.services import ServiceGrant, ServiceRefused

if TYPE_CHECKING:
    from provisa.core.database import Database

log = logging.getLogger(__name__)

#: The prefix that names a model by the AI Models role it is assigned.
ROLE_PREFIX = "provisa:"

#: The audit trail's action for one call.
LLM_CALL = "code_source.llm.call"


def refusal_body(reason: str, status: int) -> bytes:
    """A refusal as an OpenAI-compatible SDK reads one: its message names what was refused."""
    return json.dumps(
        {"error": {"message": reason, "type": "provisa_refused", "code": status}}
    ).encode()


@asynccontextmanager
async def org_bound(grant: ServiceGrant) -> AsyncIterator[tuple["Database", dict]]:
    """``grant``'s org bound, with its vault, for the block: its model store and resolved config."""
    from provisa.core import secrets_store
    from provisa.core.request_context import reset_current_org, set_current_org

    token = set_current_org(grant.org_id)
    try:
        async with secrets_store.bound(grant.admin_db, grant.org_id):
            model_db = await _model_db(grant.org_id)
            yield model_db, await _org_config(model_db)
    finally:
        reset_current_org(token)


async def answer(
    grant: ServiceGrant, method: str, path: str, body: bytes, span: Any = None
) -> bytes:
    """The JSON answer to ``path`` (after ``llm/``) for ``grant``'s source; a refusal is raised.
    ``span``: the stage the call reports its model and tokens on (REQ-2106)."""
    if method != "POST":
        raise ServiceRefused(405, f"the LLM service is called with POST, not {method}")
    if path not in ("v1/chat/completions", "v1/embeddings"):
        raise ServiceRefused(
            404, f"the LLM service offers v1/chat/completions and v1/embeddings, not {path!r}"
        )
    try:
        request = json.loads(body or b"{}")
    except ValueError as exc:
        raise ServiceRefused(400, f"the request body is not JSON: {exc}") from None
    if not isinstance(request, dict) or not isinstance(request.get("model"), str):
        raise ServiceRefused(400, "the request names no model")
    async with org_bound(grant) as (model_db, cfg):
        if path == "v1/chat/completions":
            answered = await _chat(grant, model_db, cfg, request)
        else:
            answered = await _embeddings(grant, model_db, cfg, request)
    if span is not None:
        span.set_attribute("codesource.llm.model", answered["model"])
        span.set_attribute("codesource.llm.input_tokens", answered["usage"]["prompt_tokens"])
        span.set_attribute(
            "codesource.llm.output_tokens", answered["usage"].get("completion_tokens", 0)
        )
    return json.dumps(answered, default=str).encode()


async def embed(
    grant: ServiceGrant, model_db: "Database", cfg: dict, model: str, texts: list[str]
) -> list[list[float]]:
    """``texts`` embedded by the org's vector model ``model``, metered for the source as an
    embeddings call is (a source's own vector collection, REQ-2106)."""
    answered = await _embeddings(grant, model_db, cfg, {"model": model, "input": texts})
    return [d["embedding"] for d in answered["data"]]


async def _model_db(org_id: str) -> "Database":
    from provisa.api.admin.orgs_router import _org_model_db

    return await _org_model_db(org_id)


async def _org_config(model_db: "Database") -> dict:
    """The org's resolved config: the deployment's with its own AI Models, a sandbox-tier org's
    on the tier's (provisa.core.org_settings)."""
    from provisa.core.org_settings import resolve_org_config

    return await resolve_org_config(model_db)


# -- which model --------------------------------------------------------------------------------


def _choice(entry: Any) -> tuple[str, str] | None:
    """A role's (vendor, model) as AI Models assign it; None where it assigns none."""
    if isinstance(entry, str) and entry.strip():
        return "anthropic", entry.strip()  # the legacy form, as ProvisaLLMClient reads it
    if isinstance(entry, dict) and entry.get("model"):
        return str(entry.get("vendor") or "anthropic"), str(entry["model"])
    return None


def chat_route(cfg: dict, requested: str) -> tuple[str, str]:
    """(vendor, model) for ``requested``: ``provisa:<role>``, or a model some role is assigned."""
    roles = cfg.get("ai_models") or {}
    if requested.startswith(ROLE_PREFIX):
        role = requested[len(ROLE_PREFIX) :]
        chosen = _choice(roles.get(role))
        if chosen is None:
            raise ServiceRefused(403, f"the org's AI Models assign no model to the role {role!r}")
        return chosen
    for role in sorted(roles):
        chosen = _choice(roles[role])
        if chosen is not None and chosen[1] == requested:
            return chosen
    raise ServiceRefused(
        403,
        f"the model {requested!r} is not one the org's AI Models assign; name it as "
        f"{ROLE_PREFIX}<role> or by a model a role is assigned",
    )


# -- the cap and the meter ----------------------------------------------------------------------


def _month() -> str:
    return datetime.now(UTC).strftime("%Y-%m")


def _price(model: str) -> Any:
    from provisa.api.mcp import polly_usage

    return polly_usage.prices().get(model)


async def _admit(grant: ServiceGrant, model_db: "Database", model: str) -> Any:
    """The model's price, the call admitted under the source's cap: a capped source uses only
    priced models, and none once its month's cost has reached the cap."""
    price = _price(model)
    if grant.llm_cap_usd is None:
        return price
    if price is None:
        raise ServiceRefused(
            403,
            f"the model {model!r} has no polly.model_prices entry, and the source "
            f"{grant.source_id!r} has a cap, so it uses only priced models",
        )
    spent = await spent_usd(model_db, grant.source_id, _month())
    if spent >= grant.llm_cap_usd:
        raise ServiceRefused(
            429,
            f"the source {grant.source_id!r} has spent ${spent:.4f} of its "
            f"${grant.llm_cap_usd:.2f} LLM cap this month",
        )
    return price


async def spent_usd(model_db: "Database", source_id: str, month: str) -> float:
    """What ``source_id``'s LLM calls cost in ``month`` (YYYY-MM, UTC)."""
    from sqlalchemy import func, select

    from provisa.core.schema_org import code_source_llm_usage as t

    async with model_db.acquire() as conn:
        result = await conn.execute_core(
            select(func.coalesce(func.sum(t.c.cost_usd), 0.0)).where(
                (t.c.source_id == source_id) & (t.c.month == month)
            )
        )
        return float(result.fetchone()[0])


async def _meter(
    grant: ServiceGrant, model_db: "Database", model: str, usage: Any, price: Any
) -> float | None:
    """One call in the source's meter and the org's trail; what it cost, or None unpriced."""
    from sqlalchemy import insert

    from provisa.api.mcp import polly_usage
    from provisa.core.org_membership import record_admin_action
    from provisa.core.schema_org import code_source_llm_usage as t

    cost = None if price is None else polly_usage.cost_usd(usage, price)
    async with model_db.acquire() as conn:
        await conn.execute_core(
            insert(t).values(
                source_id=grant.source_id,
                month=_month(),
                model=model,
                input_tokens=usage.input_tokens,
                output_tokens=usage.output_tokens,
                cache_read_tokens=usage.cache_read_tokens,
                cache_write_tokens=usage.cache_write_tokens,
                cost_usd=cost,
            )
        )
    await record_admin_action(
        model_db,
        action=LLM_CALL,
        actor_id=f"source:{grant.source_id}",
        subject_id=model,
        detail={
            "source": grant.source_id,
            "input_tokens": usage.input_tokens,
            "output_tokens": usage.output_tokens,
            "cost_usd": cost,
        },
    )
    return cost


# -- chat ---------------------------------------------------------------------------------------


async def _provider(cfg: dict, model_db: "Database", vendor: str) -> tuple[Any, str]:
    """The aisuite provider for ``vendor`` -- one of aisuite's named vendors, or a configured
    endpoint by its id (REQ-1790), reached through the provider its ``style`` names -- with the
    org's key for it, as Polly is routed (provisa.api.mcp.chat._resolve_aisuite_routing)."""
    from aisuite.provider import ProviderFactory

    from provisa.core.org_secrets import read_org_api_keys
    from provisa.core.request_context import current_org
    from provisa.core.sandbox_ai import endpoint_key
    from provisa.core.secrets import resolve_secrets

    endpoints = {ep["id"]: ep for ep in (cfg.get("ai_endpoints") or []) if ep.get("enabled", True)}
    endpoint = endpoints.get(vendor)
    if endpoint is None:
        key = (await read_org_api_keys(model_db)).get(vendor)
        return ProviderFactory.create_provider(vendor, {"api_key": key} if key else {}), vendor
    style = endpoint["style"]
    config: dict[str, Any] = {"base_url": resolve_secrets(endpoint["base_url"])}
    key = await endpoint_key(endpoint, current_org.get())
    # aisuite's OpenAI provider requires a key even for a server that checks none (a local
    # Ollama): a placeholder satisfies that constructor, and is no credential.
    config["api_key"] = key or ("not-required" if style == "openai" else None)
    return ProviderFactory.create_provider(style, config), style


def _redacted_messages(messages: Any, org_id: str) -> Any:
    """What goes out, through the central redaction (REQ-2087)."""
    from provisa.security.redaction import redact_value

    return redact_value(messages, org_id)


async def _chat(grant: ServiceGrant, model_db: "Database", cfg: dict, request: dict) -> dict:
    from provisa.api.mcp import polly_usage

    if request.get("stream"):
        raise ServiceRefused(400, "the LLM service does not stream; send stream: false")
    messages = request.get("messages")
    if not isinstance(messages, list) or not messages:
        raise ServiceRefused(400, "the request has no messages")
    vendor, model = chat_route(cfg, request["model"])
    price = await _admit(grant, model_db, model)
    provider, provider_key = await _provider(cfg, model_db, vendor)
    params = {k: v for k, v in request.items() if k not in ("model", "messages", "stream")}
    try:
        resp = await asyncio.to_thread(
            provider.chat_completions_create,
            model,
            _redacted_messages(messages, grant.org_id),
            **params,
        )
    except Exception as exc:  # allow-ble: any vendor failure is the one refusal of the call, naming the model; its message is redacted
        from provisa.security.redaction import redact

        raise ServiceRefused(
            502, f"the model {model!r} ({provider_key}) failed: {redact(str(exc), grant.org_id)}"
        ) from None
    reported = getattr(resp, "usage", None)
    usage = (
        polly_usage.from_openai(reported)
        if reported is not None and getattr(reported, "prompt_tokens", None) is not None
        else polly_usage.Usage(calls=1)
    )
    await _meter(grant, model_db, model, usage, price)
    return completion_json(resp, model, usage)


def completion_json(resp: Any, model: str, usage: Any) -> dict:
    """An OpenAI chat completion, from an OpenAI SDK response or aisuite's normalized one."""
    choices = []
    for i, choice in enumerate(resp.choices):
        message = choice.message
        dumped = (
            message.model_dump(exclude_none=True)
            if hasattr(message, "model_dump")
            else {"role": "assistant", "content": getattr(message, "content", None)}
        )
        dumped.setdefault("role", "assistant")
        choices.append(
            {
                "index": i,
                "message": dumped,
                "finish_reason": getattr(choice, "finish_reason", None) or "stop",
            }
        )
    prompt = usage.input_tokens + usage.cache_read_tokens + usage.cache_write_tokens
    return {
        "id": getattr(resp, "id", None) or "provisa-llm",
        "object": "chat.completion",
        "created": int(datetime.now(UTC).timestamp()),
        "model": model,
        "choices": choices,
        "usage": {
            "prompt_tokens": prompt,
            "completion_tokens": usage.output_tokens,
            "total_tokens": prompt + usage.output_tokens,
        },
    }


# -- embeddings ---------------------------------------------------------------------------------


async def _embeddings(grant: ServiceGrant, model_db: "Database", cfg: dict, request: dict) -> dict:
    from provisa.api.mcp import polly_usage
    from provisa.security.redaction import redact
    from provisa.vector.providers import EmbeddingError, get_provider
    from provisa.vector.registry import VectorModelError, VectorModelRegistry

    texts = request.get("input")
    if isinstance(texts, str):
        texts = [texts]
    if not isinstance(texts, list) or not texts or not all(isinstance(t, str) for t in texts):
        raise ServiceRefused(400, "input is a string or a list of strings")
    try:
        vector_model = VectorModelRegistry.from_config(cfg.get("vector_models") or []).get(
            request["model"]
        )
    except VectorModelError:
        raise ServiceRefused(
            403, f"the model {request['model']!r} is not one of the org's vector models"
        ) from None
    price = await _admit(grant, model_db, vector_model.id)
    try:
        vectors, tokens = await get_provider(vector_model.provider).embed_with_usage(
            [redact(t, grant.org_id) for t in texts], vector_model
        )
    except EmbeddingError as exc:
        raise ServiceRefused(
            502, f"the model {vector_model.id!r} failed: {redact(str(exc), grant.org_id)}"
        ) from None
    usage = polly_usage.Usage(input_tokens=tokens or 0, calls=1)
    await _meter(grant, model_db, vector_model.id, usage, price)
    return {
        "object": "list",
        "model": vector_model.id,
        "data": [
            {"object": "embedding", "index": i, "embedding": v} for i, v in enumerate(vectors)
        ],
        "usage": {"prompt_tokens": usage.input_tokens, "total_tokens": usage.input_tokens},
    }
