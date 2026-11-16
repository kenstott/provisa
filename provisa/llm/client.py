# Copyright (c) 2026 Kenneth Stott
# Canary: a1b2c3d4-e5f6-7890-bcde-f01234567890
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Multi-vendor LLM abstraction layer using aisuite."""

from __future__ import annotations

import concurrent.futures
import contextvars
import logging
import os
import threading
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Optional, TypeVar

from provisa.llm.limits import Limits, limits_for

# Requirements: REQ-355, REQ-356, REQ-358

log = logging.getLogger(__name__)

# Default config when operation key is absent from yaml
_DEFAULTS: dict[str, dict] = {
    "table_description": {"vendor": "anthropic", "model": "claude-haiku-4-5-20251001"},
    "column_description": {"vendor": "anthropic", "model": "claude-haiku-4-5-20251001"},
    "relationship_inference": {"vendor": "anthropic", "model": "claude-haiku-4-5-20251001"},
    "sql_generation": {"vendor": "anthropic", "model": "claude-opus-4-6"},
    "table_selection": {"vendor": "anthropic", "model": "claude-haiku-4-5-20251001"},
}


#: How long one model call may take, in seconds. A vendor's own client gives up after anything from
#: 30 seconds to half an hour, depending on the vendor, or never; this holds for all of them. A
#: streamed request is given the time its answer may take (:meth:`ProvisaLLMClient.call_timeout_s`).
CALL_TIMEOUT_S = 300.0

#: The Anthropic SDK's own measure of a request's time (anthropic._base_client,
#: ``_calculate_nonstreaming_timeout``): an answer of 128,000 tokens is expected to take an hour,
#: and a request expected to take over ten minutes is refused unless it is streamed.
_ANTHROPIC_SECONDS_PER_TOKEN = 3600 / 128_000
_ANTHROPIC_WHOLE_REQUEST_S = 600.0

#: What a vendor's ``finish_reason`` says of an answer that stopped at ``max_tokens``.
_CUT_OFF_REASONS = frozenset({"length", "max_tokens"})
#: The host of an OpenAI-wire endpoint that takes OpenRouter's ``reasoning`` request object.
_OPENROUTER_HOST = "openrouter.ai"

_T = TypeVar("_T")


def _refuse_key_variable_of_held_org(variable: str | None) -> None:
    """REQ-1557: an endpoint's key variable a held organization's own settings name is not read
    (``core.env_reference_guard.refuse_named_by_org``); the operator's endpoints are."""
    from provisa.core.env_reference_guard import refuse_named_by_org

    refuse_named_by_org(variable)


@dataclass(frozen=True)
class Completion:
    """A completion with what produced it: the vendor and model that answered (the fallback's,
    when the primary failed) and the usage the vendor reported (aisuite's CompletionUsage, OpenAI
    shaped; None when the vendor reported none). A caller held to a role's LLM budget (REQ-1174)
    counts the usage against that model. ``cut_off`` says the answer stopped at its output limit,
    so its text is the start of an answer (:func:`_stopped_at_limit`)."""

    text: str
    vendor: str
    model: str
    usage: Any
    cut_off: bool


class LLMEmptyResponseError(RuntimeError):
    """The provider answered 200 but the message carried no text content. ``answer`` is that
    answer, with no text: its usage was spent, and ``cut_off`` says the output limit is why."""

    def __init__(self, message: str, answer: Completion) -> None:
        super().__init__(message)
        self.answer = answer


class LLMTimeoutError(TimeoutError):
    """The vendor gave no answer within the call's bound, ``seconds``
    (:meth:`ProvisaLLMClient.call_timeout_s`); the message names the model."""

    def __init__(self, message: str, seconds: float) -> None:
        super().__init__(message)
        self.seconds = seconds


def failure(exc: BaseException) -> dict[str, Any]:
    """A failed model call as the product says it: a catalogued ``code`` with its ``params`` (the
    UI's ``serverErrors`` catalog words it in the reader's language) and the English ``message``.
    The vendor's own reason is carried as a param, never as the message."""
    if isinstance(exc, LLMTimeoutError):
        seconds = round(exc.seconds)
        return {
            "code": "llm.no_answer_in_time",
            "params": {"seconds": seconds},
            "message": f"The model gave no answer within {seconds} seconds.",
        }
    if isinstance(exc, LLMEmptyResponseError):
        return {"code": "llm.empty_answer", "params": {}, "message": "The model returned no text."}
    from provisa.core.org_secrets import AiKeyRequired

    if isinstance(exc, AiKeyRequired):
        return {"code": exc.code, "params": exc.params, "message": str(exc)}
    reason = str(exc) or type(exc).__name__
    return {
        "code": "llm.request_failed",
        "params": {"reason": reason},
        "message": f"The request to the model failed: {reason}",
    }


def anthropic_requires_stream(model: str, max_tokens: int) -> bool:
    """Whether the Anthropic SDK sends a request for ``max_tokens`` only as a stream: one it
    expects to take over ten minutes (above 21,333 tokens), or one above the model's own unstreamed
    limit. Sent whole, the SDK refuses it before anything is sent."""
    from anthropic._constants import MODEL_NONSTREAMING_TOKENS

    if max_tokens * _ANTHROPIC_SECONDS_PER_TOKEN > _ANTHROPIC_WHOLE_REQUEST_S:
        return True
    whole_limit = MODEL_NONSTREAMING_TOKENS.get(model)
    return whole_limit is not None and max_tokens > whole_limit


def _stopped_at_limit(finish_reason: Any, usage: Any, max_tokens: int) -> bool:
    """Whether an answer stopped at ``max_tokens``. aisuite's converted vendors (groq, mistral, xai
    and the like) lose ``finish_reason``, and keep the usage that shows it. aisuite's google,
    cohere and ollama providers report neither, so a cut-off there is not seen."""
    if finish_reason in _CUT_OFF_REASONS:
        return True
    used = getattr(usage, "completion_tokens", None)
    return isinstance(used, int) and used >= max_tokens


class ProvisaLLMClient:  # REQ-355, REQ-356, REQ-358
    """Vendor-agnostic LLM client backed by aisuite.

    Reads vendor/model from config `ai_models.<operation>` section.
    Supports a fallback vendor/model tried on primary failure.
    """

    def __init__(
        self,
        operation: str = "column_description",
        *,
        config: dict | None = None,
        api_keys: dict[str, str] | None = None,
    ) -> None:
        from provisa.api.admin._config_io import read_config

        # REQ-1349: `config` is an already-resolved config — the deployment file with the acting
        # org's overrides layered on (provisa.core.org_settings.resolve_org_config). It is passed
        # by request paths that have an org bound; paths with no org (startup, discovery jobs)
        # construct with none and get the deployment config, which is the base in both cases.
        cfg = read_config() if config is None else config
        if config is None:
            # REQ-2006: a sandbox-tier org's roles use the tier's AI models (the resolved config a
            # request path passes carries them already, provisa.core.org_settings).
            from provisa.core.org_settings import merge_org_overrides
            from provisa.core.request_context import current_org
            from provisa.core.sandbox_ai import defaults_for

            cfg = merge_org_overrides(cfg, defaults_for(current_org.get()))

        # REQ-1395, REQ-1398: the org's own per-vendor API keys (provisa.core.org_secrets), keyed
        # by vendor name, as the caller read them for the acting org; a path with no org bound
        # passes none. The key a call is made with is org_secrets.provider_key's answer.
        self._api_keys: dict[str, str] = api_keys or {}
        # REQ-464: kept to answer the model's limits (provisa.llm.limits), the org's overrides
        # being in this same resolved config.
        self._config = cfg
        # REQ-1790: custom endpoints (id -> {style, base_url, api_key_env}), keyed by the `id` a
        # role assignment names as its "vendor". A role that names one of these is dispatched to
        # aisuite's openai/anthropic provider (per `style`) with `base_url` overridden, rather
        # than to a vendor aisuite knows by that name.
        self._endpoints = {
            ep["id"]: ep for ep in (cfg.get("ai_endpoints", []) or []) if ep.get("enabled", True)
        }
        ai_models = cfg.get("ai_models", {})
        op_cfg = ai_models.get(operation, {})

        # Support both legacy string format ("claude-haiku-4-5-20251001") and
        # new dict format ({"vendor": "anthropic", "model": "..."})
        if isinstance(op_cfg, str):
            self._vendor = "anthropic"
            self._model = op_cfg
            self._fallback_vendor: Optional[str] = None
            self._fallback_model: Optional[str] = None
        elif isinstance(op_cfg, dict):
            defaults = _DEFAULTS.get(
                operation, {"vendor": "anthropic", "model": "claude-haiku-4-5-20251001"}
            )
            self._vendor = op_cfg.get("vendor") or defaults["vendor"]
            self._model = op_cfg.get("model") or defaults["model"]
            fallback = op_cfg.get("fallback")
            if isinstance(fallback, dict):
                self._fallback_vendor = fallback.get("vendor")
                self._fallback_model = fallback.get("model")
            else:
                self._fallback_vendor = None
                self._fallback_model = None
        else:
            defaults = _DEFAULTS.get(
                operation, {"vendor": "anthropic", "model": "claude-haiku-4-5-20251001"}
            )
            self._vendor = defaults["vendor"]
            self._model = defaults["model"]
            self._fallback_vendor = None
            self._fallback_model = None

    def _make_aisuite_model_id(self, vendor: str, model: str) -> str:
        return f"{vendor}:{model}"

    def _build_messages(self, prompt: str, system: str) -> list[dict]:
        messages = []
        if system:
            messages.append({"role": "system", "content": system})
        messages.append({"role": "user", "content": prompt})
        return messages

    @property
    def vendor(self) -> str:
        return self._vendor

    @property
    def model(self) -> str:
        return self._model

    def limits(self) -> Limits:
        """The primary model's input and output limits in tokens, and their source (REQ-464)."""
        return limits_for(self._vendor, self._model, self._config)

    def key_problem(self) -> str | None:
        """Why the model cannot be called, as far as that shows with no call made: neither its
        vendor nor its fallback's has the key it needs. None when one has, or needs none."""
        primary = self._vendor_key_problem(self._vendor)
        if primary is None or not self._fallback_vendor:
            return primary
        return None if self._vendor_key_problem(self._fallback_vendor) is None else primary

    def _vendor_key_problem(self, vendor: str) -> str | None:
        if self._api_keys.get(vendor):
            return None
        endpoint = self._endpoints.get(vendor)
        if endpoint is None:
            from provisa.core.org_secrets import AiKeyRequired, provider_key

            try:
                provider_key(self._api_keys, vendor)
            except AiKeyRequired:
                return f"{vendor} has no API key"
            return None
        env = endpoint.get("api_key_env")
        _refuse_key_variable_of_held_org(env)  # REQ-1557
        if not env or os.environ.get(env):
            return None
        return f"{vendor} has no API key"

    def _wire(self, vendor: str) -> str:
        """The wire ``vendor`` is spoken to on: an endpoint's ``style``, else the vendor itself."""
        endpoint = self._endpoints.get(vendor)
        return endpoint["style"] if endpoint is not None else vendor

    def streams(self, vendor: str, model: str, max_tokens: int) -> bool:
        """Whether a request to ``vendor``'s model for ``max_tokens`` is streamed and collected:
        on Anthropic's wire, when its SDK requires it (:func:`anthropic_requires_stream`). The
        OpenAI SDK, and aisuite's other providers, have no such rule and are sent whole."""
        return self._wire(vendor) == "anthropic" and anthropic_requires_stream(model, max_tokens)

    def call_timeout_s(self, vendor: str, model: str, max_tokens: int) -> float:
        """How long one call to ``vendor``'s model may take: :data:`CALL_TIMEOUT_S`, and for a
        streamed request the time the vendor's SDK expects an answer of ``max_tokens`` to take
        (an hour for 128,000 tokens)."""
        if self.streams(vendor, model, max_tokens):
            return max(CALL_TIMEOUT_S, max_tokens * _ANTHROPIC_SECONDS_PER_TOKEN)
        return CALL_TIMEOUT_S

    def _complete_sync(
        self, vendor: str, model: str, prompt: str, system: str, max_tokens: int
    ) -> str:
        return self._respond(vendor, model, prompt, system, max_tokens).text

    def _reasoning_off(self, vendor: str, model: str) -> dict[str, Any]:
        """The request arguments that turn ``vendor``'s model's reasoning off, on its wire:
        OpenRouter's ``reasoning`` object with ``enabled: false`` (sent as the OpenAI client's
        ``extra_body``), or Anthropic's ``thinking`` of type ``disabled`` (natively or through an
        Anthropic-style endpoint). Any other wire gets no setting (``{}``)."""
        endpoint = self._endpoints.get(vendor)
        wire = self._wire(vendor)
        if wire == "anthropic":
            return {"thinking": {"type": "disabled"}}
        if wire == "openai" and endpoint is not None:
            if _OPENROUTER_HOST in str(endpoint.get("base_url") or ""):
                return {"extra_body": {"reasoning": {"enabled": False}}}
        # Maintainer decision, 2026-11-04: "leave other vendors on their default." Only wires with
        # a documented switch get one; the rest (openai direct, groq, ollama, lmstudio, other
        # openai-compatible gateways) send no reasoning setting and keep the vendor's default.
        # This is the documented design, not a fallback.
        return {}

    def _json_answer(self, vendor: str, schema: dict[str, Any], name: str) -> dict[str, Any]:
        """The request arguments that hold ``vendor``'s answer to one JSON value following
        ``schema`` (an object schema, every property required and no other allowed), on its wire:
        Anthropic's structured output (``output_config.format`` of type ``json_schema``, natively
        or through an Anthropic-style endpoint), or the OpenAI ``response_format`` of type
        ``json_schema``, strict, for OpenAI itself and OpenRouter. Any other wire gets no setting
        (``{}``) and is asked in the prompt alone."""
        endpoint = self._endpoints.get(vendor)
        wire = self._wire(vendor)
        if wire == "anthropic":
            return {"output_config": {"format": {"type": "json_schema", "schema": schema}}}
        if wire == "openai" and (
            endpoint is None or _OPENROUTER_HOST in str(endpoint.get("base_url") or "")
        ):
            return {
                "response_format": {
                    "type": "json_schema",
                    "json_schema": {"name": name, "schema": schema, "strict": True},
                }
            }
        # As with reasoning (_reasoning_off): only wires with a documented, known-supported way to
        # ask get one. Other OpenAI-compatible gateways (ollama, lmstudio, groq and the like)
        # differ in which response_format they take, so they keep their default.
        return {}

    def _respond(
        self,
        vendor: str,
        model: str,
        prompt: str,
        system: str,
        max_tokens: int,
        reasoning_off: bool = False,
        json_schema: dict[str, Any] | None = None,
        json_name: str = "answer",
    ) -> Completion:
        import aisuite as ai

        asked = self._reasoning_off(vendor, model) if reasoning_off else {}
        if json_schema is not None:
            asked |= self._json_answer(vendor, json_schema, json_name)

        endpoint = self._endpoints.get(vendor)
        if endpoint is not None:
            # REQ-1790: dispatch to the wire protocol named by `style`, pointed at this
            # endpoint's base_url instead of the vendor's own. No fallback key invention: a
            # configured api_key_env that isn't set in the environment is a config error, not a
            # reason to try the call unauthenticated.
            # The key the caller resolved for this endpoint, by its id (the org's vendor keys carry a
            # sandbox's tier endpoint's, REQ-2006); else its api_key_env, an env-var name.
            key = self._api_keys.get(vendor)
            if key is None and endpoint.get("api_key_env"):
                _refuse_key_variable_of_held_org(endpoint["api_key_env"])  # REQ-1557
                key = os.environ.get(endpoint["api_key_env"])
                if not key:
                    raise ValueError(
                        f"ai_endpoints '{vendor}': api_key_env '{endpoint['api_key_env']}' "
                        "is not set"
                    )
            provider_vendor = endpoint["style"]
            from provisa.core.org_secrets import endpoint_provider_key

            provider_cfg: dict = {"base_url": endpoint["base_url"]}
            key = endpoint_provider_key(key)
            if key:
                provider_cfg["api_key"] = key
        else:
            from provisa.core.org_secrets import provider_key

            # REQ-1395: the org's stored key, or on an install of one organization the
            # deployment's; refused before anything is sent when there is none.
            key = provider_key(self._api_keys, vendor)
            provider_vendor = vendor
            provider_cfg = {"api_key": key} if key else {}
        model_id = self._make_aisuite_model_id(provider_vendor, model)
        messages = self._build_messages(prompt, system)
        if self.streams(vendor, model, max_tokens):
            response = self._streamed(provider_cfg, model, messages, max_tokens, asked)
        else:
            client = ai.Client({provider_vendor: provider_cfg}) if provider_cfg else ai.Client()
            response = client.chat.completions.create(
                model=model_id,
                messages=messages,
                max_tokens=max_tokens,
                **asked,
            )
        choice = response.choices[0]
        content = choice.message.content
        finish_reason = getattr(choice, "finish_reason", None)
        usage = getattr(response, "usage", None)
        cut_off = _stopped_at_limit(finish_reason, usage, max_tokens)
        if content is None:
            raise LLMEmptyResponseError(
                f"{model_id} returned no message content "
                f"(finish_reason={finish_reason!r}, "
                f"max_tokens={max_tokens}); a reasoning model can spend the whole token budget "
                "before emitting text",
                Completion("", vendor, model, usage, cut_off),
            )
        return Completion(content.strip(), vendor, model, usage, cut_off)

    def _streamed(
        self,
        provider_cfg: dict[str, Any],
        model: str,
        messages: list[dict],
        max_tokens: int,
        asked: dict[str, Any],
    ) -> Any:
        """The answer to a request the Anthropic SDK sends only as a stream, as aisuite answers
        one sent whole. aisuite's Anthropic provider cannot stream (it reads a stream as a whole
        message), so its own SDK client streams the request the SDK's documented way for a long
        request (``messages.stream(...)`` then ``get_final_message()``), and its own converter
        turns the collected message into the same response: text, finish reason and usage."""
        from aisuite.provider import ProviderFactory

        provider = ProviderFactory.create_provider("anthropic", provider_cfg)
        system, converted = provider.converter.convert_request(messages)
        with provider.client.messages.stream(
            model=model, system=system, messages=converted, max_tokens=max_tokens, **asked
        ) as stream:
            message = stream.get_final_message()
        return provider.converter.convert_response(message)

    def _bounded(
        self, call: Callable[[str, str], _T], vendor: str, model: str, timeout_s: float
    ) -> _T:
        """``call(vendor, model)``, given up after ``timeout_s``. On a thread of its own,
        the one way to hold a blocking call in a vendor's client whatever that client does: a
        stalled call's thread ends when its client gives up, and nothing waits for it."""
        answered: concurrent.futures.Future[_T] = concurrent.futures.Future()
        context = contextvars.copy_context()

        def run() -> None:
            try:
                answered.set_result(context.run(call, vendor, model))
            except BaseException as exc:  # handed to the caller, who re-raises it
                answered.set_exception(exc)

        threading.Thread(target=run, name=f"llm-call:{vendor}", daemon=True).start()
        finished, _ = concurrent.futures.wait([answered], timeout=timeout_s)
        if not finished:
            raise LLMTimeoutError(
                f"{vendor}:{model} gave no answer within {timeout_s:g} seconds", timeout_s
            )
        return answered.result()

    def _with_fallback(self, call: Callable[[str, str], _T], max_tokens: int) -> _T:
        """``call(vendor, model)`` on the primary vendor/model, and on its failure on the
        fallback when one is configured. Each is held to its bound for an answer of
        ``max_tokens`` (:meth:`call_timeout_s`)."""
        try:
            return self._bounded(
                call,
                self._vendor,
                self._model,
                self.call_timeout_s(self._vendor, self._model, max_tokens),
            )
        except Exception as primary_exc:
            log.warning(
                "LLM primary vendor=%s model=%s failed: %s",
                self._vendor,
                self._model,
                primary_exc,
            )
            if self._fallback_vendor and self._fallback_model:
                try:
                    return self._bounded(
                        call,
                        self._fallback_vendor,
                        self._fallback_model,
                        self.call_timeout_s(
                            self._fallback_vendor, self._fallback_model, max_tokens
                        ),
                    )
                except Exception as fallback_exc:
                    log.warning(
                        "LLM fallback vendor=%s model=%s failed: %s",
                        self._fallback_vendor,
                        self._fallback_model,
                        fallback_exc,
                    )
                    raise fallback_exc
            raise primary_exc

    def complete_sync(
        self, prompt: str, system: str = "You are a helpful assistant.", max_tokens: int = 1024
    ) -> str:
        """Complete a prompt synchronously.

        Tries primary vendor/model first; on failure tries fallback if configured.
        Returns plain text response.
        """
        return self._with_fallback(
            lambda vendor, model: self._complete_sync(vendor, model, prompt, system, max_tokens),
            max_tokens,
        )

    def complete_sync_detailed(
        self, prompt: str, system: str = "You are a helpful assistant.", max_tokens: int = 1024
    ) -> Completion:
        """:meth:`complete_sync` with the vendor, model, usage and ``cut_off`` of the answer
        (:class:`Completion`): a caller that reads a structured answer learns whether it stopped
        at ``max_tokens``."""
        return self._with_fallback(
            lambda vendor, model: self._respond(vendor, model, prompt, system, max_tokens),
            max_tokens,
        )

    async def complete_detailed(
        self,
        prompt: str,
        system: str = "You are a helpful assistant.",
        max_tokens: int = 1024,
        *,
        reasoning_off: bool = False,
        json_schema: dict[str, Any] | None = None,
        json_name: str = "answer",
    ) -> Completion:
        """:meth:`complete` with the vendor, model and usage of the answer (:class:`Completion`).
        ``reasoning_off`` asks each model called, the fallback's too, with its reasoning turned off
        (:meth:`_reasoning_off`); a wire with no documented switch is sent none and keeps its default.
        ``json_schema`` (named ``json_name``) asks each model called for one JSON value following
        it (:meth:`_json_answer`), on the wires that can be held to one."""
        import asyncio

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None,
            lambda: self._with_fallback(
                lambda vendor, model: self._respond(
                    vendor,
                    model,
                    prompt,
                    system,
                    max_tokens,
                    reasoning_off,
                    json_schema,
                    json_name,
                ),
                max_tokens,
            ),
        )

    async def complete(
        self,
        prompt: str,
        system: str = "You are a helpful assistant.",
        max_tokens: int = 1024,
    ) -> str:
        """Complete a prompt asynchronously.

        Tries primary vendor/model first; on failure tries fallback if configured.
        Returns plain text response.
        """
        import asyncio

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None,
            lambda: self.complete_sync(prompt, system, max_tokens),
        )
