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
#: 30 seconds to half an hour, depending on the vendor, or never; this holds for all of them.
CALL_TIMEOUT_S = 300.0

#: The environment variable aisuite reads each key-taking vendor's key from, when the caller
#: resolved none (provisa.core.org_secrets.LLM_VENDORS).
_KEY_ENV = {
    "anthropic": "ANTHROPIC_API_KEY",
    "openai": "OPENAI_API_KEY",
    "cohere": "CO_API_KEY",
    "groq": "GROQ_API_KEY",
    "mistral": "MISTRAL_API_KEY",
    "xai": "XAI_API_KEY",
    "deepseek": "DEEPSEEK_API_KEY",
    "together": "TOGETHER_API_KEY",
    "fireworks": "FIREWORKS_API_KEY",
    "nebius": "NEBIUS_API_KEY",
    "sambanova": "SAMBANOVA_API_KEY",
    "inception": "INCEPTION_API_KEY",
}

#: What a vendor's ``finish_reason`` says of an answer that stopped at ``max_tokens``.
_CUT_OFF_REASONS = frozenset({"length", "max_tokens"})
#: The host of an OpenAI-wire endpoint that takes OpenRouter's ``reasoning`` request object.
_OPENROUTER_HOST = "openrouter.ai"

_T = TypeVar("_T")


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
    """The vendor gave no answer within :data:`CALL_TIMEOUT_S`; the message names the model."""


def _stopped_at_limit(finish_reason: Any, usage: Any, max_tokens: int) -> bool:
    """Whether an answer stopped at ``max_tokens``. aisuite's converted vendors (groq, mistral, xai
    and the like) lose ``finish_reason``, and keep the usage that shows it. aisuite's google,
    cohere and ollama providers report neither, so a cut-off there is not seen."""
    if finish_reason in _CUT_OFF_REASONS:
        return True
    used = getattr(usage, "completion_tokens", None)
    return isinstance(used, int) and used >= max_tokens


class ReasoningNotControllable(ValueError):
    """A call asked for the model's reasoning off, and its vendor's wire has no setting Provisa
    knows that turns it off: the call is not sent as though it had one."""


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

        # REQ-1395, REQ-1398: an org's own per-vendor API keys (provisa.core.org_secrets),
        # keyed by vendor name, when the caller resolved them for the acting org. Vendors with
        # no key configured here (e.g. a local ollama/lmstudio endpoint) fall through to
        # aisuite's own env/config resolution.
        self._api_keys = api_keys
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

    def key_problem(self) -> str | None:
        """Why the model cannot be called, as far as that shows with no call made: neither its
        vendor nor its fallback's has the key it needs. None when one has, or needs none."""
        primary = self._vendor_key_problem(self._vendor)
        if primary is None or not self._fallback_vendor:
            return primary
        return None if self._vendor_key_problem(self._fallback_vendor) is None else primary

    def _vendor_key_problem(self, vendor: str) -> str | None:
        if (self._api_keys or {}).get(vendor):
            return None
        endpoint = self._endpoints.get(vendor)
        env = endpoint.get("api_key_env") if endpoint is not None else _KEY_ENV.get(vendor)
        if not env or os.environ.get(env):
            return None
        return f"{vendor} has no API key"

    def _complete_sync(
        self, vendor: str, model: str, prompt: str, system: str, max_tokens: int
    ) -> str:
        return self._respond(vendor, model, prompt, system, max_tokens).text

    def _reasoning_off(self, vendor: str, model: str) -> dict[str, Any]:
        """The request arguments that turn ``vendor``'s model's reasoning off, on its wire:
        OpenRouter's ``reasoning`` object with ``enabled: false`` (sent as the OpenAI client's
        ``extra_body``), or Anthropic's ``thinking`` of type ``disabled`` (natively or through an
        Anthropic-style endpoint). Any other wire raises :class:`ReasoningNotControllable`."""
        endpoint = self._endpoints.get(vendor)
        wire = endpoint["style"] if endpoint is not None else vendor
        if wire == "anthropic":
            return {"thinking": {"type": "disabled"}}
        if wire == "openai" and endpoint is not None:
            if _OPENROUTER_HOST in str(endpoint.get("base_url") or ""):
                return {"extra_body": {"reasoning": {"enabled": False}}}
        raise ReasoningNotControllable(
            f"{vendor}:{model} was asked with its reasoning off, and Provisa knows no setting "
            f"that turns reasoning off on the {wire} wire"
            + (f" at {endpoint.get('base_url')}" if endpoint is not None else "")
        )

    def _respond(
        self,
        vendor: str,
        model: str,
        prompt: str,
        system: str,
        max_tokens: int,
        reasoning_off: bool = False,
    ) -> Completion:
        import aisuite as ai

        # Settled before any client is made: a wire that cannot be told is never called.
        reasoning = self._reasoning_off(vendor, model) if reasoning_off else {}

        endpoint = self._endpoints.get(vendor)
        if endpoint is not None:
            # REQ-1790: dispatch to the wire protocol named by `style`, pointed at this
            # endpoint's base_url instead of the vendor's own. No fallback key invention: a
            # configured api_key_env that isn't set in the environment is a config error, not a
            # reason to try the call unauthenticated.
            # The key the caller resolved for this endpoint, by its id (the org's vendor keys carry a
            # sandbox's tier endpoint's, REQ-2006); else its api_key_env, an env-var name.
            key = (self._api_keys or {}).get(vendor)
            if key is None and endpoint.get("api_key_env"):
                key = os.environ.get(endpoint["api_key_env"])
                if not key:
                    raise ValueError(
                        f"ai_endpoints '{vendor}': api_key_env '{endpoint['api_key_env']}' "
                        "is not set"
                    )
            provider_vendor = endpoint["style"]
            provider_cfg: dict = {"base_url": endpoint["base_url"]}
            if key:
                provider_cfg["api_key"] = key
            client = ai.Client({provider_vendor: provider_cfg})
            model_id = self._make_aisuite_model_id(provider_vendor, model)
        else:
            key = (self._api_keys or {}).get(vendor)
            if key:
                client = ai.Client({vendor: {"api_key": key}})
            else:
                client = ai.Client()
            model_id = self._make_aisuite_model_id(vendor, model)
        messages = self._build_messages(prompt, system)
        response = client.chat.completions.create(
            model=model_id,
            messages=messages,
            max_tokens=max_tokens,
            **reasoning,
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

    def _bounded(self, call: Callable[[str, str], _T], vendor: str, model: str) -> _T:
        """``call(vendor, model)``, given up after :data:`CALL_TIMEOUT_S`. On a thread of its own,
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
        finished, _ = concurrent.futures.wait([answered], timeout=CALL_TIMEOUT_S)
        if not finished:
            raise LLMTimeoutError(
                f"{vendor}:{model} gave no answer within {CALL_TIMEOUT_S:g} seconds"
            )
        return answered.result()

    def _with_fallback(self, call: Callable[[str, str], _T]) -> _T:
        """``call(vendor, model)`` on the primary vendor/model, and on its failure on the
        fallback when one is configured. Each is held to :data:`CALL_TIMEOUT_S`."""
        try:
            return self._bounded(call, self._vendor, self._model)
        except Exception as primary_exc:
            log.warning(
                "LLM primary vendor=%s model=%s failed: %s",
                self._vendor,
                self._model,
                primary_exc,
            )
            if self._fallback_vendor and self._fallback_model:
                try:
                    return self._bounded(call, self._fallback_vendor, self._fallback_model)
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
            lambda vendor, model: self._complete_sync(vendor, model, prompt, system, max_tokens)
        )

    async def complete_detailed(
        self,
        prompt: str,
        system: str = "You are a helpful assistant.",
        max_tokens: int = 1024,
        *,
        reasoning_off: bool = False,
    ) -> Completion:
        """:meth:`complete` with the vendor, model and usage of the answer (:class:`Completion`).
        ``reasoning_off`` asks each model called, the fallback's too, with its reasoning turned off
        (:meth:`_reasoning_off`); a vendor whose wire has no such setting fails the call."""
        import asyncio

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None,
            lambda: self._with_fallback(
                lambda vendor, model: self._respond(
                    vendor, model, prompt, system, max_tokens, reasoning_off
                )
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
